//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::scoped_lock lock{latch_};
  frame_id_t frame_id = -1;
  Page *page = nullptr;

  if (page_table_.find(page_id) != page_table_.end()) {
    // 找到页面，增加pin值,返回page
    frame_id = page_table_[page_id];
    page = &pages_[frame_id];
    page->pin_count_++;
    replacer_->Pin(frame_id);
    return page;
  }

  // 若不存在，则需要从磁盘读入缓冲池中
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = &pages_[frame_id];
  } else if (replacer_->Victim(&frame_id)) {
    // 若free_list中无空闲页，则在缓冲池中选择淘汰页(不一定成功)
    page = &pages_[frame_id];
    // 淘汰时需要写出脏页，删除页表项
    if (page->IsDirty()) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
    }
    page_table_.erase(page->GetPageId());
  } else {
    // 两者都失败
    return nullptr;
  }

  // 从磁盘读取此页
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  disk_manager_->ReadPage(page->GetPageId(), page->GetData());

  // 更新页表和replacer
  page_table_[page_id] = frame_id;
  replacer_->Pin(frame_id);

  return page;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::scoped_lock lock{latch_};
  frame_id_t frame_id;
  Page *page = nullptr;
  // page不存在
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  frame_id = page_table_[page_id];
  page = &pages_[frame_id];

  if (page->GetPinCount() <= 0) {
    return false;
  }
  page->pin_count_--;

  if (is_dirty) {
    page->is_dirty_ = true;
  }

  if (page->GetPinCount() <= 0) {
    replacer_->Unpin(frame_id);
  }

  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::scoped_lock lock{latch_};
  // page无效
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  Page *page = &pages_[page_table_[page_id]];

  // 找不到page
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  disk_manager_->WritePage(page->GetPageId(), page->GetData());
  page->is_dirty_ = false;
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::scoped_lock lock{latch_};
  frame_id_t frame_id = -1;
  Page *page = nullptr;

  if (!free_list_.empty()) {
    // 优先从free_list中找空闲页
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = &pages_[frame_id];
  } else if (replacer_->Victim(&frame_id)) {
    // 若free_list中无空闲页，则在缓冲池中选择淘汰页(不一定成功)
    page = &pages_[frame_id];
    // 淘汰时需要写出脏页，删除页表项
    if (page->IsDirty()) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
    }
    page_table_.erase(page->GetPageId());
  } else {
    // 两者都失败
    return nullptr;
  }

  //重置新页数据
  page->page_id_ = *page_id = cur_max_page_id_++;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  page->ResetMemory();

  // 更新页表和repalcer
  page_table_[*page_id] = frame_id;
  replacer_->Pin(frame_id);

  return page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::scoped_lock lock{latch_};
  frame_id_t frame_id;
  Page *page = nullptr;

  // P does not exist
  if (page_table_.find(page_id) == page_table_.end()) {
    // DeallocatePage(page_id);
    return true;
  }

  // P exists
  frame_id = page_table_[page_id];
  page = &pages_[frame_id];
  if (page->GetPinCount() != 0) {
    return false;
  }

  // write dirty page
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
  }

  // Remove P from the page table
  page_table_.erase(page_id);

  // reset metadata
  page->page_id_ = INVALID_PAGE_ID;
  page->is_dirty_ = false;
  page->pin_count_ = 0;
  page->ResetMemory();
  free_list_.push_back(frame_id);
  replacer_->Pin(frame_id);
  // replacer_->Unpin(frame_id);

  // DeallocatePage(page_id);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  std::scoped_lock lock{latch_};
  auto it = page_table_.begin();
  while (it != page_table_.end()) {
    Page *page = &pages_[it->second];
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
    page->is_dirty_ = false;
    it++;
  }
}

}  // namespace bustub
