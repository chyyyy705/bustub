//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : max_size_(num_pages) { mutex_.lock(); }

LRUReplacer::~LRUReplacer() { mutex_.unlock(); }

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  // 根据LRU策略删除某个frame
  if (lru_list_.empty()) {
    return false;
  }

  *frame_id = lru_list_.back();
  lru_map_.erase(lru_list_.back());
  lru_list_.pop_back();

  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  // 锁住某个frame，使他不能被Victim,即在链表中删除此frame
  auto it = lru_map_.find(frame_id);
  if (it == lru_map_.end()) {
    // 如果将要锁住的frame已被删除
    return;
  }

  lru_list_.erase(it->second);
  lru_map_.erase(it);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  //避免重复
  if (lru_map_.count(frame_id) != 0) {
    return;
  }
  //超出容量
  if (lru_list_.size() >= max_size_) {
    return;
  }

  lru_list_.push_front(frame_id);
  lru_map_[frame_id] = lru_list_.begin();
}

size_t LRUReplacer::Size() {
  // 返回能被Victim的数目
  return lru_list_.size();
}

}  // namespace bustub
