//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  // set page type
  BPlusTreePage::SetPageType(IndexPageType::INTERNAL_PAGE);
  // set current size
  BPlusTreePage::SetSize(0);
  // set page id
  BPlusTreePage::SetPageId(page_id);
  // set parent id
  BPlusTreePage::SetParentPageId(parent_id);
  // set max page size(INTERNAL_PAGE_SIZE)
  BPlusTreePage::SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  if (index < 0 && index >= GetSize()) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "OUT_OF_RANGE");
  }
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  if (index < 0 && index >= GetSize()) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "OUT_OF_RANGE");
  }
  array_[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  int offset;
  for (offset = 0; offset < GetSize(); offset++) {
    if (array_[offset].second == value) {
      break;
    }
  }
  return offset;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  if (index < 0 && index >= GetSize()) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "OUT_OF_RANGE");
  }
  return array_[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  int low = 1;
  int high = GetSize() - 1;
  int mid;
  // key < minkey
  if (comparator(key, KeyAt(1)) < 0) {
    return ValueAt(0);
  }
  // key >= maxkey
  if (comparator(key, KeyAt(high)) >= 0) {
    return ValueAt(high);
  }

  while (low <= high) {
    mid = (low + high) / 2;
    if (comparator(key, KeyAt(mid)) == 0) {
      return array_[mid].second;
    } else if (comparator(key, KeyAt(mid)) > 0) {
      low = mid + 1;
    } else {
      high = mid - 1;
    }
  }
  return ValueAt(high);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  if (GetSize() == 0) {
    array_[0].second = old_value;
    array_[1] = {new_key, new_value};
    IncreaseSize(2);
  }
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  for (int i = GetSize() - 1; i >= 0; i--) {
    if (array_[i].second == old_value) {
      array_[i + 1] = {new_key, new_value};
      IncreaseSize(1);
      break;
    }
    array_[i + 1] = array_[i];
  }
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  int minsize = GetMinSize() < 2 ? 2 : GetMinSize();
  int half = GetSize() - minsize;
  // 移除后半部分
  recipient->CopyNFrom(array_ + minsize, half, buffer_pool_manager);
  IncreaseSize(-1 * half);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  for (int i = GetSize(); i < GetSize() + size; i++) {
    Page *page = buffer_pool_manager->FetchPage(items->second);
    if (page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "all pages are pinned while CopyNFrom");
    }
    array_[i] = *items++;
    BPlusTreePage *child = reinterpret_cast<BPlusTreePage *>(page->GetData());
    // 修改孩子页的父亲页id为自己
    child->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(child->GetPageId(), true);
  }
  IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  if (index < 0 && index >= GetSize()) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "OUT_OF_RANGE");
  }

  for (int i = index; i < GetSize() - 1; i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  SetSize(0);
  return ValueAt(0);
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  // 向左兄弟转移
  // 设置separation key
  SetKeyAt(0, middle_key);

  // 移动并修改孩子节点的父亲页id
  recipient->CopyNFrom(array_, GetSize(), buffer_pool_manager);
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  // // get the first pair
  // MappingType pair = {KeyAt(1), ValueAt(0)};
  // array_[0].second = ValueAt(1);

  // // get pair's parent page
  // Page *page = buffer_pool_manager->FetchPage(GetParentPageId());
  // if (page == nullptr) {
  //   throw Exception(ExceptionType::OUT_OF_MEMORY, "all pages are pinned while MoveFirstToEndOf");
  // }
  // BPlusTreeInternalPage *parent = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
  // int index = parent->ValueIndex(GetPageId());

  // // 修改父节点key
  // parent->SetKeyAt(index, pair.first);
  // buffer_pool_manager->UnpinPage(parent->GetPageId(), true);

  // // copy pair to recipient
  // pair.first = middle_key;
  // recipient->CopyLastFrom(pair, buffer_pool_manager);

  // Remove(1);

  // 此函数不对父节点修改，父节点的修改在Redistribute()
  MappingType pair = {middle_key, ValueAt(0)};
  recipient->CopyLastFrom(pair, buffer_pool_manager);
  Remove(0);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  // move
  array_[GetSize()] = pair;

  // 对孩子节点的父页id修改
  Page *page = buffer_pool_manager->FetchPage(ValueAt(GetSize()));
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "all pages are pinned while CopyLastFrom");
  }
  auto child = reinterpret_cast<BPlusTreePage *>(page->GetData());
  child->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(child->GetPageId(), true);

  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  // // get the last pair
  // int last = GetSize() - 1;
  // MappingType pair = array_[last];

  // //get pair's parent page
  // Page *page = buffer_pool_manager->FetchPage(GetParentPageId());
  // if (page == nullptr) {
  //   throw Exception(ExceptionType::OUT_OF_MEMORY, "all pages are pinned while MoveLastToFrontOf");
  // }
  // BPlusTreeInternalPage *parent = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
  // int index = parent->ValueIndex(GetPageId()) + 1;
  // // get middle_key and modify
  // //middle_key = parent->KeyAt(index);
  // parent->SetKeyAt(index, pair.first);
  // buffer_pool_manager->UnpinPage(parent->GetPageId(), true);

  // // copy pair to recipient
  // pair.first = middle_key;
  // recipient->CopyFirstFrom(pair, buffer_pool_manager);
  // Remove(last);

  // 此函数不对父节点修改，父节点的修改在Redistribute()
  MappingType pair = {middle_key, ValueAt(GetSize())};
  recipient->CopyFirstFrom(pair, buffer_pool_manager);
  IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  // move
  for (int i = GetSize() - 1; i >= 0; i--) {
    array_[i + 1] = array_[i];
  }
  array_[0] = pair;

  // 对孩子节点的父页id修改
  Page *page = buffer_pool_manager->FetchPage(ValueAt(0));
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "all pages are pinned while CopyFirstFrom");
  }
  auto child = reinterpret_cast<BPlusTreePage *>(page->GetData());
  child->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(child->GetPageId(), true);

  IncreaseSize(1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
