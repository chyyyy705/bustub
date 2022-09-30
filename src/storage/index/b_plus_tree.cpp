//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  Page *leaf_page = FindLeafPage(key, false);
  // 首先找到叶子节点
  if (leaf_page != nullptr) {
    ValueType value{};
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());

    bool is_exist = leaf_node->Lookup(key, &value, comparator_);
    // unlock and unpin
    leaf_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    // 最终找到value
    if (is_exist) {
      result->push_back(value);
      return true;
    }
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  {
    std::lock_guard<std::mutex> guard(root_latch_);
    if (IsEmpty()) {
      StartNewTree(key, value);
      return true;
    }
  }

  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  Page *page = buffer_pool_manager_->NewPage(&root_page_id_);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "all pages are pinned while StartNewTree");
  }
  LeafPage *root = reinterpret_cast<LeafPage *>(page->GetData());

  root->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  root->Insert(key, value, comparator_);

  buffer_pool_manager_->UnpinPage(root->GetPageId(), true);

  UpdateRootPageId(1);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  // 找到叶子
  // Page *page = FindLeafPage(key, false);
  auto [page, root_is_latched] = FindLeafPageByOperation(key, Operation::INSERT, transaction);
  LeafPage *leafp = reinterpret_cast<LeafPage *>(page->GetData());

  // if user try to insert duplicate keys
  ValueType vvalue{};
  if (leafp->Lookup(key, &vvalue, comparator_)) {
    UnlockUnpinPages(transaction);
    if (root_is_latched) {
      root_latch_.unlock();
      root_is_latched = false;
    }
    return false;
  }

  // insert or split
  int new_size = leafp->Insert(key, value, comparator_);
  if (new_size <= leafp->GetMaxSize()) {
    // 直接插入不需要分裂
    // unlock and upin
    if (root_is_latched) {
      root_latch_.unlock();
      root_is_latched = false;
    }
    // 有必要批量释放?
    // UnlockUnpinPages(transaction);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leafp->GetPageId(), true);
    return true;
  }

  // if new_size >= leafp->GetMaxSize()
  // split
  LeafPage *newleafp = Split(leafp);
  InsertIntoParent(leafp, newleafp->KeyAt(0), newleafp, transaction);

  if (root_is_latched) {
    root_latch_.unlock();
    root_is_latched = false;
  }

  // InsertIntoParent内部应该已经调用了UnlockUnpinPages(transaction)
  // 再次调用
  UnlockUnpinPages(transaction);
  // page->WUnlatch();
  buffer_pool_manager_->UnpinPage(newleafp->GetPageId(), true);

  assert(root_is_latched == false);

  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  // 新页不上锁，因此不需要解锁，但需要unpin
  page_id_t new_page_id = INVALID_PAGE_ID;
  Page *page = buffer_pool_manager_->NewPage(&new_page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "all pages are pinned while split");
  }
  N *new_node = reinterpret_cast<N *>(page->GetData());

  // 按情况分裂
  if (node->IsLeafPage()) {  // leaf page
    LeafPage *oldnode = reinterpret_cast<LeafPage *>(node);
    LeafPage *newnode = reinterpret_cast<LeafPage *>(page->GetData());
    // 初始化
    newnode->Init(new_page_id, oldnode->GetParentPageId(), leaf_max_size_);
    // 分裂
    oldnode->MoveHalfTo(newnode);
    // 更新叶子层链表
    newnode->SetNextPageId(oldnode->GetNextPageId());
    oldnode->SetNextPageId(newnode->GetPageId());
    new_node = reinterpret_cast<N *>(newnode);
  } else {  // internal page
    InternalPage *oldnode = reinterpret_cast<InternalPage *>(node);
    InternalPage *newnode = reinterpret_cast<InternalPage *>(page->GetData());
    // 初始化
    newnode->Init(new_page_id, oldnode->GetParentPageId(), internal_max_size_);
    // 分裂
    oldnode->MoveHalfTo(newnode, buffer_pool_manager_);
    new_node = reinterpret_cast<N *>(newnode);
  }

  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  // 根节点
  if (old_node->IsRootPage()) {  // create new root
    page_id_t new_root_page_id = INVALID_PAGE_ID;
    Page *parent = buffer_pool_manager_->NewPage(&new_root_page_id);
    if (parent == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "all pages are pinned while InsertIntoParent");
    }
    root_page_id_ = new_root_page_id;
    InternalPage *new_root = reinterpret_cast<InternalPage *>(parent->GetData());

    // 初始化并填充新根
    new_root->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);
    new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

    buffer_pool_manager_->UnpinPage(new_root->GetPageId(), true);

    // 更新根节点id
    UpdateRootPageId(0);

    // 修改old_node和new_node的父指针
    old_node->SetParentPageId(new_root_page_id);
    new_node->SetParentPageId(new_root_page_id);

    UnlockUnpinPages(transaction);
    return;
  }

  // 非根节点
  // insert into old_node's parent, first find it
  Page *page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "all pages are pinned while InsertIntoParent");
  }
  InternalPage *parent = reinterpret_cast<InternalPage *>(page->GetData());

  // 插入
  int new_size = parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  if (new_size <= parent->GetMaxSize()) {
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    UnlockUnpinPages(transaction);
    return;  //未溢出,结束
  }

  // split, if new_size > parent->GetMaxSize()
  InternalPage *new_parent = Split(parent);
  // 递归插入
  InsertIntoParent(parent, new_parent->KeyAt(0), new_parent, transaction);

  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_parent->GetPageId(), true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }

  // find the deletion target
  // Page *leaf = FindLeafPage(key, false);
  auto [leaf_page, root_is_latched] = FindLeafPageByOperation(key, Operation::DELETE, transaction);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  // assert(root_is_latched == true);

  int old_size = leaf_node->GetSize();
  int new_size = leaf_node->RemoveAndDeleteRecord(key, comparator_);

  // can't find key and return
  if (old_size == new_size) {
    UnlockUnpinPages(transaction);
    if (root_is_latched) {
      root_latch_.unlock();
      root_is_latched = false;
    }
    return;
  }

  if (CoalesceOrRedistribute(leaf_node, transaction)) {
    transaction->AddIntoDeletedPageSet(leaf_page->GetPageId());
  }

  UnlockUnpinPages(transaction);

  if (root_is_latched) {
    root_latch_.unlock();
    root_is_latched = false;
  }

  assert(root_is_latched == false);

  for (page_id_t page_id : *transaction->GetDeletedPageSet()) {
    buffer_pool_manager_->DeletePage(page_id);
  }
  transaction->GetDeletedPageSet()->clear();
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  bool should_delete = false;

  // 需要调整的节点是根
  if (node->IsRootPage()) {
    should_delete = AdjustRoot(node);
    UnlockUnpinPages(transaction);
    return should_delete;
  }

  // 不需要合并和再分配
  if (node->GetSize() >= node->GetMinSize()) {
    UnlockUnpinPages(transaction);
    return should_delete;
  }

  // find parent page
  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  if (parent_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY,
                    "all pages are pinned while CoalesceOrRedistribute--Fetch parentpage");
  }
  InternalPage *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());

  // 找到node在父节点的位置
  int index = parent->ValueIndex(node->GetPageId());
  // node已经是最左时，找右兄弟，否则找左兄弟
  page_id_t sibling_page_id = parent->ValueAt(index == 0 ? 1 : index - 1);
  Page *sibling_page = buffer_pool_manager_->FetchPage(sibling_page_id);
  if (sibling_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY,
                    "all pages are pinned while CoalesceOrRedistribute--Fectch siblingpage");
  }

  sibling_page->WLatch();  // 锁住兄弟节点

  N *sibling_node = reinterpret_cast<N *>(sibling_page->GetData());

  if (node->GetSize() + sibling_node->GetSize() > node->GetMaxSize()) {
    // 再分配，不必删除node
    Redistribute(sibling_node, node, index);
  } else {
    // 合并node和sibling_node到后者
    should_delete = Coalesce(&sibling_node, &node, &parent, index, transaction);
  }

  // parent_page unlock is finishdd in Remove;
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);

  sibling_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);

  return should_delete;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  if (index == 0) {
    // 为了调用MoveAllTo
    std::swap(node, neighbor_node);
    index = 1;
  }

  KeyType middle_key = (*parent)->KeyAt(index);

  // neighbor <--- node
  if ((*node)->IsLeafPage()) {
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(*node);
    LeafPage *neighbor_leaf_node = reinterpret_cast<LeafPage *>(*neighbor_node);

    leaf_node->MoveAllTo(neighbor_leaf_node);
    // transaction->AddIntoDeletedPageSet(leaf_node->GetPageId());
  } else {
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(*node);
    InternalPage *neighbor_internal_node = reinterpret_cast<InternalPage *>(*neighbor_node);

    internal_node->MoveAllTo(neighbor_internal_node, middle_key, buffer_pool_manager_);
    transaction->AddIntoDeletedPageSet(internal_node->GetPageId());
  }

  // 删除父节点的middle_key
  (*parent)->Remove(index);

  // 由于parent中删除了key-value,递归调用此函数
  return CoalesceOrRedistribute(*parent, transaction);
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  // 在node中已经删除了一个key,但不需要删除node
  // 我们需要对其及兄弟进行再分配
  // index表示node在父节点的位置
  // 首先找到父节点
  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  if (parent_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "all pages are pinned while Redistribute--Fetch parentpage");
  }
  InternalPage *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());

  // 分类讨论
  if (node->IsLeafPage()) {  // leaf page
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(node);
    LeafPage *neighbor_leaf_node = reinterpret_cast<LeafPage *>(neighbor_node);

    if (index == 0) {
      // node.end <--- neighbor.first
      neighbor_leaf_node->MoveFirstToEndOf(leaf_node);
      parent->SetKeyAt(index + 1, neighbor_leaf_node->KeyAt(0));
    } else {
      // neighbor.end ---> node.first
      neighbor_leaf_node->MoveLastToFrontOf(leaf_node);
      parent->SetKeyAt(index, leaf_node->KeyAt(0));
    }
  } else {  // internal page
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(node);
    InternalPage *neighbor_internal_node = reinterpret_cast<InternalPage *>(neighbor_node);

    if (index == 0) {
      // node.end <--- neighbor.first
      neighbor_internal_node->MoveFirstToEndOf(internal_node, parent->KeyAt(index + 1), buffer_pool_manager_);
      parent->SetKeyAt(index + 1, neighbor_internal_node->KeyAt(0));
    } else {
      // neighbor.end ---> node.first
      neighbor_internal_node->MoveLastToFrontOf(internal_node, parent->KeyAt(index), buffer_pool_manager_);
      parent->SetKeyAt(index, internal_node->KeyAt(0));
    }
  }

  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  // 当需要调整的节点是根节点的时候应当调用此函数
  bool root_should_delete = false;

  // case 1：根节点是内部节点，且最后的key已删除，只剩其左孩子
  // 将左孩子设置为root
  if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(old_root_node);
    page_id_t child_id = internal_node->RemoveAndReturnOnlyChild();

    // 修改根节点id
    root_page_id_ = child_id;
    UpdateRootPageId(0);

    Page *new_root = buffer_pool_manager_->FetchPage(root_page_id_);
    if (new_root == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "all pages are pinned while AdjustRoot");
    }
    BPlusTreePage *new_root_node = reinterpret_cast<BPlusTreePage *>(new_root->GetData());
    // 修改根节点的父节点id
    new_root_node->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(new_root->GetPageId(), true);

    root_should_delete = true;
  }

  // case 2：根节点为叶子节点，即整颗树，且已经清空
  if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);

    root_should_delete = true;
  }

  return root_should_delete;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  Page *leaf_page = FindLeafPage(KeyType(), true);

  // leaf_page会在迭代器虚构函数中释放 读锁
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  Page *leaf_page = FindLeafPage(key, false);
  LeafPage *leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int index = leaf->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() {
  Page *leaf_page = FindLeafPageByOperation(KeyType(), Operation::FIND, nullptr, false, true).first;
  LeafPage *leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page, leaf->GetSize());
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  return FindLeafPageByOperation(key, Operation::FIND, nullptr, leftMost, false).first;
}

// INDEX_TEMPLATE_ARGUMENTS
// Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
//   if (IsEmpty()) {
//     return nullptr;
//   }

//   // get the root
//   Page *root = buffer_pool_manager_->FetchPage(root_page_id_);
//   if (root == nullptr) {
//     throw Exception(ExceptionType::OUT_OF_MEMORY, "all pages are pinned while FindLeafPage");
//   }
//   auto *node = reinterpret_cast<BPlusTreePage*>(root->GetData());

//   while (!node->IsLeafPage()) {
//     InternalPage *internal = reinterpret_cast<InternalPage*>(node);
//     page_id_t child_page_id;
//     // find child
//     if (leftMost) {
//       child_page_id = internal->ValueAt(0);
//     } else {
//       child_page_id = internal->Lookup(key, comparator_);
//     }

//     auto *child = buffer_pool_manager_->FetchPage(child_page_id);
//     if (child == nullptr) {
//       throw Exception(ExceptionType::OUT_OF_MEMORY, "all pages are pinned while FindLeafPage");
//     }
//     // unpin
//     buffer_pool_manager_->UnpinPage(node->GetPageId(), false);

//     node = reinterpret_cast<BPlusTreePage*>(child->GetData());
//   }

//   return reinterpret_cast<Page *>(node);

// }

INDEX_TEMPLATE_ARGUMENTS
std::pair<Page *, bool> BPLUSTREE_TYPE::FindLeafPageByOperation(const KeyType &key, Operation operation,
                                                                Transaction *transaction, bool leftMost,
                                                                bool rightMost) {
  root_latch_.lock();
  // 返回值is_root_latched用于判断root_latch是否上锁
  bool is_root_latched = true;

  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "all pages are pinned while FindLeafPageByOperation");
  }

  // 获取读锁或写锁
  if (operation == Operation::FIND) {
    page->RLatch();
  } else {
    page->WLatch();
  }

  // 从上到下记录被上锁的页
  if (transaction != nullptr) {
    transaction->AddIntoPageSet(page);
  }

  // 向下查找
  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while (!node->IsLeafPage()) {
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(node);
    page_id_t child_page_id = INVALID_PAGE_ID;
    // find child
    if (leftMost) {
      child_page_id = internal_node->ValueAt(0);
    } else if (rightMost) {
      child_page_id = internal_node->ValueAt(internal_node->GetSize() - 1);
    } else {
      child_page_id = internal_node->Lookup(key, comparator_);
    }

    Page *child_page = buffer_pool_manager_->FetchPage(child_page_id);
    if (child_page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "all pages are pinned while FindLeafPageByOperation");
    }

    if (operation == Operation::FIND) {
      // 读是安全的
      // 获取孩子页读锁
      child_page->RLatch();
      if (is_root_latched) {
        root_latch_.unlock();
        is_root_latched = false;
      }
      // 释放当前页读锁
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    } else {
      // 插入或删除需要判断是否安全
      // 获取孩子页写锁
      child_page->WLatch();
      if (IsSafe(node, operation)) {
        // 该节点安全则释放 所有 父页 写锁
        UnlockUnpinPages(transaction);
        if (is_root_latched) {
          root_latch_.unlock();
          is_root_latched = false;
        }
      }

      if (transaction != nullptr) {
        transaction->AddIntoPageSet(child_page);
      }
    }

    BPlusTreePage *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());

    page = child_page;
    node = child_node;
  }

  // 根节点是叶子节点时，需要释放锁
  if (operation == Operation::FIND) {
    if (is_root_latched) {
      root_latch_.unlock();
      is_root_latched = false;
    }
  }

  return std::make_pair(page, is_root_latched);
}

// 插入或删除时判断node是否安全
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::IsSafe(N *node, Operation op) {
  // 根节点size可以小于minsize
  if (node->IsRootPage()) {
    return (op == Operation::INSERT && node->GetSize() < node->GetMaxSize()) ||
           (op == Operation::DELETE && node->GetSize() > 0);
  }

  if (op == Operation::INSERT) {
    return node->GetSize() < node->GetMaxSize();
  }

  if (op == Operation::DELETE) {
    return node->GetSize() > node->GetMinSize();
  }

  return true;
}

// 批量释放页的写锁
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockUnpinPages(Transaction *transaction) {
  if (transaction == nullptr) {
    return;
  }

  for (Page *page : *transaction->GetPageSet()) {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  }
  transaction->GetPageSet()->clear();
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
