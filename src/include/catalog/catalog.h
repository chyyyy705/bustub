#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/schema.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/index/index.h"
#include "storage/table/table_heap.h"

namespace bustub {

/**
 * Typedefs
 */
using table_oid_t = uint32_t;
using column_oid_t = uint32_t;
using index_oid_t = uint32_t;

/**
 * Metadata about a table.
 */
struct TableMetadata {
  TableMetadata(Schema schema, std::string name, std::unique_ptr<TableHeap> &&table, table_oid_t oid)
      : schema_(std::move(schema)), name_(std::move(name)), table_(std::move(table)), oid_(oid) {}
  Schema schema_;
  std::string name_;
  std::unique_ptr<TableHeap> table_;
  table_oid_t oid_;
};

/**
 * Metadata about a index
 */
struct IndexInfo {
  IndexInfo(Schema key_schema, std::string name, std::unique_ptr<Index> &&index, index_oid_t index_oid,
            std::string table_name, size_t key_size)
      : key_schema_(std::move(key_schema)),
        name_(std::move(name)),
        index_(std::move(index)),
        index_oid_(index_oid),
        table_name_(std::move(table_name)),
        key_size_(key_size) {}
  Schema key_schema_;
  std::string name_;
  std::unique_ptr<Index> index_;
  index_oid_t index_oid_;
  std::string table_name_;
  const size_t key_size_;
};

/**
 * Catalog is a non-persistent catalog that is designed for the executor to use.
 * It handles table creation and table lookup.
 */
class Catalog {
 public:
  /**
   * Creates a new catalog object.
   * @param bpm the buffer pool manager backing tables created by this catalog
   * @param lock_manager the lock manager in use by the system
   * @param log_manager the log manager in use by the system
   */
  Catalog(BufferPoolManager *bpm, LockManager *lock_manager, LogManager *log_manager)
      : bpm_{bpm}, lock_manager_{lock_manager}, log_manager_{log_manager} {}

  /**
   * Create a new table and return its metadata.
   * @param txn the transaction in which the table is being created
   * @param table_name the name of the new table
   * @param schema the schema of the new table
   * @return a pointer to the metadata of the new table
   */
  TableMetadata *CreateTable(Transaction *txn, const std::string &table_name, const Schema &schema) {
    BUSTUB_ASSERT(names_.count(table_name) == 0, "Table names should be unique!");

    // construct the table heap
    auto table = std::make_unique<TableHeap>(bpm_, lock_manager_, log_manager_, txn);

    // fetch the table OID for the new table
    const table_oid_t table_oid = next_table_oid_.fetch_add(1);

    // construct the table metadata
    auto meta = std::make_unique<TableMetadata>(schema, table_name, std::move(table), table_oid);
    TableMetadata *metadata = meta.get();

    // update the internal map
    names_.emplace(table_name, table_oid);
    tables_.emplace(table_oid, std::move(meta));
    index_names_.emplace(table_name, std::unordered_map<std::string, index_oid_t>{});

    return metadata;
  }

  /** @return table metadata by name */
  TableMetadata *GetTable(const std::string &table_name) {
    if (names_.find(table_name) == names_.end()) {
      return nullptr;
    }

    table_oid_t table_oid = names_[table_name];
    TableMetadata *metadata = tables_[table_oid].get();

    return metadata; 
  }

  /** @return table metadata by oid */
  TableMetadata *GetTable(table_oid_t table_oid) {
    if (tables_.find(table_oid) == tables_.end()) {
      return nullptr;
    }

    return tables_[table_oid].get();
  }

  /**
   * Create a new index, populate existing data of the table and return its metadata.
   * @param txn the transaction in which the table is being created
   * @param index_name the name of the new index
   * @param table_name the name of the table
   * @param schema the schema of the table
   * @param key_schema the schema of the key
   * @param key_attrs key attributes
   * @param keysize size of the key
   * @return a pointer to the metadata of the new table
   */
  template <class KeyType, class ValueType, class KeyComparator>
  IndexInfo *CreateIndex(Transaction *txn, const std::string &index_name, const std::string &table_name,
                         const Schema &schema, const Schema &key_schema, const std::vector<uint32_t> &key_attrs,
                         size_t keysize) {
    // table must exist before an index
    if (names_.find(table_name) == names_.end()) {
      return nullptr;
    }

    // If the table exists, an entry for the table should already be present in index_names_
    BUSTUB_ASSERT((index_names_.find(table_name) != index_names_.end()), "Broken Invariant");

    // check if index already exists for this table
    auto &table_index = index_names_.find(table_name)->second;
    if (table_index.find(index_name) != table_index.end()) {
      return nullptr;
    }

    // construct the index metadata
    auto meta = std::make_unique<IndexMetadata>(index_name, table_name, &schema, key_attrs);

    // construct the bplustree index
    auto index = std::make_unique<BPlusTreeIndex<KeyType, ValueType, KeyComparator>>(meta.get(), bpm_);
    
    // populate the index info with all tuples in table heap
    TableMetadata *table_meta = GetTable(table_name);
    TableHeap *heap = table_meta->table_.get();
    for (auto tuple = heap->Begin(txn); tuple != heap->End(); tuple++) {
      index->InsertEntry(tuple->KeyFromTuple(schema, key_schema, key_attrs), tuple->GetRid(), txn);
    }

    // get the next index oid for the new index
    const index_oid_t index_oid = next_index_oid_.fetch_add(1);

    // construct the index info
    auto info = std::make_unique<IndexInfo>(key_schema, index_name, std::move(index), index_oid, table_name, keysize);
    IndexInfo *index_info = info.get();

    // update the internal map
    indexes_.emplace(index_oid, std::move(info));
    table_index.emplace(index_name, index_oid);

    return index_info;
  }

  IndexInfo *GetIndex(const std::string &index_name, const std::string &table_name) {
    auto table = index_names_.find(table_name);
    if (table == index_names_.end()) {
      return nullptr;
    }

    auto &table_indexes = table->second;

    auto index_meta = table_indexes.find(index_name);
    if (index_meta == table_indexes.end()) {
      return nullptr;
    }

    auto index = indexes_.find(index_meta->second);

    return index->second.get();
  }

  IndexInfo *GetIndex(index_oid_t index_oid) {
    auto index = indexes_.find(index_oid);
    if (index == indexes_.end()) {
      return nullptr;
    }
    return index->second.get();
  }

  std::vector<IndexInfo *> GetTableIndexes(const std::string &table_name) {
    // ensure the table exists
    if (names_.find(table_name) == names_.end()) {
      return std::vector<IndexInfo *>();
    }

    auto table_indexes = index_names_.find(table_name);
    BUSTUB_ASSERT(table_indexes != index_names_.end(), "Broken Invariant");

    std::vector<IndexInfo *> indexes{};
    indexes.reserve(table_indexes->second.size());
    for (const auto &index_info : table_indexes->second) {
      auto index = indexes_.find(index_info.second);
      indexes.push_back(index->second.get());
    }

    return indexes;
  }

 private:
  [[maybe_unused]] BufferPoolManager *bpm_;
  [[maybe_unused]] LockManager *lock_manager_;
  [[maybe_unused]] LogManager *log_manager_;

  /** tables_ : table identifiers -> table metadata. Note that tables_ owns all table metadata. */
  std::unordered_map<table_oid_t, std::unique_ptr<TableMetadata>> tables_;
  /** names_ : table names -> table identifiers */
  std::unordered_map<std::string, table_oid_t> names_;
  /** The next table identifier to be used. */
  std::atomic<table_oid_t> next_table_oid_{0};
  /** indexes_: index identifiers -> index metadata. Note that indexes_ owns all index metadata */
  std::unordered_map<index_oid_t, std::unique_ptr<IndexInfo>> indexes_;
  /** index_names_: table name -> index names -> index identifiers */
  std::unordered_map<std::string, std::unordered_map<std::string, index_oid_t>> index_names_;
  /** The next index identifier to be used */
  std::atomic<index_oid_t> next_index_oid_{0};
};
}  // namespace bustub
