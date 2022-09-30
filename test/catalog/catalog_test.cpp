//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// catalog_test.cpp
//
// Identification: test/catalog/catalog_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <unordered_set>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "gtest/gtest.h"
#include "type/value_factory.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(CatalogTest, DISABLED_CreateTableTest) {
  auto disk_manager = new DiskManager("catalog_test.db");
  auto bpm = new BufferPoolManager(32, disk_manager);
  auto catalog = new Catalog(bpm, nullptr, nullptr);
  std::string table_name = "potato";

  // The table shouldn't exist in the catalog yet.
  EXPECT_THROW(catalog->GetTable(table_name), std::out_of_range);

  // Put the table into the catalog.
  std::vector<Column> columns;
  columns.emplace_back("A", TypeId::INTEGER);
  columns.emplace_back("B", TypeId::BOOLEAN);

  Schema schema(columns);
  auto *table_metadata = catalog->CreateTable(nullptr, table_name, schema);
  (void)table_metadata;

  // Notice that this test case doesn't check anything! :(
  // It is up to you to extend it

  delete catalog;
  delete bpm;
  delete disk_manager;

  remove("catalog_test.db");
  remove("catalog_test.log");
}

TEST(CatalogTest, CreateTable) {
  auto disk_manager = std::make_unique<DiskManager>("catalog_test.db");
  auto bpm = std::make_unique<BufferPoolManager>(32, disk_manager.get());
  auto catalog = std::make_unique<Catalog>(bpm.get(), nullptr, nullptr);

  const std::string table_name{"foobar"};

  // The table shouldn't exist in the catalog yet
  EXPECT_EQ(nullptr, catalog->GetTable(table_name));

  // Construct a new table and add it to the catalog
  std::vector<Column> columns{};
  columns.emplace_back("A", TypeId::INTEGER);
  columns.emplace_back("B", TypeId::BOOLEAN);

  // Table creation should succeed
  Schema schema{columns};
  auto *table_info = catalog->CreateTable(nullptr, table_name, schema);
  EXPECT_NE(nullptr, table_info);

  // Querying the table name should now succeed
  EXPECT_NE(nullptr, catalog->GetTable(table_name));

  // Querying the table OID should also succeed
  const auto table_oid = table_info->oid_;
  EXPECT_NE(nullptr, catalog->GetTable(table_oid));

  remove("catalog_test.db");
  remove("catalog_test.log");
}

TEST(CatalogTest, CreateIndex) {
  auto disk_manager = std::make_unique<DiskManager>("catalog_test.db");
  auto bpm = std::make_unique<BufferPoolManager>(32, disk_manager.get());
  auto catalog = std::make_unique<Catalog>(bpm.get(), nullptr, nullptr);
  auto txn = std::make_unique<Transaction>(0);

  const std::string table_name{"foobar"};
  const std::string index_name{"index1"};

  // Construct a new table and add it to the catalog
  std::vector<Column> columns{};
  columns.emplace_back("A", TypeId::BIGINT);
  columns.emplace_back("B", TypeId::BOOLEAN);
  Schema schema{columns};
  EXPECT_NE(nullptr, catalog->CreateTable(nullptr, table_name, schema));

  // No indexes should exist for the table
  const auto table_indexes1 = catalog->GetTableIndexes(table_name);
  EXPECT_TRUE(table_indexes1.empty());

  // Construction of an index for the table should succeed
  std::vector<Column> key_columns_A{};
  std::vector<uint32_t> key_attrs_A{};
  key_columns_A.emplace_back("A", TypeId::BIGINT);
  key_attrs_A.emplace_back(0);

  Schema key_schema_A{key_columns_A};

  // Index construction should succeed
  auto *index_A = catalog->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(
      txn.get(), index_name, table_name, schema, key_schema_A, key_attrs_A, 8);
  EXPECT_NE(nullptr, index_A);

  // Querying the table indexes should return our index
  const auto table_indexes2 = catalog->GetTableIndexes(table_name);
  EXPECT_EQ(table_indexes2.size(), 1);

  std::vector<Column> key_columns_B{};
  std::vector<uint32_t> key_attrs_B{};
  key_columns_A.emplace_back("B", TypeId::BOOLEAN);
  key_attrs_A.emplace_back(2);

  Schema key_schema_B{key_columns_B};

  auto *index_B = catalog->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(
      txn.get(), "index2", table_name, schema, key_schema_B, key_attrs_B, 8);
  EXPECT_NE(nullptr, index_B);

  const auto table_indexes3 = catalog->GetTableIndexes(table_name);
  EXPECT_EQ(table_indexes3.size(), 2);


  remove("catalog_test.db");
  remove("catalog_test.log");
}


}  // namespace bustub
