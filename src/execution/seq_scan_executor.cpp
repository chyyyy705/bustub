//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
            : AbstractExecutor(exec_ctx), plan_(plan), cur_iter_(nullptr, RID(), nullptr) {
    tablemeta_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
}

void SeqScanExecutor::Init() {
    cur_iter_ = tablemeta_->table_->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
    const Schema *output_schema = plan_->OutputSchema();
    while (cur_iter_ != tablemeta_->table_->End()) {
        auto temp = cur_iter_++;
        // 判断当前扫描的tuple是否满足predicate条件
        auto value = plan_->GetPredicate()->Evaluate(&(*temp), &tablemeta_->schema_);
        if (value.GetAs<bool>()) {
            std::vector<Value> values;
            values.reserve(output_schema->GetColumnCount());
            for (const Column &column : output_schema->GetColumns()) {
                values.emplace_back(column.GetExpr()->Evaluate(&(*temp), &tablemeta_->schema_));
            }
            *tuple = Tuple(values, output_schema);
            *rid = temp->GetRid();
            return true;
        }
    }
    
    return false; 
}

}  // namespace bustub
