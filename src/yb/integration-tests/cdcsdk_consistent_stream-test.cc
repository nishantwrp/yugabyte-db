// Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.

#include "yb/integration-tests/cdcsdk_ysql_test_base.h"

namespace yb {
namespace cdc {

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestCDCSDKConsistentStreamWithManyTransactions)) {
  FLAGS_cdc_max_stream_intent_records = 40;

  ASSERT_OK(SetUpWithParams(3, 1, false));
  auto table = ASSERT_RESULT(CreateTable(&test_cluster_, kNamespaceName, kTableName));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table, 0, &tablets, nullptr));
  ASSERT_EQ(tablets.size(), 1);
  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream());
  auto set_resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets, OpId::Min()));
  ASSERT_FALSE(set_resp.has_error());

  int num_batches = 150;
  int inserts_per_batch = 100;

  std::thread t1(
      [&]() -> void { PerformSingleAndMultiShardInserts(num_batches, inserts_per_batch, true); });
  std::thread t2([&]() -> void {
    PerformSingleAndMultiShardInserts(
        num_batches, inserts_per_batch, true, num_batches * inserts_per_batch);
  });

  t1.join();
  t2.join();

  ASSERT_OK(test_client()->FlushTables({table.table_id()}, false, 1000, false));

  // Wait for all transactions to be applied.
  SleepFor(MonoDelta::FromSeconds(5));

  // The count array stores counts of DDL, INSERT, UPDATE, DELETE, READ, TRUNCATE, BEGIN, COMMIT in
  // that order.
  const int expected_count[] = {
      1, 2 * num_batches * inserts_per_batch, 0, 0, 0, 0, 2 * num_batches, 2 * num_batches,
  };
  int count[] = {0, 0, 0, 0, 0, 0, 0, 0};

  auto get_changes_resp = GetAllPendingChangesFromCdc(stream_id, tablets);
  for (auto record : get_changes_resp.records) {
    UpdateRecordCount(record, count);
  }

  CheckRecordsConsistency(get_changes_resp.records);
  LOG(INFO) << "Got " << get_changes_resp.records.size() << " records.";
  for (int i = 0; i < 8; i++) {
    ASSERT_EQ(expected_count[i], count[i]);
  }
  ASSERT_EQ(30601, get_changes_resp.records.size());
}

TEST_F(CDCSDKYsqlTest, YB_DISABLE_TEST_IN_TSAN(TestCDCSDKConsistentStreamWithForeignKeys)) {
  FLAGS_cdc_max_stream_intent_records = 30;
  ASSERT_OK(SetUpWithParams(3, 1, false));

  auto conn = ASSERT_RESULT(test_cluster_.ConnectToDB(kNamespaceName));
  ASSERT_OK(
      conn.Execute("CREATE TABLE test1(id int primary key, value_1 int) SPLIT INTO 1 TABLETS"));
  ASSERT_OK(
      conn.Execute("CREATE TABLE test2(id int primary key, value_2 int, test1_id int, CONSTRAINT "
                   "fkey FOREIGN KEY(test1_id) REFERENCES test1(id)) SPLIT INTO 1 TABLETS"));

  auto table1 = ASSERT_RESULT(GetTable(&test_cluster_, kNamespaceName, "test1"));
  auto table2 = ASSERT_RESULT(GetTable(&test_cluster_, kNamespaceName, "test2"));
  google::protobuf::RepeatedPtrField<master::TabletLocationsPB> tablets;
  ASSERT_OK(test_client()->GetTablets(table2, 0, &tablets, nullptr));
  ASSERT_EQ(tablets.size(), 1);

  CDCStreamId stream_id = ASSERT_RESULT(CreateDBStream());
  auto set_resp = ASSERT_RESULT(SetCDCCheckpoint(stream_id, tablets, OpId::Min()));
  ASSERT_FALSE(set_resp.has_error());

  ASSERT_OK(conn.Execute("INSERT INTO test1 VALUES (1, 1)"));
  ASSERT_OK(conn.Execute("INSERT INTO test1 VALUES (2, 2)"));

  int queries_per_batch = 60;
  int num_batches = 60;
  std::thread t1([&]() -> void {
    PerformSingleAndMultiShardQueries(
        num_batches, queries_per_batch, "INSERT INTO test2 VALUES ($0, 1, 1)", true);
  });
  std::thread t2([&]() -> void {
    PerformSingleAndMultiShardQueries(
        num_batches, queries_per_batch, "INSERT INTO test2 VALUES ($0, 1, 1)", true,
        num_batches * queries_per_batch);
  });

  t1.join();
  t2.join();

  std::thread t3([&]() -> void {
    PerformSingleAndMultiShardQueries(
        num_batches, queries_per_batch, "UPDATE test2 SET test1_id=2 WHERE id = $0", false);
  });
  std::thread t4([&]() -> void {
    PerformSingleAndMultiShardQueries(
        num_batches, queries_per_batch, "UPDATE test2 SET test1_id=2 WHERE id = $0", false,
        num_batches * queries_per_batch);
  });

  t3.join();
  t4.join();

  ASSERT_OK(test_client()->FlushTables({table1.table_id()}, false, 1000, false));
  ASSERT_OK(test_client()->FlushTables({table2.table_id()}, false, 1000, false));

  // Wait for all transactions to be applied.
  SleepFor(MonoDelta::FromSeconds(5));

  // The count array stores counts of DDL, INSERT, UPDATE, DELETE, READ, TRUNCATE, BEGIN, COMMIT in
  // that order.
  const int expected_count[] = {
      1, queries_per_batch * num_batches * 2,       queries_per_batch * num_batches * 2,       0, 0,
      0, num_batches * (4 + 2 * queries_per_batch), num_batches * (4 + 2 * queries_per_batch),
  };
  int count[] = {0, 0, 0, 0, 0, 0, 0, 0};

  auto get_changes_resp = GetAllPendingChangesFromCdc(stream_id, tablets);
  for (auto record : get_changes_resp.records) {
    UpdateRecordCount(record, count);
  }

  CheckRecordsConsistency(get_changes_resp.records);

  LOG(INFO) << "Got " << get_changes_resp.records.size() << " records.";
  for (int i = 0; i < 8; i++) {
    ASSERT_EQ(expected_count[i], count[i]);
  }
  ASSERT_EQ(29281, get_changes_resp.records.size());
}

}  // namespace cdc
}  // namespace yb
