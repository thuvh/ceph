// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

// Unit tests for the RGW Kafka notification endpoint (rgw_kafka.{h,cc}).
//
// Uses the kafka_mock library to exercise the librdkafka call paths in
// rgw_kafka without a live broker. In particular, this covers the
// topic-creation failure branch that used to crash when rd_kafka_topic_name()
// was called on a null topic handle returned by rd_kafka_topic_new() (see
// tracker #68033, and the regression test scenario for #69566).

#include "rgw_kafka.h"
#include "kafka_mock.h"

#include "common/ceph_context.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

using namespace rgw;

const std::chrono::milliseconds wait_time(10);

class CctCleaner {
  CephContext* cct;
public:
  explicit CctCleaner(CephContext* _cct) : cct(_cct) {}
  ~CctCleaner() {
#ifdef WITH_CRIMSON
    delete cct;
#else
    cct->put();
#endif
  }
};

auto cct = new CephContext(CEPH_ENTITY_TYPE_CLIENT);
CctCleaner cleaner(cct);

class TestKafka : public ::testing::Test {
protected:
  kafka::connection_id_t conn_id;
  unsigned current_dequeued = 0U;

  void SetUp() override {
    kafka_mock::reset();
    ASSERT_TRUE(kafka::init(cct));
    current_dequeued = kafka::get_dequeued();
  }

  void TearDown() override {
    kafka::shutdown();
    kafka_mock::reset();
  }

  // wait for at least one new (since last drain) message to be dequeued
  // and then wait for all pending answers to be received
  void wait_until_drained() {
    const auto deadline = std::chrono::steady_clock::now()
                          + std::chrono::seconds(30);
    while (kafka::get_dequeued() == current_dequeued) {
      ASSERT_LT(std::chrono::steady_clock::now(), deadline)
          << "timed out waiting for a message to be dequeued";
      std::this_thread::sleep_for(wait_time);
    }
    while (kafka::get_inflight() > 0) {
      ASSERT_LT(std::chrono::steady_clock::now(), deadline)
          << "timed out waiting for inflight messages to clear";
      std::this_thread::sleep_for(wait_time);
    }
    current_dequeued = kafka::get_dequeued();
  }
};

std::atomic<bool> callback_invoked(false);
std::atomic<int> callback_rc(0);

void my_callback(int rc) {
  callback_rc = rc;
  callback_invoked = true;
}

// happy path: connect + publish without a callback
TEST_F(TestKafka, ConnectionOK) {
  const auto connection_number = kafka::get_connection_count();
  auto rc = kafka::connect(conn_id, "kafka://localhost:9092", false, false,
                           boost::none, boost::none, boost::none,
                           boost::none, boost::none, boost::none,
                           boost::none, boost::none, boost::none,
                           boost::none, boost::none, boost::none);
  EXPECT_TRUE(rc);
  EXPECT_EQ(kafka::get_connection_count(), connection_number + 1);
  rc = kafka::publish(conn_id, "topic", "message");
  EXPECT_EQ(rc, 0);
}

// a second connect() to the same endpoint reuses the existing connection
TEST_F(TestKafka, ConnectionReuse) {
  kafka::connection_id_t conn_id1;
  auto rc = kafka::connect(conn_id1, "kafka://localhost:9092", false, false,
                           boost::none, boost::none, boost::none,
                           boost::none, boost::none, boost::none,
                           boost::none, boost::none, boost::none,
                           boost::none, boost::none, boost::none);
  EXPECT_TRUE(rc);
  const auto connection_number = kafka::get_connection_count();
  kafka::connection_id_t conn_id2;
  rc = kafka::connect(conn_id2, "kafka://localhost:9092", false, false,
                      boost::none, boost::none, boost::none,
                      boost::none, boost::none, boost::none,
                      boost::none, boost::none, boost::none,
                      boost::none, boost::none, boost::none);
  EXPECT_TRUE(rc);
  EXPECT_EQ(kafka::get_connection_count(), connection_number);
  rc = kafka::publish(conn_id1, "topic", "message");
  EXPECT_EQ(rc, 0);
}

// bogus URL scheme: connect() returns false and no connection is created
TEST_F(TestKafka, URLParseError) {
  kafka::connection_id_t local_conn_id;
  const auto connection_number = kafka::get_connection_count();
  auto rc = kafka::connect(local_conn_id, "http://localhost:9092", false, false,
                           boost::none, boost::none, boost::none,
                           boost::none, boost::none, boost::none,
                           boost::none, boost::none, boost::none,
                           boost::none, boost::none, boost::none);
  EXPECT_FALSE(rc);
  EXPECT_EQ(kafka::get_connection_count(), connection_number);
}

// ---------------------------------------------------------------------------
// regression coverage for tracker #68033 / #69566.
//
// rd_kafka_topic_new() can return NULL when the broker rejects topic
// creation -- most notably when the topic already exists with conflicting
// ACLs. Before the fix in PR #59741, rgw_kafka.cc derived the topic name via
// rd_kafka_topic_name() on the (NULL) handle and crashed. The fix refactored
// topic bookkeeping to key on the in-memory topic map by name, and to invoke
// the publishing callback with a negative errno when topic creation fails.
// This test exercises the post-fix path so that any regression that reverts
// to touching the NULL topic handle will surface.
// ---------------------------------------------------------------------------
TEST_F(TestKafka, TopicCreationFailure) {
  // simulate the broker rejecting topic creation because the topic was
  // already created with ACLs that forbid us from using it.
  kafka_mock::FAIL_NEXT_TOPIC_NEW = true;
  kafka_mock::TOPIC_NEW_FAIL_ERR = RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS;

  callback_invoked = false;
  callback_rc = 0;

  auto rc = kafka::connect(conn_id, "kafka://localhost:9092", false, false,
                           boost::none, boost::none, boost::none,
                           boost::none, boost::none, boost::none,
                           boost::none, boost::none, boost::none,
                           boost::none, boost::none, boost::none);
  ASSERT_TRUE(rc);

  // publish_with_confirm enqueues to the manager thread; publish_internal
  // will call rd_kafka_topic_new(), the mock returns NULL, and the callback
  // must be invoked synchronously with a *negative* errno and no crash.
  rc = kafka::publish_with_confirm(conn_id, "topic-with-acl", "message",
                                   my_callback);
  EXPECT_EQ(rc, 0);
  wait_until_drained();
  EXPECT_TRUE(callback_invoked) << "callback was not invoked on topic creation failure";
  EXPECT_LT(callback_rc.load(), 0)
      << "expected negative errno on topic creation failure, got "
      << callback_rc.load();

  // flag was auto-reset by the mock, so a follow-up publish on a fresh
  // topic should succeed normally (no crash, callback invoked with 0).
  callback_invoked = false;
  callback_rc = 0;
  rc = kafka::publish_with_confirm(conn_id, "topic-other", "message",
                                   my_callback);
  EXPECT_EQ(rc, 0);
  wait_until_drained();
  EXPECT_TRUE(callback_invoked);
  EXPECT_EQ(callback_rc.load(), 0);
}

// rd_kafka_produce() can also fail; ensure that's surfaced to the callback
// as a negative errno rather than crashing.
TEST_F(TestKafka, ProduceFailure) {
  kafka_mock::FAIL_NEXT_PRODUCE = true;
  kafka_mock::PRODUCE_FAIL_ERR = RD_KAFKA_RESP_ERR__MSG_TIMED_OUT;

  callback_invoked = false;
  callback_rc = 0;

  auto rc = kafka::connect(conn_id, "kafka://localhost:9092", false, false,
                           boost::none, boost::none, boost::none,
                           boost::none, boost::none, boost::none,
                           boost::none, boost::none, boost::none,
                           boost::none, boost::none, boost::none);
  ASSERT_TRUE(rc);

  rc = kafka::publish_with_confirm(conn_id, "topic", "message", my_callback);
  EXPECT_EQ(rc, 0);
  wait_until_drained();
  EXPECT_TRUE(callback_invoked);
  EXPECT_LT(callback_rc.load(), 0);
}

// successful produce path: the broker confirms receipt, the callback fires
// with rc == 0.
TEST_F(TestKafka, ReceiveAck) {
  callback_invoked = false;
  callback_rc = 0;

  auto rc = kafka::connect(conn_id, "kafka://localhost:9092", false, false,
                           boost::none, boost::none, boost::none,
                           boost::none, boost::none, boost::none,
                           boost::none, boost::none, boost::none,
                           boost::none, boost::none, boost::none);
  ASSERT_TRUE(rc);

  kafka_mock::REPLY_ACK = true;
  rc = kafka::publish_with_confirm(conn_id, "topic", "message", my_callback);
  EXPECT_EQ(rc, 0);
  wait_until_drained();
  EXPECT_TRUE(callback_invoked);
  EXPECT_EQ(callback_rc.load(), 0);
}

// the broker can also nack; the callback must be invoked with rc < 0.
TEST_F(TestKafka, ReceiveNack) {
  callback_invoked = false;
  callback_rc = 0;

  auto rc = kafka::connect(conn_id, "kafka://localhost:9092", false, false,
                           boost::none, boost::none, boost::none,
                           boost::none, boost::none, boost::none,
                           boost::none, boost::none, boost::none,
                           boost::none, boost::none, boost::none);
  ASSERT_TRUE(rc);

  kafka_mock::REPLY_ACK = false;
  rc = kafka::publish_with_confirm(conn_id, "topic", "message", my_callback);
  EXPECT_EQ(rc, 0);
  wait_until_drained();
  EXPECT_TRUE(callback_invoked);
  EXPECT_LT(callback_rc.load(), 0);
}
