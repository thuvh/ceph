// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <librdkafka/rdkafka.h>
#include <string>

namespace kafka_mock {

// Configure the broker address that the mock treats as "valid".
// Connects to any other broker URL will simply be ignored by the mock
// (no producer allocation happens there), letting callers model
// name-resolution / configuration failures.
void set_valid_broker(const std::string& broker);

// Force the next rd_kafka_topic_new() call to return nullptr.
// When set, rd_kafka_last_error() returns `topic_new_fail_err`.
// This models the broker rejecting topic creation, for example when
// the topic already exists with conflicting ACLs (see tracker #68033 /
// #69566). The flag is auto-cleared after the failed call so a test
// only fails one topic creation.
extern bool FAIL_NEXT_TOPIC_NEW;
extern rd_kafka_resp_err_t TOPIC_NEW_FAIL_ERR;

// Force the next rd_kafka_produce() call to return -1.
// When set, rd_kafka_last_error() returns `produce_fail_err`.
// The flag is auto-cleared after the failed call.
extern bool FAIL_NEXT_PRODUCE;
extern rd_kafka_resp_err_t PRODUCE_FAIL_ERR;

// When rd_kafka_poll() is invoked with a queued produced message,
// deliver an ack (err == 0) when true, otherwise deliver a nack.
extern bool REPLY_ACK;

// Reset all controllable knobs to their defaults.
void reset();

}
