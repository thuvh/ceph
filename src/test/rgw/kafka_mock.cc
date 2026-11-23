// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

// Kafka mock for unit tests.
//
// Provides link-time replacements for the librdkafka C API used by
// rgw_kafka.cc, with a small set of controllable knobs (see kafka_mock.h)
// so that unit tests can drive scenarios like topic-creation failure
// and produce failures without a live broker.

#include "kafka_mock.h"

#include <librdkafka/rdkafka.h>

#include <atomic>
#include <cstring>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace kafka_mock {

std::string VALID_BROKER("localhost:9092");

void set_valid_broker(const std::string& broker) {
  VALID_BROKER = broker;
}

bool FAIL_NEXT_TOPIC_NEW(false);
rd_kafka_resp_err_t TOPIC_NEW_FAIL_ERR(RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS);

bool FAIL_NEXT_PRODUCE(false);
rd_kafka_resp_err_t PRODUCE_FAIL_ERR(RD_KAFKA_RESP_ERR__MSG_TIMED_OUT);

bool REPLY_ACK(true);

void reset() {
  FAIL_NEXT_TOPIC_NEW = false;
  TOPIC_NEW_FAIL_ERR = RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS;
  FAIL_NEXT_PRODUCE = false;
  PRODUCE_FAIL_ERR = RD_KAFKA_RESP_ERR__MSG_TIMED_OUT;
  REPLY_ACK = true;
  VALID_BROKER = "localhost:9092";
}

}  // namespace kafka_mock

using namespace kafka_mock;

// ---------------------------------------------------------------------------
// opaque struct definitions backing the librdkafka opaque typedefs.
// rdkafka.h only forward-declares them, so we are free to define them here.
// ---------------------------------------------------------------------------

struct rd_kafka_conf_s {
  void (*dr_msg_cb)(rd_kafka_t*, const rd_kafka_message_t*, void*) = nullptr;
  void (*log_cb)(const rd_kafka_t*, int, const char*, const char*) = nullptr;
  void (*error_cb)(rd_kafka_t*, int, const char*, void*) = nullptr;
  void* opaque = nullptr;
  std::string bootstrap_servers;
};

struct rd_kafka_s {
  // callbacks + opaque copied from conf on rd_kafka_new()
  void (*dr_msg_cb)(rd_kafka_t*, const rd_kafka_message_t*, void*) = nullptr;
  void (*log_cb)(const rd_kafka_t*, int, const char*, const char*) = nullptr;
  void (*error_cb)(rd_kafka_t*, int, const char*, void*) = nullptr;
  void* opaque = nullptr;
  std::string broker;
  int log_level = 7;

  // messages produced via rd_kafka_produce() that are pending delivery
  // (acked/nacked) via rd_kafka_poll().
  struct pending_msg {
    rd_kafka_topic_t* rkt;
    void* msg_opaque;
  };
  std::mutex pending_lock;
  std::vector<pending_msg> pending;
};

struct rd_kafka_topic_s {
  rd_kafka_t* producer;
  std::string name;
};

extern "C" {

// -- last error -----------------------------------------------------------

static thread_local rd_kafka_resp_err_t g_last_error = RD_KAFKA_RESP_ERR_NO_ERROR;

rd_kafka_resp_err_t rd_kafka_last_error() {
  return g_last_error;
}

const char* rd_kafka_err2str(rd_kafka_resp_err_t err) {
  if (err == RD_KAFKA_RESP_ERR_NO_ERROR) return "no error (mock)";
  if (err == RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS) return "topic already exists (mock)";
  if (err == RD_KAFKA_RESP_ERR__MSG_TIMED_OUT) return "message timed out (mock)";
  return "mock error";
}

// -- conf -----------------------------------------------------------------

rd_kafka_conf_t* rd_kafka_conf_new() {
  return new rd_kafka_conf_s;
}

void rd_kafka_conf_destroy(rd_kafka_conf_t* conf) {
  delete conf;
}

rd_kafka_conf_res_t rd_kafka_conf_set(rd_kafka_conf_t* conf,
                                      const char* name,
                                      const char* value,
                                      char* errstr,
                                      size_t errstr_size) {
  if (!conf || !name || !value) {
    g_last_error = RD_KAFKA_RESP_ERR__INVALID_ARG;
    return RD_KAFKA_CONF_INVALID;
  }
  if (std::string(name) == "bootstrap.servers") {
    conf->bootstrap_servers = value;
  }
  return RD_KAFKA_CONF_OK;
}

void rd_kafka_conf_set_dr_msg_cb(rd_kafka_conf_t* conf,
                                 void (*dr_msg_cb)(rd_kafka_t*,
                                                  const rd_kafka_message_t*,
                                                  void*)) {
  if (conf) conf->dr_msg_cb = dr_msg_cb;
}

void rd_kafka_conf_set_opaque(rd_kafka_conf_t* conf, void* opaque) {
  if (conf) conf->opaque = opaque;
}

void rd_kafka_conf_set_log_cb(rd_kafka_conf_t* conf,
                              void (*log_cb)(const rd_kafka_t*, int,
                                            const char*, const char*)) {
  if (conf) conf->log_cb = log_cb;
}

void rd_kafka_conf_set_error_cb(rd_kafka_conf_t* conf,
                                void (*error_cb)(rd_kafka_t*, int,
                                                const char*, void*)) {
  if (conf) conf->error_cb = error_cb;
}

// -- producer --------------------------------------------------------------

rd_kafka_t* rd_kafka_new(rd_kafka_type_t type,
                         rd_kafka_conf_t* conf,
                         char* errstr,
                         size_t errstr_size) {
  // take ownership of conf (matches librdkafka semantics)
  std::unique_ptr<rd_kafka_conf_s> conf_holder(conf);
  if (type != RD_KAFKA_PRODUCER) {
    g_last_error = RD_KAFKA_RESP_ERR__INVALID_ARG;
    if (errstr && errstr_size > 0) {
      std:: strncpy(errstr, "mock: only producer supported", errstr_size - 1);
    }
    return nullptr;
  }
  auto* rk = new rd_kafka_s;
  rk->dr_msg_cb = conf->dr_msg_cb;
  rk->log_cb = conf->log_cb;
  rk->error_cb = conf->error_cb;
  rk->opaque = conf->opaque;
  rk->broker = conf->bootstrap_servers;
  return rk;
}

void rd_kafka_destroy(rd_kafka_t* rk) {
  delete rk;
}

const char* rd_kafka_name(const rd_kafka_t* rk) {
  static const char* name = "mock-producer";
  return rk ? name : "";
}

void* rd_kafka_opaque(const rd_kafka_t* rk) {
  return rk ? rk->opaque : nullptr;
}

void rd_kafka_set_log_level(rd_kafka_t* rk, int level) {
  if (rk) rk->log_level = level;
}

rd_kafka_resp_err_t rd_kafka_flush(rd_kafka_t* rk, int timeout_ms) {
  // drain any pending messages so callbacks installed on destroy do not
  // fire spuriously when the test is checking inflight counters.
  if (!rk || !rk->dr_msg_cb) {
    return RD_KAFKA_RESP_ERR_NO_ERROR;
  }
  std::lock_guard<std::mutex> lock(rk->pending_lock);
  for (auto& msg : rk->pending) {
    rd_kafka_message_t m{};
    m.rkt = msg.rkt;
    m.err = REPLY_ACK ? RD_KAFKA_RESP_ERR_NO_ERROR : PRODUCE_FAIL_ERR;
    m._private = msg.msg_opaque;
    rk->dr_msg_cb(rk, &m, rk->opaque);
  }
  rk->pending.clear();
  return RD_KAFKA_RESP_ERR_NO_ERROR;
}

int rd_kafka_poll(rd_kafka_t* rk, int timeout_ms) {
  if (!rk || !rk->dr_msg_cb) {
    return 0;
  }
  rd_kafka_s::pending_msg msg{};
  {
    std::lock_guard<std::mutex> lock(rk->pending_lock);
    if (rk->pending.empty()) {
      return 0;
    }
    msg = rk->pending.front();
    rk->pending.erase(rk->pending.begin());
  }
  rd_kafka_message_t m{};
  m.rkt = msg.rkt;
  m.err = REPLY_ACK ? RD_KAFKA_RESP_ERR_NO_ERROR : PRODUCE_FAIL_ERR;
  m._private = msg.msg_opaque;
  rk->dr_msg_cb(rk, &m, rk->opaque);
  return 1;
}

// -- topics ----------------------------------------------------------------

rd_kafka_topic_t* rd_kafka_topic_new(rd_kafka_t* rk, const char* topic,
                                     rd_kafka_topic_conf_t* conf) {
  if (FAIL_NEXT_TOPIC_NEW) {
    FAIL_NEXT_TOPIC_NEW = false;
    g_last_error = TOPIC_NEW_FAIL_ERR;
    return nullptr;
  }
  if (!rk || !topic) {
    g_last_error = RD_KAFKA_RESP_ERR__INVALID_ARG;
    return nullptr;
  }
  auto* t = new rd_kafka_topic_s;
  t->producer = rk;
  t->name = topic;
  return t;
}

void rd_kafka_topic_destroy(rd_kafka_topic_t* rkt) {
  delete rkt;
}

const char* rd_kafka_topic_name(const rd_kafka_topic_t* rkt) {
  return rkt ? rkt->name.c_str() : "";
}

// -- produce ---------------------------------------------------------------

int rd_kafka_produce(rd_kafka_topic_t* rkt, int32_t partition,
                     int msgflags,
                     void* payload, size_t len,
                     const void* key, size_t keylen,
                     void* msg_opaque) {
  if (!rkt) {
    g_last_error = RD_KAFKA_RESP_ERR__INVALID_ARG;
    return -1;
  }
  if (FAIL_NEXT_PRODUCE) {
    FAIL_NEXT_PRODUCE = false;
    g_last_error = PRODUCE_FAIL_ERR;
    return -1;
  }
  auto* rk = rkt->producer;
  if (!rk) {
    g_last_error = RD_KAFKA_RESP_ERR__INVALID_ARG;
    return -1;
  }
  {
    std::lock_guard<std::mutex> lock(rk->pending_lock);
    rk->pending.push_back({rkt, msg_opaque});
  }
  return 0;
}

}  // extern "C"
