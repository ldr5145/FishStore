// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <cstdio>
#include <filesystem>
#include <fstream>

#include "adapters/simdjson_adapter.h"
#include "core/fishstore.h"


using namespace fishstore;

typedef environment::QueueIoHandler handler_t;
typedef device::FileSystemDisk<handler_t, 1073741824L> disk_t;
typedef adapter::SIMDJsonAdapter adapter_t;
using store_t = core::FishStore<disk_t, adapter_t>;

class JsonGeneralScanContext : public IAsyncContext {
public:
  JsonGeneralScanContext(uint16_t psf_id, const char* value)
    : hash_{ core::Utility::HashBytesWithPSFID(psf_id, value, strlen(value)) },
    psf_id_{ psf_id },
    value_size_{ static_cast<uint32_t>(strlen(value)) },
    value_{ value },
    cnt{ 0 } {
  }

  JsonGeneralScanContext(const JsonGeneralScanContext& other)
    : hash_{ other.hash_ },
    psf_id_{ other.psf_id_ },
    value_size_{ other.value_size_ },
    cnt{ other.cnt } {
    set_from_deep_copy();
    char* res = (char*)malloc(value_size_);
    memcpy(res, other.value_, value_size_);
    value_ = res;
  }

  ~JsonGeneralScanContext() {
    if (from_deep_copy()) free((void*)value_);
  }

  inline void Touch(const char* payload, uint32_t payload_size) {
    // printf("Record Hit: %.*s\n", payload_size, payload);
    ++cnt;
  }

  inline void Finalize() {
    printf("%u record has been touched...\n", cnt);
  }

  inline core::KeyHash get_hash() const {
    return hash_;
  }

  inline bool check(const core::KeyPointer* kpt) {
    return kpt->mode == 0 && kpt->general_psf_id == psf_id_ &&
      kpt->value_size == value_size_ &&
      !memcmp(kpt->get_value(), value_, value_size_);
  }

protected:
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

private:
  core::KeyHash hash_;
  uint16_t psf_id_;
  uint32_t value_size_;
  const char* value_;
  uint32_t cnt;
};

class JsonInlineScanContext : public IAsyncContext {
public:
  JsonInlineScanContext(uint32_t psf_id, int32_t value)
    : psf_id_(psf_id), value_(value), cnt(0) {}

  inline void Touch(const char* payload, uint32_t payload_size) {
    // printf("Record Hit: %.*s\n", payload_size, payload);
    ++cnt;
  }

  inline void Finalize() {
    printf("%u record has been touched...\n", cnt);
  }

  inline core::KeyHash get_hash() const {
    return core::KeyHash{ core::Utility::GetHashCode(psf_id_, value_) };
  }

  inline bool check(const core::KeyPointer* kpt) {
    return kpt->mode == 1 && kpt->inline_psf_id == psf_id_ && kpt->value == value_;
  }

protected:
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

private:
  uint32_t psf_id_;
  int32_t value_;
  uint32_t cnt;
};

void SetThreadAffinity(size_t core) {
  // Assume 28 cores.
  constexpr size_t kCoreCount = 36;  // JDH DEBUG
#ifdef _WIN32
  HANDLE thread_handle = ::GetCurrentThread();
  ::SetThreadAffinityMask(thread_handle, (uint64_t)0x1 << core);
#else
  // On MSR-SANDBOX-049, we see CPU 0, Core 0 assigned to 0, 28;
  //                            CPU 1, Core 0 assigned to 1, 29; etc.
  cpu_set_t mask;
  CPU_ZERO(&mask);
#ifdef NUMA
  switch (core % 4) {
  case 0:
    // 0 |-> 0
    // 4 |-> 2
    // 8 |-> 4
    core = core / 2;
    break;
  case 1:
    // 1 |-> 28
    // 5 |-> 30
    // 9 |-> 32
    core = kCoreCount + (core - 1) / 2;
    break;
  case 2:
    // 2  |-> 1
    // 6  |-> 3
    // 10 |-> 5
    core = core / 2;
    break;
  case 3:
    // 3  |-> 29
    // 7  |-> 31
    // 11 |-> 33
    core = kCoreCount + (core - 1) / 2;
    break;
  }
#else
  switch (core % 2) {
  case 0:
    // 0 |-> 0
    // 2 |-> 2
    // 4 |-> 4
    core = core;
    break;
  case 1:
    // 1 |-> 28
    // 3 |-> 30
    // 5 |-> 32
    core = (core - 1) + kCoreCount;
    break;
  }
#endif
  CPU_SET(core, &mask);

  ::sched_setaffinity(0, sizeof(mask), &mask);
#endif
}

int main(int argc, char* argv[]) {
  if (argc != 6) {
    printf("Usage: ./github_query <input_file> <lib_file> <n_threads> <memory_buget> <store_target>\n");
    return -1;
  }

  int n_threads = atoi(argv[3]);
  std::ifstream fin(argv[1]);
  std::vector<std::string> batches;
  const uint32_t json_batch_size = 1;
  uint32_t json_batch_cnt = 0;
  size_t line_cnt = 0;
  size_t record_cnt = 0;
  std::string line;
  while (std::getline(fin, line)) {
    if (json_batch_cnt == 0 || line_cnt == json_batch_size) {
      line_cnt = 1;
      ++json_batch_cnt;
      batches.push_back(line);
    }
    else {
      batches.back() += line;
      line_cnt++;
    }
    ++record_cnt;
  }

  uint32_t batch_size = json_batch_cnt / n_threads + 1;

  printf("Finish loading %u batches (%zu records) of json into the memory....\n",
    json_batch_cnt, record_cnt);

  std::filesystem::create_directory(argv[5]);
  size_t store_size = 1LL << atoi(argv[4]);
  store_t store{ (1L << 24), store_size, argv[5] };

  SetThreadAffinity(n_threads);
  store.StartSession();

  auto lib_id = store.LoadPSFLibrary(argv[2]);
  auto id_proj = store.MakeProjection("/id");
  auto actor_id_proj = store.MakeInlinePSF({ "/actor/id" }, lib_id, "int_projection");
  auto repo_id_proj = store.MakeInlinePSF({ "/repo/id" }, lib_id, "int_projection");
  auto type_proj = store.MakeProjection("/type");
  auto predicate1_id =
    store.MakeInlinePSF({ "/type", "/payload/action" }, lib_id, "opened_issue");
  auto predicate2_id =
    store.MakeInlinePSF({ "/type", "/payload/pull_request/head.repo/language" }, lib_id, "cpp_pr");

  std::vector<ParserAction> parser_actions;
  parser_actions.push_back({ REGISTER_GENERAL_PSF, id_proj });
  parser_actions.push_back({ REGISTER_GENERAL_PSF, actor_id_proj });
  parser_actions.push_back({ REGISTER_GENERAL_PSF, repo_id_proj });
  parser_actions.push_back({ REGISTER_GENERAL_PSF, type_proj });
  parser_actions.push_back({ REGISTER_INLINE_PSF, predicate1_id });
  parser_actions.push_back({ REGISTER_INLINE_PSF, predicate2_id });

  uint64_t safe_register_address, safe_unregister_address;
  safe_unregister_address = store.ApplyParserShift(
    parser_actions, [&safe_register_address](uint64_t safe_address) {
      safe_register_address = safe_address;
    });

  store.CompleteAction(true);

  std::this_thread::sleep_for(std::chrono::seconds(1));

  std::vector<std::thread> thds;
  std::atomic_uint64_t bytes_ingested{ 0 };
  std::atomic_uint32_t record_ingested{ 0 };
  auto worker_thd = [&](int thread_no) {
    SetThreadAffinity(thread_no);
    store.StartSession();
    size_t begin_line = thread_no;
    size_t batch_end = batches.size();
    auto callback = [](IAsyncContext * ctxt, Status result) {
      assert(false);
    };
    store.Refresh();
    size_t op_cnt = 0;
    for (size_t i = begin_line; i < batch_end; i += n_threads) {
      auto res = store.BatchInsert(batches[i], 1);
      bytes_ingested.fetch_add(batches[i].size());
      record_ingested.fetch_add(res);
      op_cnt += res;
      if (op_cnt % 256 == 0) {
        store.Refresh();
        op_cnt = 0;
      }
    }
    store.CompletePending();
    store.StopSession();
  };

  bool finish = false;
  auto callback = [](IAsyncContext * ctxt, Status result) {
    assert(result == Status::Ok);
  };
  auto begin = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < n_threads; ++i) {
    thds.emplace_back(std::thread(worker_thd, i));
  }

  std::thread timer([&n_threads, &finish, &bytes_ingested, &record_ingested]() {
    SetThreadAffinity(n_threads + 1);
    uint64_t last_bytes = 0;
    uint32_t last_records = 0;
    while (!finish) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
      uint64_t current_bytes = bytes_ingested.load();
      uint32_t current_records = record_ingested.load();
      printf("Throughput: %.6lf MB/s, %u records/s\n",
        (double)(current_bytes - last_bytes) / 1024.0 / 1024.0,
        (current_records - last_records));
      last_bytes = current_bytes;
      last_records = current_records;
    }
    });

  for (int i = 0; i < n_threads; ++i) {
    thds[i].join();
  }

  auto end = std::chrono::high_resolution_clock::now();
  finish = true;

  timer.join();
  printf("Finish inserting %u json batches in %.6f seconds....\n",
    json_batch_cnt, std::chrono::duration<double>(end - begin).count());
  printf("Log Size: %.6fMB\n",
    store.hlog.GetTailAddress().control() / 1024.0 / 1024.0);

  /// Query Examples
  core::Constants::SetAvgRecordSize(2310);

  JsonGeneralScanContext scan_context_push{ type_proj, "PushEvent" };
  begin = std::chrono::high_resolution_clock::now();
  auto status = store.Scan(scan_context_push, callback, 1);
  store.CompletePending(true);
  end = std::chrono::high_resolution_clock::now();
  printf("Scan on `type == PushEvent` is done in %.6f seconds...\n",
    std::chrono::duration<double>(end - begin).count());

  JsonInlineScanContext pred_scan_context3{ predicate2_id, 1 };
  begin = std::chrono::high_resolution_clock::now();
  status = store.Scan(pred_scan_context3, callback, 1);
  store.CompletePending(true);
  end = std::chrono::high_resolution_clock::now();
  printf("Scan over pre-registred predicate `type == PullRequestEvent && payload.head.pull_request.repo.language == C++` is done in %.6f seconds...\n",
    std::chrono::duration<double>(end - begin).count());


  parser_actions.clear();
  parser_actions.push_back({ DEREGISTER_GENERAL_PSF, id_proj });
  parser_actions.push_back({ DEREGISTER_GENERAL_PSF, actor_id_proj });
  parser_actions.push_back({ DEREGISTER_GENERAL_PSF, repo_id_proj });
  parser_actions.push_back({ DEREGISTER_GENERAL_PSF, type_proj });
  parser_actions.push_back({ DEREGISTER_INLINE_PSF, predicate1_id });
  parser_actions.push_back({ DEREGISTER_INLINE_PSF, predicate2_id });
  safe_unregister_address = store.ApplyParserShift(
    parser_actions, [&safe_register_address](uint64_t safe_address) {
      safe_register_address = safe_address;
    });

  store.CompleteAction(true);

  store.StopSession();

  return 0;
}