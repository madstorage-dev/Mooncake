#ifndef CONTROLLER_H
#define CONTROLLER_H

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <etcd/Client.hpp>
#include <etcd/Response.hpp>
#include <etcd/Watcher.hpp>
#include <mutex>
#include <string>
#include <thread>

#include "node_types.h"

namespace mooncake {

class Controller {
   public:
    Controller(const std::string &etcd_endpoints);
    void Start();
    void Stop();

   private:
    void PeriodicScan();
    void ScanClusterState();
    void AssignNewLeader(int bucket_id);
    void GrantBucketLeadership(int bucket_id, const NodeInfo &node);
    int ExtractBucketId(const std::string &key);

    etcd::Client client_;
    std::atomic<bool> running_;
    std::thread scan_thread_;
    std::mutex mutex_;
};
}  // namespace mooncake
#endif  // CONTROLLER_H
