#include "store/controller.h"

#include <iostream>
#include <limits>
#include <set>

namespace mooncake {
Controller::Controller(const std::string &etcd_endpoints)
    : client_(etcd_endpoints), running_(true) {
    LOG(INFO) << "Controller initialized with etcd endpoints: "
              << etcd_endpoints;
}

void Controller::Start() {
    LOG(INFO) << "Starting controller";

    // Start watch threads
    // TODO: implement watch threads
    scan_thread_ = std::thread(&Controller::PeriodicScan, this);
}

void Controller::Stop() {
    LOG(INFO) << "Stopping controller";
    running_ = false;

    if (scan_thread_.joinable()) {
        scan_thread_.join();
    }
}

void Controller::PeriodicScan() {
    while (running_) {
        try {
            ScanClusterState();
        } catch (const std::exception &e) {
            LOG(ERROR) << "Exception during cluster scan: " << e.what();
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}

void Controller::ScanClusterState() {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
        // Get all nodes
        auto nodes_response = client_.ls("/nodes/").get();
        if (!nodes_response.is_ok()) {
            LOG(ERROR) << "Failed to get nodes: "
                       << nodes_response.error_message();
            return;
        }

        // Get all buckets
        auto buckets_response = client_.ls("/buckets/").get();
        if (!buckets_response.is_ok()) {
            LOG(ERROR) << "Failed to get buckets: "
                       << buckets_response.error_message();
            return;
        }

        // Check for unassigned buckets
        std::set<int> assigned_buckets;
        for (const auto &value : buckets_response.values()) {
            assigned_buckets.insert(ExtractBucketId(value.key()));
        }

        for (int i = 0; i < 10; ++i) {  // Total buckets = 10
            if (assigned_buckets.find(i) == assigned_buckets.end()) {
                LOG(INFO) << "Found unassigned bucket: " << i;
                AssignNewLeader(i);
            }
        }
    } catch (const std::exception &e) {
        std::cerr << "[ERROR] Exception in ScanClusterState: " << e.what()
                  << std::endl;
    }
}

void Controller::AssignNewLeader(int bucket_id) {
    try {
        // Get all available nodes
        auto response = client_.ls("/nodes/").get();
        if (!response.is_ok() || response.values().empty()) {
            LOG(ERROR) << "Curently no available nodes";
            return;
        }

        // Find node with least load
        NodeInfo best_node;
        int min_load = std::numeric_limits<int>::max();

        for (const auto &value : response.values()) {
            NodeInfo node = parseNodeInfo(value.as_string());
            if (node.leader_num < min_load) {
                min_load = node.leader_num;
                best_node = node;
            }
        }

        // Grant leadership to chosen node
        GrantBucketLeadership(bucket_id, best_node);
    } catch (const std::exception &e) {
        std::cerr << "[ERROR] Exception in AssignNewLeader: " << e.what()
                  << std::endl;
    }
}

void Controller::GrantBucketLeadership(int bucket_id, const NodeInfo &node) {
    std::string target = node.ip + ":" + std::to_string(node.port);

    LOG(INFO) << "Granting bucket " << bucket_id << " leadership to " << target;

    // todo: implement gRPC call to grant leadership
}

int Controller::ExtractBucketId(const std::string &key) {
    size_t pos = key.rfind('/');
    if (pos != std::string::npos) {
        return std::stoi(key.substr(pos + 1));
    }
    return -1;
}
}  // namespace mooncake