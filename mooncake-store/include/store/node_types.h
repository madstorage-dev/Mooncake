#pragma once
#include <json/json.h>

#include <cstdint>
#include <string>

#include "glog/logging.h"

// Structure to store node information.
struct NodeInfo {
    std::string ip;
    int port;
    uint64_t node_id;
    int capacity;
    int leader_num;
};

// Structure to store bucket information.
struct BucketInfo {
    int bucket_id;
    uint64_t leader_node_id;
};

// Serialize NodeInfo to a JSON string using jsoncpp.
inline std::string serializeNodeInfo(const NodeInfo& info) {
    Json::Value root;
    root["ip"] = info.ip;
    root["port"] = info.port;
    root["uuid"] = info.node_id;
    root["capacity"] = info.capacity;
    root["leader_num"] = info.leader_num;

    Json::StreamWriterBuilder builder;
    return Json::writeString(builder, root);
}

// Deserialize a JSON string to NodeInfo using jsoncpp.
inline NodeInfo parseNodeInfo(const std::string& value) {
    NodeInfo info;
    Json::Value root;
    Json::CharReaderBuilder builder;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    std::string errs;

    if (!reader->parse(value.c_str(), value.c_str() + value.length(), &root,
                       &errs)) {
        LOG(ERROR) << "Error: Failed to parse JSON: " << errs;
        return {};
    }

    info.ip = root["ip"].asString();
    info.port = root["port"].asInt();
    info.node_id = root["uuid"].asUInt64();
    info.capacity = root["capacity"].asInt();
    info.leader_num = root["leader_num"].asInt();

    return info;
}

// Serialize BucketInfo to a JSON string using jsoncpp.
inline std::string serializeBucketInfo(const BucketInfo& info) {
    Json::Value root;
    root["bucket_id"] = info.bucket_id;
    root["uuid"] = info.leader_node_id;

    Json::StreamWriterBuilder builder;
    return Json::writeString(builder, root);
}

// Deserialize a JSON string to BucketInfo using jsoncpp.
inline BucketInfo parseBucketInfo(const std::string& value) {
    BucketInfo info;
    Json::Value root;
    Json::CharReaderBuilder builder;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    std::string errs;

    if (!reader->parse(value.c_str(), value.c_str() + value.length(), &root,
                       &errs)) {
        LOG(ERROR) << "Error: Failed to parse JSON: " << errs;
        return {};
    }

    info.bucket_id = root["bucket_id"].asInt();
    info.leader_node_id = root["uuid"].asUInt64();

    return info;
}