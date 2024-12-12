# Mooncake Store

## 概述

Mooncake Store 是一款专为大语言模型 (LLM) 推理场景设计的高性能**分布式键值 (KV) 缓存存储引擎**。它旨在满足 LLM 推理过程中对 KV 缓存的严苛性能要求。与 Redis 或 Memcached 等传统缓存系统不同，Mooncake Store 的核心定位是**存储引擎而非完整的缓存系统**。它提供了底层对象存储和管理功能，而具体的缓存策略（如淘汰策略）则交由上层框架或用户实现，从而提供更高的灵活性和可定制性。

Mooncake Store 的主要特性包括：

*   **对象级存储操作**：提供简单易用的对象级 API，包括 Put、Get、Replicate 和 Remove 操作，方便用户进行数据管理。
*   **多副本支持**：支持为同一对象保存多个数据副本，有效缓解热点访问压力。
*   **最终一致性**：保证 Get 操作读取到完整且正确的数据，但不保证读取到最新写入的数据。这种最终一致性模型在确保高性能的同时，简化了系统设计。
*   **高带宽利用**：支持对大型对象进行条带化和并行 I/O 传输，充分利用多网卡聚合带宽，实现高速数据读写。
*   **灵活的落盘策略**：支持 Eager、Lazy 和 None 三种将数据持久化到慢速存储的策略，满足不同场景下的数据持久化需求。
*   **动态资源伸缩**：支持动态添加和删除节点，灵活应对系统负载变化，实现资源的弹性管理。

## C/C++ API

### Get 接口

```c++
StatusCode Get(const String& object_key, std::vector<Slice>* slices);
```

用于获取 `object_key` 对应的值。该接口保证读取到的数据是完整且正确的，但不保证是最新版本的数据。

读取到的值将通过 TransferEngine 存储到 `slices` 所指向的内存区域中。 
**注意**：这里将`std::vector<Slice> slices`改为了`std::vector<Slice>* slices`，因为`Get`函数需要修改`slices`的内容，所以需要传入指针或引用。

### Put 接口

```c++
struct ReplicateConfig {
    uint64_t replica_num; // 指定对象的副本数量
};

StatusCode Put(const ObjectKey& key, const std::vector<Slice>& slices, const ReplicateConfig& config);
```

用于存储 `key` 对应的值。可通过 `config` 参数设置所需的副本数量，系统中的 Leader 节点将**尽最大努力**完成数据复制。