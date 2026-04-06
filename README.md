# DataSync

[![CI](https://github.com/joy999/datasync/actions/workflows/ci.yml/badge.svg)](https://github.com/joy999/datasync/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/joy999/datasync)](https://goreportcard.com/report/github.com/joy999/datasync)
[![Go Reference](https://pkg.go.dev/badge/github.com/joy999/datasync.svg)](https://pkg.go.dev/github.com/joy999/datasync)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

DataSync 是一个基于 etcd-io/raft 的分布式数据同步框架，使用 NATS 作为网络传输层，支持多节点组之间的数据同步。

## 特性

- **节点组（NodeGroup）**：逻辑分组，组内节点数据保持一致（克隆状态）
- **跨组同步**：支持配置单向或双向的数据同步
- **共识协议**：基于 etcd-io/raft 实现组内数据一致性
- **网络传输**：使用 NATS 作为消息总线，支持节点发现和消息路由
- **传输协议**：统一的 Protobuf 信封协议，自动携带节点信息和认证令牌
- **存储驱动**：提供适配器接口，支持自定义数据存储实现和编解码方式
- **驱动编解码器**：支持变长 DriverID，不同驱动可使用不同序列化格式
- **认证授权**：支持 Token 和 TLS 证书认证
- **全量/增量同步**：支持首次全量同步和后续增量同步

## 架构

```
┌─────────────────────────────────────────────────────────────┐
│                         Node Group A                         │
│  ┌─────────┐    ┌─────────┐    ┌─────────┐                  │
│  │ Node 1  │◄──►│ Node 2  │◄──►│ Node 3  │  (Raft 共识)      │
│  │ (Leader)│    │(Follower│    │(Follower│                  │
│  └────┬────┘    └────┬────┘    └────┬────┘                  │
│       │              │              │                        │
│       └──────────────┼──────────────┘                        │
│                      │                                       │
│              Outbound Sync (配置)                            │
└──────────────────────┼───────────────────────────────────────┘
                       │
                       │  Envelope Protocol (Protobuf)
                       │  - SourceNode, SourceGroup
                       │  - AuthToken, Timestamp
                       │  - Payload (SyncMessage/RaftMessage/...)
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                         Node Group B                         │
│  ┌─────────┐    ┌─────────┐    ┌─────────┐                  │
│  │ Node 4  │◄──►│ Node 5  │◄──►│ Node 6  │  (Raft 共识)      │
│  │ (Leader)│    │(Follower│    │(Follower│                  │
│  └─────────┘    └─────────┘    └─────────┘                  │
│                                                              │
│              Inbound Sync (配置接收)                         │
└─────────────────────────────────────────────────────────────┘

                    NATS Network (所有节点互连)
```

### 数据流分层

```
┌─────────────────────────────────────────────────────────────┐
│  应用层 (Application)                                        │
│  - 业务数据：DataRecord                                      │
│  - 编解码：StorageDriver.Marshal/Unmarshal (JSON/Protobuf/等) │
│  - 用途：应用业务数据存储                                    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  传输协议层 (DataSync Protocol)                              │
│  - 信封：Envelope (SourceNode, SourceGroup, AuthToken)       │
│  - 负载：SyncMessage/RaftMessage/DiscoveryMessage/...        │
│  - 编解码：Protobuf                                          │
│  - 用途：节点间通信                                          │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  网络传输层 (Network)                                        │
│  - NATS / JetStream                                          │
└─────────────────────────────────────────────────────────────┘
```

## 安装

```bash
go get github.com/joy999/datasync
```

## 快速开始

### 1. 创建节点

```go
package main

import (
    "context"
    "fmt"
    "time"
    
    raftsync "github.com/joy999/datasync/pkg"
    "github.com/joy999/datasync/pkg/node"
    "github.com/joy999/datasync/pkg/transport"
    "github.com/joy999/datasync/drivers/memory"
)

func main() {
    // 创建 NATS 传输层
    natsTransport, err := transport.NewNATS(&transport.NATSConfig{
        Servers: []string{"nats://localhost:4222"},
    })
    if err != nil {
        panic(err)
    }

    // 创建节点配置
    config := &node.Config{
        NodeID:    raftsync.NodeID("node1"),
        GroupID:   raftsync.GroupID("group-a"),
        Address:   "localhost:8080",
        DataDir:   "./data/node1",
        Transport: natsTransport,
    }
    
    // 创建节点
    n, err := node.New(config)
    if err != nil {
        panic(err)
    }
    
    // 启动节点
    ctx := context.Background()
    if err := n.Start(ctx); err != nil {
        panic(err)
    }
    defer n.Stop()
    
    // 注册存储驱动
    driver := memory.NewDriver("users")
    if err := n.RegisterDriver("users", driver); err != nil {
        panic(err)
    }
    
    // 设置数据变更回调
    driver.SetChangeCallback(func(record *raftsync.DataRecord) {
        n.OnDataChange(record)
    })
    
    // 插入数据
    record := &raftsync.DataRecord{
        ID:        raftsync.DataID("user-1"),
        Type:      "users",
        Version:   1,
        Timestamp: time.Now(),
        Payload:   []byte(`{"name":"Alice","email":"alice@example.com"}`),
    }
    
    if err := driver.SaveRecord(record); err != nil {
        panic(err)
    }
    
    fmt.Println("Node started and data saved")
    select {} // 保持运行
}
```

### 2. 配置跨组同步

```go
// 配置 GroupA -> GroupB 的单向同步
groupMgr := n.GetGroupManager()

// 获取源组配置
group, _ := groupMgr.GetGroup(ctx, raftsync.GroupID("group-a"))
group.OutboundSyncs = append(group.OutboundSyncs, raftsync.GroupID("group-b"))

// 获取目标组配置
targetGroup, _ := groupMgr.GetGroup(ctx, raftsync.GroupID("group-b"))
targetGroup.InboundSyncs = append(targetGroup.InboundSyncs, raftsync.GroupID("group-a"))

// 触发全量同步（只有 Leader 可以触发）
if n.IsLeader() {
    syncMgr := n.GetSyncManager()
    syncMgr.TriggerSync(ctx, raftsync.GroupID("group-b"), []string{"users"})
}
```

### 3. 实现自定义存储驱动

```go
package mydriver

import (
    "context"
    "encoding/json"
    "time"
    
    raftsync "github.com/joy999/datasync/pkg"
)

// DriverID 分配一个唯一的驱动ID（使用变长编码）
const MyDriverID raftsync.DriverID = 0x10  // 16

type MyDriver struct {
    // 自定义字段
}

func NewDriver() *MyDriver {
    return &MyDriver{}
}

// StorageDriver 接口实现

func (d *MyDriver) Initialize(ctx context.Context, config map[string]interface{}) error {
    return nil
}

func (d *MyDriver) Close() error {
    return nil
}

func (d *MyDriver) GetRecords(ctx context.Context, dataType string, cursor string, limit int) ([]*raftsync.DataRecord, string, error) {
    return nil, "", nil
}

func (d *MyDriver) GetRecord(ctx context.Context, dataType string, id raftsync.DataID) (*raftsync.DataRecord, error) {
    return nil, nil
}

func (d *MyDriver) ApplyRecord(ctx context.Context, record *raftsync.DataRecord) error {
    return nil
}

func (d *MyDriver) GetChanges(ctx context.Context, dataType string, since time.Time, limit int) ([]*raftsync.DataRecord, error) {
    return nil, nil
}

func (d *MyDriver) GetDataTypes(ctx context.Context) ([]string, error) {
    return []string{"users", "orders"}, nil
}

func (d *MyDriver) GetLatestVersion(ctx context.Context, dataType string, id raftsync.DataID) (int64, error) {
    return 0, nil
}

func (d *MyDriver) SetChangeCallback(callback func(*raftsync.DataRecord)) {
    // 设置变更回调
}

// 编解码器接口实现

func (d *MyDriver) GetDriverID() raftsync.DriverID {
    return MyDriverID
}

func (d *MyDriver) Marshal(record *raftsync.DataRecord) ([]byte, error) {
    // 使用 JSON 序列化（也可以使用 Protobuf、MsgPack 等）
    return json.Marshal(record)
}

func (d *MyDriver) Unmarshal(data []byte) (*raftsync.DataRecord, error) {
    var record raftsync.DataRecord
    if err := json.Unmarshal(data, &record); err != nil {
        return nil, err
    }
    return &record, nil
}
```

## 传输协议

DataSync 使用统一的 Protobuf 信封协议进行节点间通信：

```protobuf
message Envelope {
  uint32 version = 1;           // 协议版本
  string source_node = 2;       // 发送节点ID
  string source_group = 3;      // 发送组ID
  string auth_token = 4;        // 认证令牌
  google.protobuf.Timestamp timestamp = 5;  // 发送时间
  string message_id = 6;        // 消息ID
  oneof payload {               // 实际消息内容
    SyncMessage sync_message = 10;
    SyncRequest sync_request = 11;
    SyncResponse sync_response = 12;
    RaftMessage raft_message = 13;
    DiscoveryMessage discovery_message = 14;
    AuthMessage auth_message = 15;
    ConfigChange config_change = 16;
    SnapshotData snapshot_data = 17;
    Heartbeat heartbeat = 18;
  }
}
```

### 使用传输协议

```go
import (
    "github.com/joy999/datasync/pkg/protocol"
    pb "github.com/joy999/datasync/proto"
)

// 创建编解码器（配置节点身份和认证令牌）
codec := protocol.NewCodec(&protocol.Config{
    NodeID:    "node-1",
    GroupID:   "group-a",
    AuthToken: "secret-token",
})

// 编码消息
msg := &pb.SyncMessage{
    MessageId: "msg-123",
    Type:      pb.SyncMessage_FULL,
}
data, err := codec.EncodeSyncMessage(msg)

// 解码消息（获取信封信息和实际消息）
env, decodedMsg, err := codec.DecodeSyncMessage(data)

// 从信封获取发送者信息
nodeID, groupID := protocol.GetNodeInfoFromEnvelope(env)
authToken := protocol.GetAuthTokenFromEnvelope(env)
```

## 驱动编解码器

DataSync 支持将编解码器放到驱动中，不同数据可以使用不同的编解码方式。

### 变长 DriverID 编码

DriverID 使用变长编码（类似 UTF-8）：

| ID 范围 | 字节数 | 编码格式 |
|---------|--------|----------|
| 0 - 127 | 1 | `0xxxxxxx` |
| 128 - 16,383 | 2 | `10xxxxxx xxxxxxxx` |
| 16,384 - 2,097,151 | 3 | `110xxxxx xxxxxxxx xxxxxxxx` |
| 2,097,152 - 268,435,455 | 4 | `1110xxxx xxxxxxxx xxxxxxxx xxxxxxxx` |

```go
// 编码后的数据格式：
// [变长 DriverID][Payload]
// 例如：DriverID=1 编码为 [0x01]，DriverID=1000 编码为 [0x83, 0xE8]
```

### 驱动注册

```go
import "github.com/joy999/datasync/pkg/codec"

// 驱动自动注册到全局注册表
driver := mydriver.NewDriver()
node.RegisterDriver("users", driver)

// 内部流程：
// 1. 驱动注册到 codec.Registry
// 2. 数据变更时：codec.Encode(record, driverID)
// 3. 数据接收时：codec.Decode(data) → 根据 DriverID 查找驱动解码
```

## 配置

### NATS 配置

```go
natsConfig := &transport.NATSConfig{
    Servers:           []string{"nats://localhost:4222"},
    Username:          "user",
    Password:          "pass",
    Token:             "",
    TLSCert:           "",
    TLSKey:            "",
    TLSCA:             "",
    ReconnectWait:     time.Second,
    MaxReconnects:     60,
    ConnectionTimeout: 10 * time.Second,
    EnableJetStream:   true,
    StreamName:        "RAFTSYNC",
}
```

### Raft 配置

```go
raftConfig := &raft.Config{
    NodeID:           "node1",
    GroupID:          "group-a",
    DataDir:          "./data/node1",
    HeartbeatTick:    1,
    ElectionTick:     10,
    SnapshotInterval: 5 * time.Minute,
    MaxSnapshotFiles: 5,
}
```

### 认证配置

```go
// Token 认证
authConfig := &auth.Config{
    DefaultMethod: raftsync.AuthMethodToken,
    TokenSecret:   "your-secret-key",
    TokenTTL:      24 * time.Hour,
}

// TLS 认证
authConfig := &auth.Config{
    DefaultMethod:  raftsync.AuthMethodTLS,
    CACertFile:     "./certs/ca.crt",
    CAKeyFile:      "./certs/ca.key",
    AutoGenerateCA: true,
}
```

## 项目结构

```
datasync/
├── pkg/
│   ├── types.go              # 核心接口和类型定义（含 DriverID）
│   ├── codec/                # 驱动编解码器注册表
│   │   ├── registry.go       # 驱动注册和变长编码实现
│   │   ├── varint.go         # 变长整数编解码
│   │   └── *_test.go         # 测试文件
│   ├── protocol/             # 传输协议编解码器
│   │   ├── codec.go          # Envelope 编解码实现
│   │   └── *_test.go         # 测试文件
│   ├── node/
│   │   └── node.go           # 节点实现
│   ├── transport/
│   │   └── nats.go           # NATS 传输层实现
│   ├── raft/
│   │   └── node.go           # Raft 共识层实现
│   ├── sync/
│   │   └── manager.go        # 同步管理器实现
│   ├── group/
│   │   └── manager.go        # 节点组管理器实现
│   └── auth/
│       └── manager.go        # 认证管理器实现
├── drivers/
│   └── memory/
│       └── driver.go         # 内存存储驱动示例（JSON 编解码）
├── proto/
│   ├── datasync.proto        # Protobuf 协议定义
│   └── datasync.pb.go        # 生成的 Go 代码
├── examples/
│   └── simple/
│       └── main.go           # 简单示例
├── go.mod
└── README.md
```

## 核心概念

### 节点（Node）

系统中的单个实例，具有唯一的 NodeID。每个节点属于一个节点组。

### 节点组（NodeGroup）

逻辑分组，组内所有节点存储相同的数据（克隆状态）。组内使用 Raft 协议保证数据一致性。

### 同步方向

- **Outbound**: 向其他组推送数据
- **Inbound**: 接收来自其他组的数据
- **Bidirectional**: 双向同步

### 数据记录（DataRecord）

```go
type DataRecord struct {
    ID        DataID            // 记录唯一标识
    Type      string            // 数据类型（如 "users", "orders"）
    Version   int64             // 版本号
    Timestamp time.Time         // 修改时间
    Payload   []byte            // 实际数据内容
    Metadata  map[string]string // 元数据
}
```

### 存储驱动（StorageDriver）

```go
type StorageDriver interface {
    // 数据操作
    GetRecords(ctx context.Context, dataType string, cursor string, limit int) ([]*DataRecord, string, error)
    GetRecord(ctx context.Context, dataType string, id DataID) (*DataRecord, error)
    ApplyRecord(ctx context.Context, record *DataRecord) error
    GetChanges(ctx context.Context, dataType string, since time.Time, limit int) ([]*DataRecord, error)
    
    // 编解码器
    GetDriverID() DriverID                 // 驱动唯一标识（变长编码）
    Marshal(record *DataRecord) ([]byte, error)      // 序列化
    Unmarshal(data []byte) (*DataRecord, error)      // 反序列化
}
```

## 注意事项

1. **NATS 服务器**：所有节点必须能够连接到同一个 NATS 网络
2. **数据冲突**：框架不负责解决数据冲突，由业务层（驱动实现）处理
3. **数据格式**：框架不关心具体的数据格式，由驱动层负责序列化和反序列化
4. **Leader 同步**：只有 Leader 节点会触发跨组同步
5. **认证令牌**：传输协议自动携带认证令牌，接收方应验证令牌有效性
6. **协议版本**：Envelope 包含版本号，便于未来协议升级

## 许可证

MIT License
