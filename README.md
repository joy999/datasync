# DataSync

DataSync 是一个基于 etcd-io/raft 的分布式数据同步框架，使用 NATS 作为网络传输层，支持多节点组之间的数据同步。

## 特性

- **节点组（NodeGroup）**：逻辑分组，组内节点数据保持一致（克隆状态）
- **跨组同步**：支持配置单向或双向的数据同步
- **共识协议**：基于 etcd-io/raft 实现组内数据一致性
- **网络传输**：使用 NATS 作为消息总线，支持节点发现和消息路由
- **存储驱动**：提供适配器接口，支持自定义数据存储实现
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
    
    raftsync "github.com/joy999/datasync/pkg"
    "github.com/joy999/datasync/pkg/node"
    "github.com/joy999/datasync/drivers/memory"
)

func main() {
    // 创建节点配置
    config := &node.Config{
        NodeID:  raftsync.NodeID("node1"),
        GroupID: raftsync.GroupID("group-a"),
        Address: "localhost:8080",
        DataDir: "./data/node1",
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
        ID:      raftsync.DataID("user-1"),
        Type:    "users",
        Payload: []byte(`{"name":"Alice","email":"alice@example.com"}`),
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

// 触发全量同步
if n.IsLeader() {
    n.TriggerFullSync(ctx, raftsync.GroupID("group-b"), []string{"users"})
}
```

### 3. 实现自定义存储驱动

```go
package mydriver

import (
    "context"
    "time"
    
    raftsync "github.com/joy999/datasync/pkg"
)

type MyDriver struct {
    // 自定义字段
}

func NewDriver() *MyDriver {
    return &MyDriver{}
}

func (d *MyDriver) Initialize(ctx context.Context, config map[string]interface{}) error {
    // 初始化连接等
    return nil
}

func (d *MyDriver) Close() error {
    // 关闭连接
    return nil
}

func (d *MyDriver) GetRecords(ctx context.Context, dataType string, cursor string, limit int) ([]*raftsync.DataRecord, string, error) {
    // 实现分页查询
    return nil, "", nil
}

func (d *MyDriver) GetRecord(ctx context.Context, dataType string, id raftsync.DataID) (*raftsync.DataRecord, error) {
    // 实现单条查询
    return nil, nil
}

func (d *MyDriver) ApplyRecord(ctx context.Context, record *raftsync.DataRecord) error {
    // 应用同步过来的数据
    return nil
}

func (d *MyDriver) GetChanges(ctx context.Context, dataType string, since time.Time, limit int) ([]*raftsync.DataRecord, error) {
    // 获取变更记录（用于增量同步）
    return nil, nil
}

func (d *MyDriver) GetDataTypes(ctx context.Context) ([]string, error) {
    // 返回支持的数据类型
    return []string{"users", "orders"}, nil
}

func (d *MyDriver) GetLatestVersion(ctx context.Context, dataType string, id raftsync.DataID) (int64, error) {
    // 获取最新版本号
    return 0, nil
}
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
│   ├── types.go           # 核心接口和类型定义
│   ├── node/
│   │   └── node.go        # 节点实现
│   ├── transport/
│   │   └── nats.go        # NATS 传输层实现
│   ├── raft/
│   │   └── node.go        # Raft 共识层实现
│   ├── sync/
│   │   └── manager.go     # 同步管理器实现
│   ├── group/
│   │   └── manager.go     # 节点组管理器实现
│   └── auth/
│       └── manager.go     # 认证管理器实现
├── drivers/
│   └── memory/
│       └── driver.go      # 内存存储驱动示例
├── proto/
│   └── raftsync.proto     # Protobuf 协议定义
├── examples/
│   └── simple/
│       └── main.go        # 简单示例
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

## 注意事项

1. **NATS 服务器**：所有节点必须能够连接到同一个 NATS 网络
2. **数据冲突**：框架不负责解决数据冲突，由业务层（驱动实现）处理
3. **数据格式**：框架不关心具体的数据格式，由驱动层负责序列化和反序列化
4. **Leader 同步**：只有 Leader 节点会触发跨组同步

## 许可证

MIT License