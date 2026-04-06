# DataSync PostgreSQL 驱动

基于 RaftSync 的 PostgreSQL 驱动移植，为 DataSync 提供 PostgreSQL 存储支持和 CDC (变更数据捕获) 机制。

## 特性

- ✅ 完整的 `StorageDriver` 接口实现
- ✅ CDC (变更数据捕获) 机制
- ✅ 断点续传支持
- ✅ 自动表创建和管理
- ✅ 事务支持（数据操作和 CDC 原子性）
- ✅ 变长 DriverID 编码兼容（DriverID = 0x02）
- ✅ JSON 序列化

## 安装

```bash
go get github.com/joy999/datasync/drivers/postgres
```

## 快速开始

```go
package main

import (
    "context"
    "log"
    
    "github.com/joy999/datasync/drivers/postgres"
)

func main() {
    // 配置 PostgreSQL 连接
    config := postgres.PostgreSQLConfig{
        Host:     "localhost",
        Port:     5432,
        Database: "datasync",
        User:     "postgres",
        Password: "password",
        SSLMode:  "disable",
        NodeID:   "node-1",
        GroupID:  "group-1",
    }
    
    // 创建驱动实例
    driver := postgres.NewDriver(config)
    
    // 初始化
    ctx := context.Background()
    if err := driver.Initialize(ctx, nil); err != nil {
        log.Fatal(err)
    }
    defer driver.Close()
    
    // 使用驱动...
}
```

## 配置选项

| 选项 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| Host | string | 是 | - | PostgreSQL 主机地址 |
| Port | int | 是 | - | PostgreSQL 端口 |
| Database | string | 是 | - | 数据库名称 |
| User | string | 是 | - | 用户名 |
| Password | string | 是 | - | 密码 |
| SSLMode | string | 否 | disable | SSL 模式 (disable/require/verify-ca/verify-full) |
| NodeID | string | 是 | - | 当前节点 ID |
| GroupID | string | 是 | - | 当前组 ID |

## 核心功能

### 1. 基础 CRUD

```go
// 创建/更新记录
record := &datasync.DataRecord{
    ID:        datasync.DataID("user-001"),
    Type:      "users",
    Version:   1,
    Timestamp: time.Now(),
    Payload:   []byte(`{"name":"张三"}`),
}
err := driver.ApplyRecord(ctx, record)

// 读取记录
record, err := driver.GetRecord(ctx, "users", datasync.DataID("user-001"))

// 分页查询
records, nextCursor, err := driver.GetRecords(ctx, "users", "", 100)

// 删除记录
err := driver.DeleteRecord(ctx, "users", datasync.DataID("user-001"))
```

### 2. CDC (变更数据捕获)

CDC 会自动记录所有数据变更（INSERT/UPDATE/DELETE）：

```go
// 获取自某个时间点以来的变更
changes, err := driver.GetChanges(ctx, "users", sinceTime, 100)

// 按序列号查询 CDC 记录（用于断点续传）
cdcRecords, err := driver.GetCDCRecordsBySequence(ctx, lastSequence, 100)

// 获取最新 CDC 序列号
latestSeq, err := driver.GetLatestCDCSequence(ctx)
```

### 3. 断点续传

```go
// 获取断点
sequence, err := driver.GetCheckpoint(ctx, "users")

// 设置断点
err := driver.SetCheckpoint(ctx, "users", latestSequence)

// 重置断点
err := driver.ResetCheckpoint(ctx, "users")

// 获取同步状态
status, err := driver.GetSyncStatus(ctx, "users")
// status.LastSequence - 最后同步的序列号
// status.LatestSequence - 最新的 CDC 序列号
// status.PendingCount - 待同步的记录数
```

## 数据库表结构

驱动会自动创建以下表：

### 数据表
动态创建，表名即 `DataRecord.Type`，结构如下：
```sql
CREATE TABLE <data_type> (
    id VARCHAR(255) PRIMARY KEY,
    data JSONB NOT NULL,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
```

### CDC 表 (`_datasync_cdc`)
```sql
CREATE TABLE _datasync_cdc (
    id BIGSERIAL PRIMARY KEY,
    data_type VARCHAR(255) NOT NULL,
    record_id VARCHAR(255) NOT NULL,
    change_type VARCHAR(20) NOT NULL,  -- INSERT/UPDATE/DELETE
    payload JSONB,
    version BIGINT NOT NULL DEFAULT 1,
    source_node VARCHAR(255) NOT NULL,
    group_id VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 断点表 (`_datasync_checkpoint`)
```sql
CREATE TABLE _datasync_checkpoint (
    id SERIAL PRIMARY KEY,
    group_id VARCHAR(255) NOT NULL,
    data_type VARCHAR(255) NOT NULL,
    sequence BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(group_id, data_type)
);
```

## 与 DataSync 集成

```go
import (
    "github.com/joy999/datasync/drivers/postgres"
    datasync "github.com/joy999/datasync/pkg"
    "github.com/joy999/datasync/pkg/node"
)

// 创建节点
n, err := node.NewNode(nodeID, groupID, transport, authManager)

// 创建并初始化 PostgreSQL 驱动
pgDriver := postgres.NewDriver(config)
if err := pgDriver.Initialize(ctx, nil); err != nil {
    log.Fatal(err)
}

// 注册驱动到节点
if err := n.RegisterDriver("users", pgDriver); err != nil {
    log.Fatal(err)
}

// 启动节点
if err := n.Start(ctx); err != nil {
    log.Fatal(err)
}
```

## 测试

运行单元测试：

```bash
# 设置环境变量
export TEST_PG_HOST=localhost
export TEST_PG_PORT=5432
export TEST_PG_DB=datasync_test
export TEST_PG_USER=postgres
export TEST_PG_PASSWORD=postgres

# 运行测试
go test ./drivers/postgres/...
```

## 移植说明

本驱动从 RaftSync 移植，主要改动：

1. **接口适配**：适配 DataSync 的 `StorageDriver` 接口
2. **CDC 表名**：统一使用 `_datasync_cdc` 表，通过 `group_id` 区分不同组
3. **序列化**：使用 JSON 序列化，与 DataSync 内存驱动保持一致
4. **DriverID**：分配 DriverID = 0x02
5. **断点续传**：支持按数据类型分别记录同步进度

## 注意事项

1. **表名安全**：数据表名使用 `DataRecord.Type`，会自动清理特殊字符
2. **时区处理**：统一使用 UTC 时间存储
3. **连接池**：使用 `sql.DB` 内置连接池管理
4. **事务**：`ApplyRecord` 和 `DeleteRecord` 自动使用事务保证数据一致性
