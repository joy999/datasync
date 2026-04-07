# 两节点数据同步示例

这个示例展示了两个节点（NodeA 和 NodeB）在同一个 Group 中进行实时数据同步的场景。

## 场景说明

```
┌─────────┐                    ┌─────────┐
│  NodeA  │ ◄─────同步─────► │  NodeB  │
│ (主节点) │                    │ (从节点) │
└─────────┘                    └─────────┘
     │                               │
     └────────── 同一 Group ──────────┘
```

- **NodeA**: 主动创建/更新数据的节点
- **NodeB**: 接收同步数据的节点
- **Group**: 两个节点属于同一个数据同步组

## 快速开始

### 1. 启动 NodeA（主节点）

```bash
cd examples/two_nodes_sync
go run main.go -node=node-a -port=8081
```

### 2. 启动 NodeB（从节点）

在另一个终端窗口：

```bash
cd examples/two_nodes_sync
go run main.go -node=node-b -port=8082 -peers="localhost:8081"
```

### 3. 测试数据同步

在 NodeA 的终端中输入：

```
> create Hello World
✅ 创建记录 record-1: Hello World
```

观察 NodeB 的终端，你会看到：

```
📥 [15:04:05] 收到同步数据:
   来源节点: node-a
   数据类型: message
   记录ID: record-1
   版本: 1
   内容: Hello World
```

## 支持的命令

在 NodeA 中可以使用的命令：

| 命令 | 说明 | 示例 |
|------|------|------|
| `create <内容>` | 创建新记录 | `create Hello World` |
| `update <id> <内容>` | 更新现有记录 | `update record-1 Hi there` |
| `list` | 列出所有记录 | `list` |
| `exit` | 退出程序 | `exit` |

## 参数说明

```bash
go run main.go [参数]

参数:
  -node string    节点ID (必需): node-a 或 node-b
  -port string    监听端口 (默认: "8080")
  -peers string   其他节点地址，逗号分隔 (如: localhost:8081)
  -group string   Group ID (默认: "demo-group")
```

## 扩展场景

### 多节点同步

可以启动更多节点加入同一个 Group：

```bash
# NodeC
go run main.go -node=node-c -port=8083 -peers="localhost:8081,localhost:8082"
```

### 使用内存存储

默认使用内存存储，数据不会持久化。重启节点后数据会丢失。

### 使用 PostgreSQL 存储

可以修改代码使用 PostgreSQL 驱动来实现数据持久化：

```go
import "github.com/joy999/datasync/drivers/postgres"

// 使用 PostgreSQL 存储
driver := postgres.NewDriver(postgres.PostgreSQLConfig{
    Host:     "localhost",
    Port:     5432,
    Database: "datasync",
    User:     "postgres",
    Password: "password",
    NodeID:   "node-a",
    GroupID:  "demo-group",
})
```

## 工作原理

1. **Group 创建**: 两个节点加入同一个 Group，建立 P2P 连接
2. **数据变更**: NodeA 调用 `ApplyRecord()` 创建/更新数据
3. **自动同步**: Group 自动将数据变更广播到所有节点
4. **数据接收**: NodeB 通过回调函数收到同步的数据

## 注意事项

- 确保每个节点使用不同的端口
- `-peers` 参数只需要指定已存在的节点地址
- 节点启动顺序不重要，后启动的节点会自动连接
- 按 `Ctrl+C` 可以安全退出节点
