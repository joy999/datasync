package raftsync

import (
	"context"
	"time"
)

// NodeID 节点唯一标识
type NodeID string

// GroupID 节点组唯一标识
type GroupID string

// DataID 数据记录唯一标识
type DataID string

// AuthMethod 认证方法类型
type AuthMethod string

const (
	AuthMethodToken AuthMethod = "token"
	AuthMethodTLS   AuthMethod = "tls"
)

// DataRecord 数据记录
type DataRecord struct {
	ID        DataID            // 记录唯一标识
	Type      string            // 数据类型（如 "users", "orders"）
	Version   int64             // 版本号
	Timestamp time.Time         // 修改时间
	Payload   []byte            // 实际数据内容
	Metadata  map[string]string // 元数据
}

// Node 节点接口
type Node interface {
	// Start 启动节点
	Start(ctx context.Context) error
	// Stop 停止节点
	Stop() error
	// GetNodeID 获取节点ID
	GetNodeID() NodeID
	// GetGroupID 获取节点组ID
	GetGroupID() GroupID
	// IsLeader 检查是否为Leader
	IsLeader() bool
	// RegisterDriver 注册存储驱动
	RegisterDriver(dataType string, driver StorageDriver) error
	// GetDriver 获取存储驱动
	GetDriver(dataType string) (StorageDriver, error)
	// OnDataChange 处理数据变更
	OnDataChange(record *DataRecord)
	// TriggerFullSync 触发全量同步
	TriggerFullSync(ctx context.Context, targetGroup GroupID, dataTypes []string) error
	// GetGroupManager 获取组管理器
	GetGroupManager() GroupManager
}

// DriverID 驱动标识类型（支持变长编码）
type DriverID uint32

// StorageDriver 存储驱动接口
type StorageDriver interface {
	// Initialize 初始化驱动
	Initialize(ctx context.Context, config map[string]interface{}) error
	// Close 关闭驱动
	Close() error
	// GetRecords 分页获取记录
	GetRecords(ctx context.Context, dataType string, cursor string, limit int) ([]*DataRecord, string, error)
	// GetRecord 获取单条记录
	GetRecord(ctx context.Context, dataType string, id DataID) (*DataRecord, error)
	// ApplyRecord 应用同步过来的数据
	ApplyRecord(ctx context.Context, record *DataRecord) error
	// GetChanges 获取变更记录（用于增量同步）
	GetChanges(ctx context.Context, dataType string, since time.Time, limit int) ([]*DataRecord, error)
	// GetDataTypes 获取支持的数据类型
	GetDataTypes(ctx context.Context) ([]string, error)
	// GetLatestVersion 获取最新版本号
	GetLatestVersion(ctx context.Context, dataType string, id DataID) (int64, error)
	// SetChangeCallback 设置变更回调
	SetChangeCallback(callback func(*DataRecord))
	// GetDriverID 获取驱动唯一标识
	// ID 使用变长编码：
	//   - 0-127: 1字节
	//   - 128-16383: 2字节
	//   - 16384-2097151: 3字节
	//   - 更大值: 最多5字节（支持到 34,359,738,367）
	GetDriverID() DriverID
	// Marshal 将 DataRecord 序列化为字节
	Marshal(record *DataRecord) ([]byte, error)
	// Unmarshal 将字节反序列化为 DataRecord
	Unmarshal(data []byte) (*DataRecord, error)
}

// GroupManager 节点组管理器接口
type GroupManager interface {
	// GetGroup 获取组配置
	GetGroup(ctx context.Context, groupID GroupID) (*NodeGroup, error)
	// CreateGroup 创建组
	CreateGroup(ctx context.Context, group *NodeGroup) error
	// UpdateGroup 更新组配置
	UpdateGroup(ctx context.Context, group *NodeGroup) error
	// DeleteGroup 删除组
	DeleteGroup(ctx context.Context, groupID GroupID) error
	// ListGroups 列出所有组
	ListGroups(ctx context.Context) ([]*NodeGroup, error)
}

// NodeGroup 节点组配置
type NodeGroup struct {
	ID            GroupID       // 组ID
	Nodes         []NodeID      // 组内节点
	OutboundSyncs []GroupID     // 出站同步目标组
	InboundSyncs  []GroupID     // 入站同步来源组
	Leader        NodeID        // 当前Leader节点
	LastSyncTime  time.Time     // 上次同步时间
	SyncInterval  time.Duration // 同步间隔
	AuthRequired  bool          // 是否需要认证
	AuthMethod    AuthMethod    // 认证方法
}

// SyncManager 同步管理器接口
type SyncManager interface {
	// Start 启动同步管理器
	Start(ctx context.Context) error
	// Stop 停止同步管理器
	Stop() error
	// TriggerSync 触发同步
	TriggerSync(ctx context.Context, sourceGroup, targetGroup GroupID, dataTypes []string) error
	// GetSyncStatus 获取同步状态
	GetSyncStatus(ctx context.Context, sourceGroup, targetGroup GroupID) (*SyncStatus, error)
}

// SyncStatus 同步状态
type SyncStatus struct {
	SourceGroup    GroupID   // 源组
	TargetGroup    GroupID   // 目标组
	LastSyncTime   time.Time // 上次同步时间
	NextSyncTime   time.Time // 下次同步时间
	SyncMode       string    // 同步模式（全量/增量）
	LastSyncError  string    // 上次同步错误
	PendingRecords int       // 待同步记录数
}

// Transport 传输层接口
type Transport interface {
	// Start 启动传输层
	Start(ctx context.Context) error
	// Stop 停止传输层
	Stop() error
	// SendMessage 发送消息
	SendMessage(ctx context.Context, targetGroup GroupID, message []byte) error
	// Subscribe 订阅消息
	Subscribe(ctx context.Context, groupID GroupID, callback func([]byte)) error
	// DiscoverNodes 发现节点
	DiscoverNodes(ctx context.Context, groupID GroupID) ([]NodeID, error)
}

// AuthManager 认证管理器接口
type AuthManager interface {
	// Start 启动认证管理器
	Start(ctx context.Context) error
	// Stop 停止认证管理器
	Stop() error
	// Authenticate 认证
	Authenticate(ctx context.Context, token string) (bool, error)
	// GenerateToken 生成认证令牌
	GenerateToken(ctx context.Context, nodeID NodeID, groupID GroupID) (string, error)
	// ValidateToken 验证令牌
	ValidateToken(ctx context.Context, token string) (*AuthToken, error)
}

// AuthToken 认证令牌
type AuthToken struct {
	NodeID    NodeID    // 节点ID
	GroupID   GroupID   // 组ID
	ExpiresAt time.Time // 过期时间
	IssuedAt  time.Time // 签发时间
}
