package node

import (
	"context"
	"fmt"
	"sync"

	datasync "github.com/joy999/datasync/pkg"
	"github.com/joy999/datasync/pkg/group"
	"github.com/joy999/datasync/pkg/raft"
)

// Config 节点配置
type Config struct {
	NodeID    datasync.NodeID // 节点ID
	GroupID   datasync.GroupID // 组ID
	Address   string           // 节点地址
	DataDir   string           // 数据目录
	RaftConfig *raft.Config    // Raft配置
	Transport  datasync.Transport // 传输层
	AuthManager datasync.AuthManager // 认证管理器
}

// Node 节点实现
type Node struct {
	config        *Config
	raftNode      *raft.Node
	groupManager  *group.Manager
	drivers       map[string]datasync.StorageDriver
	driversMutex  sync.RWMutex
	ctx           context.Context
	cancel        context.CancelFunc
	running       bool
}

// New 创建新节点
func New(config *Config) (*Node, error) {
	// 初始化上下文
	ctx, cancel := context.WithCancel(context.Background())

	// 创建Raft节点
	raftNode, err := raft.NewNode(&raft.Config{
		NodeID:   config.NodeID,
		GroupID:  config.GroupID,
		DataDir:  config.DataDir,
		Transport: config.Transport,
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create raft node: %w", err)
	}

	// 创建组管理器
	groupManager, err := group.NewManager(ctx, config.GroupID)
	if err != nil {
		cancel()
		raftNode.Stop()
		return nil, fmt.Errorf("failed to create group manager: %w", err)
	}

	// 初始化节点
	n := &Node{
		config:       config,
		raftNode:     raftNode,
		groupManager: groupManager,
		drivers:      make(map[string]datasync.StorageDriver),
		ctx:          ctx,
		cancel:       cancel,
		running:      false,
	}

	return n, nil
}

// Start 启动节点
func (n *Node) Start(ctx context.Context) error {
	if n.running {
		return nil
	}

	// 启动Raft节点
	if err := n.raftNode.Start(ctx); err != nil {
		return fmt.Errorf("failed to start raft node: %w", err)
	}

	// 启动组管理器
	if err := n.groupManager.Start(ctx); err != nil {
		n.raftNode.Stop()
		return fmt.Errorf("failed to start group manager: %w", err)
	}

	// 启动传输层
	if n.config.Transport != nil {
		if err := n.config.Transport.Start(ctx); err != nil {
			n.raftNode.Stop()
			n.groupManager.Stop()
			return fmt.Errorf("failed to start transport: %w", err)
		}
	}

	// 启动认证管理器
	if n.config.AuthManager != nil {
		if err := n.config.AuthManager.Start(ctx); err != nil {
			n.raftNode.Stop()
			n.groupManager.Stop()
			if n.config.Transport != nil {
				n.config.Transport.Stop()
			}
			return fmt.Errorf("failed to start auth manager: %w", err)
		}
	}

	n.running = true
	return nil
}

// Stop 停止节点
func (n *Node) Stop() error {
	if !n.running {
		return nil
	}

	// 停止认证管理器
	if n.config.AuthManager != nil {
		n.config.AuthManager.Stop()
	}

	// 停止传输层
	if n.config.Transport != nil {
		n.config.Transport.Stop()
	}

	// 停止组管理器
	n.groupManager.Stop()

	// 停止Raft节点
	n.raftNode.Stop()

	// 取消上下文
	n.cancel()

	n.running = false
	return nil
}

// GetNodeID 获取节点ID
func (n *Node) GetNodeID() datasync.NodeID {
	return n.config.NodeID
}

// GetGroupID 获取组ID
func (n *Node) GetGroupID() datasync.GroupID {
	return n.config.GroupID
}

// IsLeader 检查是否为Leader
func (n *Node) IsLeader() bool {
	return n.raftNode.IsLeader()
}

// RegisterDriver 注册存储驱动
func (n *Node) RegisterDriver(dataType string, driver datasync.StorageDriver) error {
	n.driversMutex.Lock()
	defer n.driversMutex.Unlock()

	// 初始化驱动
	if err := driver.Initialize(n.ctx, nil); err != nil {
		return fmt.Errorf("failed to initialize driver: %w", err)
	}

	// 设置变更回调
	driver.SetChangeCallback(func(record *datasync.DataRecord) {
		n.OnDataChange(record)
	})

	n.drivers[dataType] = driver
	return nil
}

// GetDriver 获取存储驱动
func (n *Node) GetDriver(dataType string) (datasync.StorageDriver, error) {
	n.driversMutex.RLock()
	defer n.driversMutex.RUnlock()

	driver, ok := n.drivers[dataType]
	if !ok {
		return nil, fmt.Errorf("driver not found for data type: %s", dataType)
	}

	return driver, nil
}

// OnDataChange 处理数据变更
func (n *Node) OnDataChange(record *datasync.DataRecord) {
	// 应用到Raft日志
	n.raftNode.Apply(record)
}

// TriggerFullSync 触发全量同步
func (n *Node) TriggerFullSync(ctx context.Context, targetGroup datasync.GroupID, dataTypes []string) error {
	if !n.IsLeader() {
		return fmt.Errorf("only leader can trigger full sync")
	}

	return nil
}

// GetGroupManager 获取组管理器
func (n *Node) GetGroupManager() datasync.GroupManager {
	return n.groupManager
}
