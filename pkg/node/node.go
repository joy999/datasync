package node

import (
	"context"
	"fmt"
	"sync"

	datasync "github.com/joy999/datasync/pkg"
	"github.com/joy999/datasync/pkg/codec"
	"github.com/joy999/datasync/pkg/group"
	"github.com/joy999/datasync/pkg/raft"
	syncmgr "github.com/joy999/datasync/pkg/sync"
)

// Config 节点配置
type Config struct {
	NodeID      datasync.NodeID      // 节点ID
	GroupID     datasync.GroupID     // 组ID
	Address     string               // 节点地址
	DataDir     string               // 数据目录
	RaftConfig  *raft.Config         // Raft配置
	Transport   datasync.Transport   // 传输层
	AuthManager datasync.AuthManager // 认证管理器
}

// ErrorHandler 错误处理函数类型
type ErrorHandler func(ctx context.Context, err error)

// Node 节点实现
type Node struct {
	config       *Config
	raftNode     *raft.Node
	groupManager *group.Manager
	syncManager  *syncmgr.Manager
	drivers      map[string]datasync.StorageDriver
	driversMutex sync.RWMutex
	ctx          context.Context
	cancel       context.CancelFunc
	running      bool

	// 数据应用通道
	applyStopCh chan struct{}

	// 错误处理器
	errorHandler ErrorHandler
}

// New 创建新节点
func New(config *Config) (*Node, error) {
	// 初始化上下文
	ctx, cancel := context.WithCancel(context.Background())

	// 创建Raft节点
	raftNode, err := raft.NewNode(&raft.Config{
		NodeID:    config.NodeID,
		GroupID:   config.GroupID,
		DataDir:   config.DataDir,
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
		if stopErr := raftNode.Stop(); stopErr != nil {
			return nil, fmt.Errorf("failed to create group manager: %w ( additionally failed to stop raft node: %v )", err, stopErr)
		}
		return nil, fmt.Errorf("failed to create group manager: %w", err)
	}

	// 创建同步管理器
	var syncManager *syncmgr.Manager
	if config.Transport != nil {
		syncManager, err = syncmgr.NewManager(ctx, string(config.NodeID), config.GroupID, config.Transport)
		if err != nil {
			cancel()
			var cleanupErrs []error
			if stopErr := raftNode.Stop(); stopErr != nil {
				cleanupErrs = append(cleanupErrs, fmt.Errorf("failed to stop raft node: %w", stopErr))
			}
			if stopErr := groupManager.Stop(); stopErr != nil {
				cleanupErrs = append(cleanupErrs, fmt.Errorf("failed to stop group manager: %w", stopErr))
			}
			if len(cleanupErrs) > 0 {
				return nil, fmt.Errorf("failed to create sync manager: %w ( cleanup errors: %v )", err, cleanupErrs)
			}
			return nil, fmt.Errorf("failed to create sync manager: %w", err)
		}
	}

	// 初始化节点
	n := &Node{
		config:       config,
		raftNode:     raftNode,
		groupManager: groupManager,
		syncManager:  syncManager,
		drivers:      make(map[string]datasync.StorageDriver),
		ctx:          ctx,
		cancel:       cancel,
		running:      false,
		applyStopCh:  make(chan struct{}),
	}

	// 启动Raft数据应用协程
	go n.applyRaftData()

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
		return n.cleanupAfterError("failed to start group manager", err, 0)
	}

	// 启动传输层
	if n.config.Transport != nil {
		if err := n.config.Transport.Start(ctx); err != nil {
			return n.cleanupAfterError("failed to start transport", err, 1)
		}
	}

	// 启动同步管理器
	if n.syncManager != nil {
		if err := n.syncManager.Start(ctx); err != nil {
			return n.cleanupAfterError("failed to start sync manager", err, 2)
		}
	}

	// 启动认证管理器
	if n.config.AuthManager != nil {
		if err := n.config.AuthManager.Start(ctx); err != nil {
			return n.cleanupAfterError("failed to start auth manager", err, 3)
		}
	}

	n.running = true
	return nil
}

// cleanupAfterError 在启动失败后进行清理
// level: 0=raft, 1=raft+group, 2=raft+group+transport, 3=raft+group+transport+sync
func (n *Node) cleanupAfterError(msg string, startErr error, level int) error {
	var cleanupErrs []error

	if level >= 3 && n.syncManager != nil {
		if stopErr := n.syncManager.Stop(); stopErr != nil {
			cleanupErrs = append(cleanupErrs, fmt.Errorf("failed to stop sync manager: %w", stopErr))
		}
	}
	if level >= 2 && n.config.Transport != nil {
		if stopErr := n.config.Transport.Stop(); stopErr != nil {
			cleanupErrs = append(cleanupErrs, fmt.Errorf("failed to stop transport: %w", stopErr))
		}
	}
	if level >= 1 {
		if stopErr := n.groupManager.Stop(); stopErr != nil {
			cleanupErrs = append(cleanupErrs, fmt.Errorf("failed to stop group manager: %w", stopErr))
		}
	}
	if stopErr := n.raftNode.Stop(); stopErr != nil {
		cleanupErrs = append(cleanupErrs, fmt.Errorf("failed to stop raft node: %w", stopErr))
	}

	if len(cleanupErrs) > 0 {
		return fmt.Errorf("%s: %w ( cleanup errors: %v )", msg, startErr, cleanupErrs)
	}
	return fmt.Errorf("%s: %w", msg, startErr)
}

// Stop 停止节点
func (n *Node) Stop() error {
	if !n.running {
		return nil
	}

	var stopErrs []error

	// 停止数据应用协程
	close(n.applyStopCh)

	// 停止认证管理器
	if n.config.AuthManager != nil {
		if err := n.config.AuthManager.Stop(); err != nil {
			stopErrs = append(stopErrs, fmt.Errorf("failed to stop auth manager: %w", err))
		}
	}

	// 停止同步管理器
	if n.syncManager != nil {
		if err := n.syncManager.Stop(); err != nil {
			stopErrs = append(stopErrs, fmt.Errorf("failed to stop sync manager: %w", err))
		}
	}

	// 停止传输层
	if n.config.Transport != nil {
		if err := n.config.Transport.Stop(); err != nil {
			stopErrs = append(stopErrs, fmt.Errorf("failed to stop transport: %w", err))
		}
	}

	// 停止组管理器
	if err := n.groupManager.Stop(); err != nil {
		stopErrs = append(stopErrs, fmt.Errorf("failed to stop group manager: %w", err))
	}

	// 停止Raft节点
	if err := n.raftNode.Stop(); err != nil {
		stopErrs = append(stopErrs, fmt.Errorf("failed to stop raft node: %w", err))
	}

	// 取消上下文
	n.cancel()

	n.running = false

	if len(stopErrs) > 0 {
		return fmt.Errorf("stop errors: %v", stopErrs)
	}
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

	// 注册驱动到 codec registry
	if err := codec.Register(driver); err != nil {
		return fmt.Errorf("failed to register driver to codec: %w", err)
	}

	// 设置 Raft 节点的 driverID（使用第一个注册的驱动的ID）
	n.raftNode.SetDriverID(driver.GetDriverID())

	// 注册驱动到同步管理器
	if n.syncManager != nil {
		n.syncManager.RegisterDriver(dataType, driver)
	}

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
	if err := n.raftNode.Apply(record); err != nil {
		if n.errorHandler != nil {
			n.errorHandler(n.ctx, fmt.Errorf("failed to apply data change for record %s: %w", record.ID, err))
		}
	}
}

// applyRaftData 应用Raft提交的数据到存储驱动
func (n *Node) applyRaftData() {
	applyCh := n.raftNode.GetApplyChannel()

	for {
		select {
		case <-n.applyStopCh:
			return
		case record, ok := <-applyCh:
			if !ok {
				return
			}

			// 获取对应的驱动
			n.driversMutex.RLock()
			driver, exists := n.drivers[record.Type]
			n.driversMutex.RUnlock()

			if !exists {
				// 没有对应的驱动，跳过
				continue
			}

			// 应用记录到驱动（不触发回调，避免循环）
			if err := driver.ApplyRecord(n.ctx, record); err != nil {
				// 应用失败，调用错误处理器
				if n.errorHandler != nil {
					n.errorHandler(n.ctx, fmt.Errorf("failed to apply record %s: %w", record.ID, err))
				}
			}
		}
	}
}

// TriggerFullSync 触发全量同步
func (n *Node) TriggerFullSync(ctx context.Context, targetGroup datasync.GroupID, dataTypes []string) error {
	if !n.IsLeader() {
		return fmt.Errorf("only leader can trigger full sync")
	}

	if n.syncManager == nil {
		return fmt.Errorf("sync manager is not initialized")
	}

	// 如果没有指定数据类型，获取所有已注册的驱动类型
	if len(dataTypes) == 0 {
		n.driversMutex.RLock()
		for dataType := range n.drivers {
			dataTypes = append(dataTypes, dataType)
		}
		n.driversMutex.RUnlock()
	}

	// 触发同步
	if err := n.syncManager.TriggerSync(ctx, targetGroup, dataTypes); err != nil {
		return fmt.Errorf("failed to trigger sync: %w", err)
	}

	return nil
}

// GetGroupManager 获取组管理器
func (n *Node) GetGroupManager() datasync.GroupManager {
	return n.groupManager
}

// GetSyncManager 获取同步管理器
func (n *Node) GetSyncManager() *syncmgr.Manager {
	return n.syncManager
}

// SetErrorHandler 设置错误处理器
func (n *Node) SetErrorHandler(handler ErrorHandler) {
	n.errorHandler = handler
}
