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

// Config 表示节点配置。
type Config struct {
	NodeID      datasync.NodeID
	GroupID     datasync.GroupID
	Address     string
	DataDir     string
	RaftConfig  *raft.Config
	Transport   datasync.Transport
	AuthManager datasync.AuthManager
}

// ErrorHandler 定义节点级错误处理函数。
type ErrorHandler func(ctx context.Context, err error)

type transportNodeConfigurator interface {
	SetLocalNodeInfo(nodeID datasync.NodeID, groupID datasync.GroupID, address string)
	SetLeaderState(isLeader bool)
}

// Node 负责协调 Raft、同步、驱动和可选认证能力。
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
	applyStopCh  chan struct{}
	errorHandler ErrorHandler
}

// New 创建节点实例。
func New(config *Config) (*Node, error) {
	ctx, cancel := context.WithCancel(context.Background())

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

	groupManager, err := group.NewManager(ctx, config.GroupID)
	if err != nil {
		cancel()
		if stopErr := raftNode.Stop(); stopErr != nil {
			return nil, fmt.Errorf("failed to create group manager: %w ( additionally failed to stop raft node: %v )", err, stopErr)
		}
		return nil, fmt.Errorf("failed to create group manager: %w", err)
	}

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

	if configurator, ok := config.Transport.(transportNodeConfigurator); ok {
		configurator.SetLocalNodeInfo(config.NodeID, config.GroupID, config.Address)
		configurator.SetLeaderState(false)
	}
	if syncManager != nil {
		syncManager.SetLeaderChecker(func() bool {
			return n.IsLeader()
		})
	}
	if syncManager != nil && config.AuthManager != nil {
		syncManager.SetAuthValidator(func(token string) bool {
			ok, err := config.AuthManager.Authenticate(n.ctx, token)
			return err == nil && ok
		})
	}

	go n.applyRaftData()
	return n, nil
}

// Start 启动节点及其依赖组件。
func (n *Node) Start(ctx context.Context) error {
	if n.running {
		return nil
	}

	if err := n.raftNode.Start(ctx); err != nil {
		return fmt.Errorf("failed to start raft node: %w", err)
	}
	if err := n.groupManager.Start(ctx); err != nil {
		return n.cleanupAfterError("failed to start group manager", err, 0)
	}
	if n.config.Transport != nil {
		if err := n.config.Transport.Start(ctx); err != nil {
			return n.cleanupAfterError("failed to start transport", err, 1)
		}
	}
	if n.syncManager != nil {
		if err := n.syncManager.Start(ctx); err != nil {
			return n.cleanupAfterError("failed to start sync manager", err, 2)
		}
	}
	if n.config.AuthManager != nil {
		if err := n.config.AuthManager.Start(ctx); err != nil {
			return n.cleanupAfterError("failed to start auth manager", err, 3)
		}
	}

	n.running = true
	n.updateTransportLeaderState()
	return nil
}

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

// Stop 停止节点。
func (n *Node) Stop() error {
	if !n.running {
		return nil
	}

	var stopErrs []error
	close(n.applyStopCh)

	if n.config.AuthManager != nil {
		if err := n.config.AuthManager.Stop(); err != nil {
			stopErrs = append(stopErrs, fmt.Errorf("failed to stop auth manager: %w", err))
		}
	}
	if n.syncManager != nil {
		if err := n.syncManager.Stop(); err != nil {
			stopErrs = append(stopErrs, fmt.Errorf("failed to stop sync manager: %w", err))
		}
	}
	if n.config.Transport != nil {
		if err := n.config.Transport.Stop(); err != nil {
			stopErrs = append(stopErrs, fmt.Errorf("failed to stop transport: %w", err))
		}
	}
	if err := n.groupManager.Stop(); err != nil {
		stopErrs = append(stopErrs, fmt.Errorf("failed to stop group manager: %w", err))
	}
	if err := n.raftNode.Stop(); err != nil {
		stopErrs = append(stopErrs, fmt.Errorf("failed to stop raft node: %w", err))
	}

	n.cancel()
	n.running = false
	n.updateTransportLeaderState()

	if len(stopErrs) > 0 {
		return fmt.Errorf("stop errors: %v", stopErrs)
	}
	return nil
}

// GetNodeID 返回节点 ID。
func (n *Node) GetNodeID() datasync.NodeID {
	return n.config.NodeID
}

// GetGroupID 返回组 ID。
func (n *Node) GetGroupID() datasync.GroupID {
	return n.config.GroupID
}

// IsLeader 返回当前节点是否为 Leader。
func (n *Node) IsLeader() bool {
	return n.raftNode.IsLeader()
}

// RegisterDriver 注册并初始化存储驱动。
func (n *Node) RegisterDriver(dataType string, driver datasync.StorageDriver) error {
	n.driversMutex.Lock()
	defer n.driversMutex.Unlock()

	if err := driver.Initialize(n.ctx, nil); err != nil {
		return fmt.Errorf("failed to initialize driver: %w", err)
	}
	driver.SetChangeCallback(func(record *datasync.DataRecord) {
		n.OnDataChange(record)
	})
	if err := codec.Register(driver); err != nil {
		return fmt.Errorf("failed to register driver to codec: %w", err)
	}
	if n.syncManager != nil {
		n.syncManager.RegisterDriver(dataType, driver)
	}

	n.drivers[dataType] = driver
	return nil
}

// GetDriver 返回已注册的驱动。
func (n *Node) GetDriver(dataType string) (datasync.StorageDriver, error) {
	n.driversMutex.RLock()
	defer n.driversMutex.RUnlock()

	driver, ok := n.drivers[dataType]
	if !ok {
		return nil, fmt.Errorf("driver not found for data type: %s", dataType)
	}
	return driver, nil
}

// OnDataChange 将数据变更提议到 Raft。
func (n *Node) OnDataChange(record *datasync.DataRecord) {
	n.driversMutex.RLock()
	driver, ok := n.drivers[record.Type]
	n.driversMutex.RUnlock()
	if !ok {
		if n.errorHandler != nil {
			n.errorHandler(n.ctx, fmt.Errorf("failed to locate driver for record type %s", record.Type))
		}
		return
	}

	if err := n.raftNode.Apply(record, driver.GetDriverID()); err != nil {
		if n.errorHandler != nil {
			n.errorHandler(n.ctx, fmt.Errorf("failed to apply data change for record %s: %w", record.ID, err))
		}
	}
}

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

			n.driversMutex.RLock()
			driver, exists := n.drivers[record.Type]
			n.driversMutex.RUnlock()
			if !exists {
				continue
			}

			if err := driver.ApplyRecord(n.ctx, record); err != nil && n.errorHandler != nil {
				n.errorHandler(n.ctx, fmt.Errorf("failed to apply record %s: %w", record.ID, err))
			}
		}
	}
}

// TriggerFullSync 触发到目标组的全量同步。
func (n *Node) TriggerFullSync(ctx context.Context, targetGroup datasync.GroupID, dataTypes []string) error {
	if !n.IsLeader() {
		return fmt.Errorf("only leader can trigger full sync")
	}
	if n.syncManager == nil {
		return fmt.Errorf("sync manager is not initialized")
	}

	if len(dataTypes) == 0 {
		n.driversMutex.RLock()
		for dataType := range n.drivers {
			dataTypes = append(dataTypes, dataType)
		}
		n.driversMutex.RUnlock()
	}

	if err := n.syncManager.TriggerSync(ctx, targetGroup, dataTypes); err != nil {
		return fmt.Errorf("failed to trigger sync: %w", err)
	}
	return nil
}

// GetGroupManager 返回组管理器。
func (n *Node) GetGroupManager() datasync.GroupManager {
	return n.groupManager
}

// GetSyncManager 返回同步管理器。
func (n *Node) GetSyncManager() *syncmgr.Manager {
	return n.syncManager
}

// SetErrorHandler 设置节点错误处理器。
func (n *Node) SetErrorHandler(handler ErrorHandler) {
	n.errorHandler = handler
}

func (n *Node) updateTransportLeaderState() {
	configurator, ok := n.config.Transport.(transportNodeConfigurator)
	if !ok {
		return
	}
	configurator.SetLeaderState(n.IsLeader())
}
