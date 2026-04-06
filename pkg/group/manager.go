package group

import (
	"context"
	"fmt"
	"sync"
	"time"

	datasync "github.com/joy999/datasync/pkg"
)

// Manager 组管理器实现
type Manager struct {
	groupID     datasync.GroupID
	groups      map[datasync.GroupID]*datasync.NodeGroup
	groupsMutex sync.RWMutex
	ctx         context.Context
	cancel      context.CancelFunc
}

// NewManager 创建组管理器
func NewManager(ctx context.Context, groupID datasync.GroupID) (*Manager, error) {
	// 初始化上下文
	ctx, cancel := context.WithCancel(ctx)

	// 初始化组管理器
	m := &Manager{
		groupID: groupID,
		groups:  make(map[datasync.GroupID]*datasync.NodeGroup),
		ctx:     ctx,
		cancel:  cancel,
	}

	// 初始化默认组
	m.groups[groupID] = &datasync.NodeGroup{
		ID:            groupID,
		Nodes:         []datasync.NodeID{},
		OutboundSyncs: []datasync.GroupID{},
		InboundSyncs:  []datasync.GroupID{},
		Leader:        "",
		LastSyncTime:  time.Now(),
		SyncInterval:  5 * time.Minute,
		AuthRequired:  false,
		AuthMethod:    datasync.AuthMethodToken,
	}

	return m, nil
}

// Start 启动组管理器
func (m *Manager) Start(ctx context.Context) error {
	// 组管理器已经在NewManager中初始化
	return nil
}

// Stop 停止组管理器
func (m *Manager) Stop() error {
	// 取消上下文
	m.cancel()

	return nil
}

// GetGroup 获取组配置
func (m *Manager) GetGroup(ctx context.Context, groupID datasync.GroupID) (*datasync.NodeGroup, error) {
	m.groupsMutex.RLock()
	defer m.groupsMutex.RUnlock()

	group, ok := m.groups[groupID]
	if !ok {
		return nil, fmt.Errorf("group not found: %s", groupID)
	}

	return group, nil
}

// CreateGroup 创建组
func (m *Manager) CreateGroup(ctx context.Context, group *datasync.NodeGroup) error {
	m.groupsMutex.Lock()
	defer m.groupsMutex.Unlock()

	// 检查组是否已存在
	if _, ok := m.groups[group.ID]; ok {
		return fmt.Errorf("group already exists: %s", group.ID)
	}

	// 创建组
	m.groups[group.ID] = group

	return nil
}

// UpdateGroup 更新组配置
func (m *Manager) UpdateGroup(ctx context.Context, group *datasync.NodeGroup) error {
	m.groupsMutex.Lock()
	defer m.groupsMutex.Unlock()

	// 检查组是否存在
	if _, ok := m.groups[group.ID]; !ok {
		return fmt.Errorf("group not found: %s", group.ID)
	}

	// 更新组配置
	m.groups[group.ID] = group

	return nil
}

// DeleteGroup 删除组
func (m *Manager) DeleteGroup(ctx context.Context, groupID datasync.GroupID) error {
	m.groupsMutex.Lock()
	defer m.groupsMutex.Unlock()

	// 检查组是否存在
	if _, ok := m.groups[groupID]; !ok {
		return fmt.Errorf("group not found: %s", groupID)
	}

	// 删除组
	delete(m.groups, groupID)

	return nil
}

// ListGroups 列出所有组
func (m *Manager) ListGroups(ctx context.Context) ([]*datasync.NodeGroup, error) {
	m.groupsMutex.RLock()
	defer m.groupsMutex.RUnlock()

	// 构建组列表
	groups := make([]*datasync.NodeGroup, 0, len(m.groups))
	for _, group := range m.groups {
		groups = append(groups, group)
	}

	return groups, nil
}
