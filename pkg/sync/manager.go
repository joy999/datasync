package sync

import (
	"context"
	"fmt"
	"sync"
	"time"

	datasync "github.com/joy999/datasync/pkg"
	"github.com/joy999/datasync/pkg/protocol"
	pb "github.com/joy999/datasync/proto"
)

// Manager 同步管理器实现
type Manager struct {
	nodeID      string
	groupID     datasync.GroupID
	transport   datasync.Transport
	codec       *protocol.Codec
	syncStatus  map[string]*datasync.SyncStatus
	statusMutex sync.RWMutex
	ctx         context.Context
	cancel      context.CancelFunc
}

// NewManager 创建同步管理器
func NewManager(ctx context.Context, nodeID string, groupID datasync.GroupID, transport datasync.Transport) (*Manager, error) {
	// 初始化上下文
	ctx, cancel := context.WithCancel(ctx)

	// 初始化编解码器配置
	codecConfig := &protocol.Config{
		NodeID:    nodeID,
		GroupID:   string(groupID),
		AuthToken: "", // 初始为空，可以通过 SetAuthToken 设置
	}

	// 初始化同步管理器
	m := &Manager{
		nodeID:     nodeID,
		groupID:    groupID,
		transport:  transport,
		codec:      protocol.NewCodec(codecConfig),
		syncStatus: make(map[string]*datasync.SyncStatus),
		ctx:        ctx,
		cancel:     cancel,
	}

	// 启动订阅
	if err := m.startSubscription(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to start subscription: %w", err)
	}

	return m, nil
}

// SetAuthToken 设置认证令牌
func (m *Manager) SetAuthToken(token string) {
	m.codec.SetAuthToken(token)
}

// Start 启动同步管理器
func (m *Manager) Start(ctx context.Context) error {
	// 同步管理器已经在NewManager中启动
	return nil
}

// Stop 停止同步管理器
func (m *Manager) Stop() error {
	// 取消上下文
	m.cancel()

	return nil
}

// TriggerSync 触发同步
func (m *Manager) TriggerSync(ctx context.Context, targetGroup datasync.GroupID, dataTypes []string) error {
	// 构建同步状态键
	statusKey := fmt.Sprintf("%s->%s", m.groupID, targetGroup)

	// 更新同步状态
	m.statusMutex.Lock()
	m.syncStatus[statusKey] = &datasync.SyncStatus{
		SourceGroup:    m.groupID,
		TargetGroup:    targetGroup,
		LastSyncTime:   time.Now(),
		NextSyncTime:   time.Now().Add(5 * time.Minute),
		SyncMode:       "full",
		LastSyncError:  "",
		PendingRecords: 0,
	}
	m.statusMutex.Unlock()

	// 构建同步请求（使用 Protobuf）
	req := &pb.SyncRequest{
		RequestId: generateRequestID(),
		SyncType:  pb.SyncMessage_FULL,
		DataTypes: dataTypes,
		Cursor:    "",
		Limit:     1000,
	}

	// 编码请求（使用 Protobuf Envelope）
	data, err := m.codec.EncodeSyncRequest(req)
	if err != nil {
		return fmt.Errorf("failed to encode sync request: %w", err)
	}

	// 发送同步请求
	if err := m.transport.SendMessage(ctx, targetGroup, data); err != nil {
		// 更新同步状态
		m.statusMutex.Lock()
		if status, ok := m.syncStatus[statusKey]; ok {
			status.LastSyncError = err.Error()
		}
		m.statusMutex.Unlock()

		return fmt.Errorf("failed to send sync request: %w", err)
	}

	return nil
}

// SendSyncMessage 发送同步消息（用于发送实际数据）
func (m *Manager) SendSyncMessage(ctx context.Context, targetGroup datasync.GroupID, msg *pb.SyncMessage) error {
	// 确保消息包含发送者信息（Envelope 会自动填充）
	data, err := m.codec.EncodeSyncMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to encode sync message: %w", err)
	}

	return m.transport.SendMessage(ctx, targetGroup, data)
}

// GetSyncStatus 获取同步状态
func (m *Manager) GetSyncStatus(ctx context.Context, sourceGroup, targetGroup datasync.GroupID) (*datasync.SyncStatus, error) {
	// 构建同步状态键
	statusKey := fmt.Sprintf("%s->%s", sourceGroup, targetGroup)

	// 获取同步状态
	m.statusMutex.RLock()
	defer m.statusMutex.RUnlock()

	status, ok := m.syncStatus[statusKey]
	if !ok {
		return nil, fmt.Errorf("sync status not found")
	}

	return status, nil
}

// startSubscription 启动订阅
func (m *Manager) startSubscription() error {
	return m.transport.Subscribe(m.ctx, m.groupID, func(data []byte) {
		m.handleMessage(data)
	})
}

// handleMessage 处理消息
func (m *Manager) handleMessage(data []byte) {
	// 解码信封
	env, err := m.codec.DecodeEnvelope(data)
	if err != nil {
		return
	}

	// 可以在这里进行认证验证
	// if !m.codec.ValidateAuth(env, m.authValidator) { return }

	// 根据负载类型分发处理
	switch payload := env.Payload.(type) {
	case *pb.Envelope_SyncRequest:
		m.handleSyncRequest(env, payload.SyncRequest)
	case *pb.Envelope_SyncMessage:
		m.handleSyncMessage(env, payload.SyncMessage)
	case *pb.Envelope_SyncResponse:
		m.handleSyncResponse(env, payload.SyncResponse)
	case *pb.Envelope_Heartbeat:
		m.handleHeartbeat(env, payload.Heartbeat)
	default:
		// 忽略未知类型的消息
	}
}

// handleSyncRequest 处理同步请求
func (m *Manager) handleSyncRequest(env *pb.Envelope, req *pb.SyncRequest) {
	// 可以从信封获取发送者信息
	sourceNode, sourceGroup := protocol.GetNodeInfoFromEnvelope(env)
	_ = sourceNode
	_ = sourceGroup

	// 处理同步请求
	go m.processSyncRequest(req)
}

// handleSyncMessage 处理同步消息（实际数据）
func (m *Manager) handleSyncMessage(env *pb.Envelope, msg *pb.SyncMessage) {
	// 可以从信封获取发送者信息
	sourceNode, sourceGroup := protocol.GetNodeInfoFromEnvelope(env)
	_ = sourceNode
	_ = sourceGroup

	// 处理同步数据
	go m.processSyncMessage(msg)
}

// handleSyncResponse 处理同步响应
func (m *Manager) handleSyncResponse(env *pb.Envelope, resp *pb.SyncResponse) {
	// 处理同步响应
	go m.processSyncResponse(resp)
}

// handleHeartbeat 处理心跳消息
func (m *Manager) handleHeartbeat(env *pb.Envelope, hb *pb.Heartbeat) {
	// 处理心跳
	_ = hb
}

// processSyncRequest 处理同步请求
func (m *Manager) processSyncRequest(req *pb.SyncRequest) {
	// 这里可以实现同步请求处理逻辑
	// 例如，根据请求类型执行全量或增量同步
	_ = req
}

// processSyncMessage 处理同步消息
func (m *Manager) processSyncMessage(msg *pb.SyncMessage) {
	// 处理接收到的数据记录
	_ = msg
}

// processSyncResponse 处理同步响应
func (m *Manager) processSyncResponse(resp *pb.SyncResponse) {
	// 处理同步响应
	_ = resp
}

// generateRequestID 生成请求ID（简单实现）
func generateRequestID() string {
	return fmt.Sprintf("req_%d", time.Now().UnixNano())
}
