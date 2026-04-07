package sync

import (
	"context"
	"fmt"
	"sync"
	"time"

	datasync "github.com/joy999/datasync/pkg"
	"github.com/joy999/datasync/pkg/protocol"
	pb "github.com/joy999/datasync/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ErrorHandler 错误处理函数类型
type ErrorHandler func(ctx context.Context, err error)

// Manager 同步管理器实现
type Manager struct {
	nodeID       string
	groupID      datasync.GroupID
	transport    datasync.Transport
	codec        *protocol.Codec
	drivers      map[string]datasync.StorageDriver // 数据类型 -> 驱动映射
	driversMutex sync.RWMutex
	syncStatus   map[string]*datasync.SyncStatus
	statusMutex  sync.RWMutex
	ctx          context.Context
	cancel       context.CancelFunc

	// 心跳相关
	heartbeatInterval time.Duration
	processedMessages uint64
	msgMutex          sync.RWMutex

	// 错误处理器
	errorHandler ErrorHandler
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
		nodeID:            nodeID,
		groupID:           groupID,
		transport:         transport,
		codec:             protocol.NewCodec(codecConfig),
		drivers:           make(map[string]datasync.StorageDriver),
		syncStatus:        make(map[string]*datasync.SyncStatus),
		ctx:               ctx,
		cancel:            cancel,
		heartbeatInterval: 30 * time.Second,
		processedMessages: 0,
	}

	// 启动订阅
	if err := m.startSubscription(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to start subscription: %w", err)
	}

	// 启动心跳协程
	go m.heartbeatLoop()

	return m, nil
}

// RegisterDriver 注册存储驱动
func (m *Manager) RegisterDriver(dataType string, driver datasync.StorageDriver) {
	m.driversMutex.Lock()
	defer m.driversMutex.Unlock()
	m.drivers[dataType] = driver
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
	// 从信封获取发送者信息
	sourceNode, sourceGroup := protocol.GetNodeInfoFromEnvelope(env)

	// 异步处理同步请求
	go m.processSyncRequest(req, datasync.GroupID(sourceGroup), sourceNode)
}

// handleSyncMessage 处理同步消息（实际数据）
func (m *Manager) handleSyncMessage(env *pb.Envelope, msg *pb.SyncMessage) {
	// 从信封获取发送者信息
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
	// 更新同步状态中的节点信息
	m.statusMutex.Lock()
	defer m.statusMutex.Unlock()

	// 可以在这里维护活跃节点列表
	_ = hb
}

// processSyncRequest 处理同步请求
func (m *Manager) processSyncRequest(req *pb.SyncRequest, sourceGroup datasync.GroupID, sourceNode string) {
	// 增加处理消息计数
	m.msgMutex.Lock()
	m.processedMessages++
	m.msgMutex.Unlock()

	// 获取请求的数据类型
	dataTypes := req.DataTypes
	if len(dataTypes) == 0 {
		// 如果没有指定数据类型，获取所有支持的数据类型
		m.driversMutex.RLock()
		for dataType := range m.drivers {
			dataTypes = append(dataTypes, dataType)
		}
		m.driversMutex.RUnlock()
	}

	// 根据同步类型处理
	switch req.SyncType {
	case pb.SyncMessage_FULL:
		// 全量同步
		m.processFullSyncRequest(req, sourceGroup, dataTypes)
	case pb.SyncMessage_INCREMENTAL:
		// 增量同步
		m.processIncrementalSyncRequest(req, sourceGroup, dataTypes)
	default:
		// 未知的同步类型，发送错误响应
		m.sendSyncErrorResponse(req.RequestId, sourceGroup, "unknown sync type")
	}
}

// processFullSyncRequest 处理全量同步请求
func (m *Manager) processFullSyncRequest(req *pb.SyncRequest, targetGroup datasync.GroupID, dataTypes []string) {
	for _, dataType := range dataTypes {
		// 获取驱动
		m.driversMutex.RLock()
		driver, ok := m.drivers[dataType]
		m.driversMutex.RUnlock()

		if !ok {
			continue // 跳过不支持的驱动
		}

		// 分页获取记录
		cursor := req.Cursor
		hasMore := true

		for hasMore {
			records, nextCursor, err := driver.GetRecords(m.ctx, dataType, cursor, int(req.Limit))
			if err != nil {
				m.sendSyncErrorResponse(req.RequestId, targetGroup, fmt.Sprintf("failed to get records: %v", err))
				return
			}

			// 转换为 protobuf 记录
			pbRecords := make([]*pb.DataRecord, 0, len(records))
			for _, record := range records {
				pbRecords = append(pbRecords, convertToPBDataRecord(record))
			}

			hasMore = nextCursor != ""
			cursor = nextCursor

			// 构建同步消息
			msg := &pb.SyncMessage{
				MessageId: generateRequestID(),
				Type:      pb.SyncMessage_FULL,
				Records:   pbRecords,
				Cursor:    cursor,
				HasMore:   hasMore,
			}

			// 发送同步消息
			if err := m.SendSyncMessage(m.ctx, targetGroup, msg); err != nil {
				m.sendSyncErrorResponse(req.RequestId, targetGroup, fmt.Sprintf("failed to send sync message: %v", err))
				return
			}

			// 如果还有更多数据，继续获取
			if !hasMore {
				break
			}
		}
	}

	// 发送完成响应
	resp := &pb.SyncResponse{
		RequestId:    req.RequestId,
		Success:      true,
		ErrorMessage: "",
	}
	if err := m.sendSyncResponse(targetGroup, resp); err != nil {
		// 发送成功响应失败，发送错误响应
		m.sendSyncErrorResponse(req.RequestId, targetGroup, fmt.Sprintf("failed to send success response: %v", err))
	}
}

// processIncrementalSyncRequest 处理增量同步请求
func (m *Manager) processIncrementalSyncRequest(req *pb.SyncRequest, targetGroup datasync.GroupID, dataTypes []string) {
	// 获取增量同步的起始时间
	var since time.Time
	if req.Since != nil {
		since = req.Since.AsTime()
	}

	for _, dataType := range dataTypes {
		// 获取驱动
		m.driversMutex.RLock()
		driver, ok := m.drivers[dataType]
		m.driversMutex.RUnlock()

		if !ok {
			continue // 跳过不支持的驱动
		}

		// 获取变更记录
		changes, err := driver.GetChanges(m.ctx, dataType, since, int(req.Limit))
		if err != nil {
			m.sendSyncErrorResponse(req.RequestId, targetGroup, fmt.Sprintf("failed to get changes: %v", err))
			return
		}

		// 转换为 protobuf 记录
		pbRecords := make([]*pb.DataRecord, 0, len(changes))
		for _, record := range changes {
			pbRecords = append(pbRecords, convertToPBDataRecord(record))
		}

		// 构建同步消息
		msg := &pb.SyncMessage{
			MessageId: generateRequestID(),
			Type:      pb.SyncMessage_INCREMENTAL,
			Records:   pbRecords,
			HasMore:   false, // 增量同步一次性返回
		}

		// 发送同步消息
		if err := m.SendSyncMessage(m.ctx, targetGroup, msg); err != nil {
			m.sendSyncErrorResponse(req.RequestId, targetGroup, fmt.Sprintf("failed to send sync message: %v", err))
			return
		}
	}

	// 发送完成响应
	resp := &pb.SyncResponse{
		RequestId:    req.RequestId,
		Success:      true,
		ErrorMessage: "",
	}
	if err := m.sendSyncResponse(targetGroup, resp); err != nil {
		// 发送成功响应失败，尝试发送错误响应
		m.sendSyncErrorResponse(req.RequestId, targetGroup, fmt.Sprintf("failed to send success response: %v", err))
	}
}

// processSyncMessage 处理同步消息
func (m *Manager) processSyncMessage(msg *pb.SyncMessage) {
	// 增加处理消息计数
	m.msgMutex.Lock()
	m.processedMessages++
	m.msgMutex.Unlock()

	// 处理接收到的数据记录
	for _, pbRecord := range msg.Records {
		// 转换为内部 DataRecord
		record := convertFromPBDataRecord(pbRecord)

		// 获取对应的驱动
		m.driversMutex.RLock()
		driver, ok := m.drivers[record.Type]
		m.driversMutex.RUnlock()

		if !ok {
			// 没有对应的驱动，跳过
			continue
		}

		// 应用记录到存储
		if err := driver.ApplyRecord(m.ctx, record); err != nil {
			// 应用失败，记录日志但继续处理其他记录
			// 实际生产环境应该有更完善的错误处理
			continue
		}
	}
}

// processSyncResponse 处理同步响应
func (m *Manager) processSyncResponse(resp *pb.SyncResponse) {
	// 增加处理消息计数
	m.msgMutex.Lock()
	m.processedMessages++
	m.msgMutex.Unlock()

	// 更新同步状态
	m.statusMutex.Lock()
	defer m.statusMutex.Unlock()

	// 查找对应的同步状态并更新
	for _, status := range m.syncStatus {
		// 这里可以通过 RequestId 关联到具体的同步请求
		_ = status
		_ = resp
		break
	}
}

// sendSyncResponse 发送同步响应
func (m *Manager) sendSyncResponse(targetGroup datasync.GroupID, resp *pb.SyncResponse) error {
	data, err := m.codec.EncodeSyncResponse(resp)
	if err != nil {
		return fmt.Errorf("failed to encode sync response: %w", err)
	}

	if err := m.transport.SendMessage(m.ctx, targetGroup, data); err != nil {
		return fmt.Errorf("failed to send sync response: %w", err)
	}
	return nil
}

// SetErrorHandler 设置错误处理器
func (m *Manager) SetErrorHandler(handler ErrorHandler) {
	m.errorHandler = handler
}

// sendSyncErrorResponse 发送同步错误响应
func (m *Manager) sendSyncErrorResponse(requestID string, targetGroup datasync.GroupID, errorMsg string) {
	resp := &pb.SyncResponse{
		RequestId:    requestID,
		Success:      false,
		ErrorMessage: errorMsg,
	}
	// 尝试发送错误响应，如果失败则调用错误处理器
	if err := m.sendSyncResponse(targetGroup, resp); err != nil && m.errorHandler != nil {
		m.errorHandler(m.ctx, fmt.Errorf("failed to send error response: %w", err))
	}
}

// heartbeatLoop 心跳循环
func (m *Manager) heartbeatLoop() {
	ticker := time.NewTicker(m.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.sendHeartbeat()
		}
	}
}

// sendHeartbeat 发送心跳
func (m *Manager) sendHeartbeat() {
	m.msgMutex.RLock()
	processedCount := m.processedMessages
	m.msgMutex.RUnlock()

	hb := &pb.Heartbeat{
		NodeId:            m.nodeID,
		GroupId:           string(m.groupID),
		IsLeader:          false, // 需要通过其他方式获取
		ProcessedMessages: processedCount,
	}

	data, err := m.codec.EncodeHeartbeat(hb)
	if err != nil {
		return
	}

	// 广播心跳到所有已知的组
	// 实际实现中应该维护一个已知组的列表
	// TODO: 实现心跳广播
	_ = data // 暂时忽略，等待完整实现
}

// generateRequestID 生成请求ID（简单实现）
func generateRequestID() string {
	return fmt.Sprintf("req_%d", time.Now().UnixNano())
}

// convertToPBDataRecord 将内部 DataRecord 转换为 protobuf DataRecord
func convertToPBDataRecord(record *datasync.DataRecord) *pb.DataRecord {
	return &pb.DataRecord{
		Id:        string(record.ID),
		Type:      record.Type,
		Version:   record.Version,
		Timestamp: timestamppb.New(record.Timestamp),
		Payload:   record.Payload,
		Metadata:  record.Metadata,
	}
}

// convertFromPBDataRecord 将 protobuf DataRecord 转换为内部 DataRecord
func convertFromPBDataRecord(pbRecord *pb.DataRecord) *datasync.DataRecord {
	record := &datasync.DataRecord{
		ID:       datasync.DataID(pbRecord.Id),
		Type:     pbRecord.Type,
		Version:  pbRecord.Version,
		Payload:  pbRecord.Payload,
		Metadata: pbRecord.Metadata,
	}

	if pbRecord.Timestamp != nil {
		record.Timestamp = pbRecord.Timestamp.AsTime()
	}

	return record
}
