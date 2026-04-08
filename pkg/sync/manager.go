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

// ErrorHandler 定义同步管理器错误处理函数。
type ErrorHandler func(ctx context.Context, err error)

// Manager 负责协调跨组同步。
type Manager struct {
	nodeID       string
	groupID      datasync.GroupID
	transport    datasync.Transport
	codec        *protocol.Codec
	drivers      map[string]datasync.StorageDriver
	driversMutex sync.RWMutex
	syncStatus   map[string]*datasync.SyncStatus
	statusMutex  sync.RWMutex
	ctx          context.Context
	cancel       context.CancelFunc

	heartbeatInterval time.Duration
	processedMessages uint64
	msgMutex          sync.RWMutex
	requestMap        map[string]string
	requestMutex      sync.RWMutex
	knownGroups       map[datasync.GroupID]struct{}
	groupsMutex       sync.RWMutex

	errorHandler  ErrorHandler
	authValidator func(string) bool
	leaderChecker func() bool
}

// NewManager 创建同步管理器并订阅所属组主题。
func NewManager(ctx context.Context, nodeID string, groupID datasync.GroupID, transport datasync.Transport) (*Manager, error) {
	ctx, cancel := context.WithCancel(ctx)

	m := &Manager{
		nodeID:            nodeID,
		groupID:           groupID,
		transport:         transport,
		codec:             protocol.NewCodec(&protocol.Config{NodeID: nodeID, GroupID: string(groupID)}),
		drivers:           make(map[string]datasync.StorageDriver),
		syncStatus:        make(map[string]*datasync.SyncStatus),
		ctx:               ctx,
		cancel:            cancel,
		heartbeatInterval: 30 * time.Second,
		requestMap:        make(map[string]string),
		knownGroups:       map[datasync.GroupID]struct{}{groupID: {}},
	}

	if err := m.startSubscription(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to start subscription: %w", err)
	}

	go m.heartbeatLoop()
	return m, nil
}

// RegisterDriver 为指定数据类型注册存储驱动。
func (m *Manager) RegisterDriver(dataType string, driver datasync.StorageDriver) {
	m.driversMutex.Lock()
	defer m.driversMutex.Unlock()
	m.drivers[dataType] = driver
}

// SetAuthToken 设置出站信封中的认证令牌。
func (m *Manager) SetAuthToken(token string) {
	m.codec.SetAuthToken(token)
}

// SetAuthValidator 配置入站消息认证校验逻辑。
func (m *Manager) SetAuthValidator(validator func(string) bool) {
	m.authValidator = validator
}

// SetLeaderChecker 配置同步管理器如何判断 Leader 状态。
func (m *Manager) SetLeaderChecker(checker func() bool) {
	m.leaderChecker = checker
}

// Start 保持与包内接口兼容。
func (m *Manager) Start(ctx context.Context) error {
	return nil
}

// Stop 停止同步管理器。
func (m *Manager) Stop() error {
	m.cancel()
	return nil
}

// TriggerSync 向目标组发起一次全量同步请求。
func (m *Manager) TriggerSync(ctx context.Context, targetGroup datasync.GroupID, dataTypes []string) error {
	statusKey := fmt.Sprintf("%s->%s", m.groupID, targetGroup)
	requestID := generateRequestID()

	m.statusMutex.Lock()
	m.syncStatus[statusKey] = &datasync.SyncStatus{
		SourceGroup:    m.groupID,
		TargetGroup:    targetGroup,
		LastSyncTime:   time.Now(),
		NextSyncTime:   time.Now().Add(5 * time.Minute),
		SyncMode:       "full",
		PendingRecords: 0,
	}
	m.statusMutex.Unlock()

	m.requestMutex.Lock()
	m.requestMap[requestID] = statusKey
	m.requestMutex.Unlock()
	m.TrackGroup(targetGroup)

	req := &pb.SyncRequest{
		RequestId: requestID,
		SyncType:  pb.SyncMessage_FULL,
		DataTypes: dataTypes,
		Cursor:    "",
		Limit:     1000,
	}

	data, err := m.codec.EncodeSyncRequest(req)
	if err != nil {
		return fmt.Errorf("failed to encode sync request: %w", err)
	}
	if err := m.transport.SendMessage(ctx, targetGroup, data); err != nil {
		m.statusMutex.Lock()
		if status, ok := m.syncStatus[statusKey]; ok {
			status.LastSyncError = err.Error()
		}
		m.statusMutex.Unlock()
		return fmt.Errorf("failed to send sync request: %w", err)
	}

	return nil
}

// SendSyncMessage 向目标组发送同步消息。
func (m *Manager) SendSyncMessage(ctx context.Context, targetGroup datasync.GroupID, msg *pb.SyncMessage) error {
	data, err := m.codec.EncodeSyncMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to encode sync message: %w", err)
	}

	return m.transport.SendMessage(ctx, targetGroup, data)
}

// GetSyncStatus 返回两个组之间的同步状态。
func (m *Manager) GetSyncStatus(ctx context.Context, sourceGroup, targetGroup datasync.GroupID) (*datasync.SyncStatus, error) {
	statusKey := fmt.Sprintf("%s->%s", sourceGroup, targetGroup)

	m.statusMutex.RLock()
	defer m.statusMutex.RUnlock()

	status, ok := m.syncStatus[statusKey]
	if !ok {
		return nil, fmt.Errorf("sync status not found")
	}

	return status, nil
}

func (m *Manager) startSubscription() error {
	return m.transport.Subscribe(m.ctx, m.groupID, func(data []byte) {
		m.handleMessage(data)
	})
}

func (m *Manager) handleMessage(data []byte) {
	env, err := m.codec.DecodeEnvelope(data)
	if err != nil {
		return
	}
	if !m.codec.ValidateAuth(env, m.authValidator) {
		return
	}

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
	}
}

func (m *Manager) handleSyncRequest(env *pb.Envelope, req *pb.SyncRequest) {
	sourceNode, sourceGroup := protocol.GetNodeInfoFromEnvelope(env)
	m.TrackGroup(datasync.GroupID(sourceGroup))
	go m.processSyncRequest(req, datasync.GroupID(sourceGroup), sourceNode)
}

func (m *Manager) handleSyncMessage(env *pb.Envelope, msg *pb.SyncMessage) {
	_, sourceGroup := protocol.GetNodeInfoFromEnvelope(env)
	m.TrackGroup(datasync.GroupID(sourceGroup))
	go m.processSyncMessage(msg)
}

func (m *Manager) handleSyncResponse(env *pb.Envelope, resp *pb.SyncResponse) {
	_, sourceGroup := protocol.GetNodeInfoFromEnvelope(env)
	m.TrackGroup(datasync.GroupID(sourceGroup))
	go m.processSyncResponse(resp)
}

func (m *Manager) handleHeartbeat(env *pb.Envelope, hb *pb.Heartbeat) {
	_, sourceGroup := protocol.GetNodeInfoFromEnvelope(env)
	m.TrackGroup(datasync.GroupID(sourceGroup))

	m.statusMutex.Lock()
	defer m.statusMutex.Unlock()

	statusKey := fmt.Sprintf("%s->%s", hb.GetGroupId(), m.groupID)
	status, ok := m.syncStatus[statusKey]
	if !ok {
		status = &datasync.SyncStatus{
			SourceGroup: datasync.GroupID(hb.GetGroupId()),
			TargetGroup: m.groupID,
			SyncMode:    "heartbeat",
		}
		m.syncStatus[statusKey] = status
	}
	status.LastSyncTime = time.Now()
	status.NextSyncTime = time.Now().Add(m.heartbeatInterval)
}

func (m *Manager) processSyncRequest(req *pb.SyncRequest, sourceGroup datasync.GroupID, sourceNode string) {
	m.msgMutex.Lock()
	m.processedMessages++
	m.msgMutex.Unlock()

	dataTypes := req.DataTypes
	if len(dataTypes) == 0 {
		m.driversMutex.RLock()
		for dataType := range m.drivers {
			dataTypes = append(dataTypes, dataType)
		}
		m.driversMutex.RUnlock()
	}

	switch req.SyncType {
	case pb.SyncMessage_FULL:
		m.processFullSyncRequest(req, sourceGroup, dataTypes)
	case pb.SyncMessage_INCREMENTAL:
		m.processIncrementalSyncRequest(req, sourceGroup, dataTypes)
	default:
		m.sendSyncErrorResponse(req.RequestId, sourceGroup, "unknown sync type")
	}
	_ = sourceNode
}

func (m *Manager) processFullSyncRequest(req *pb.SyncRequest, targetGroup datasync.GroupID, dataTypes []string) {
	for _, dataType := range dataTypes {
		m.driversMutex.RLock()
		driver, ok := m.drivers[dataType]
		m.driversMutex.RUnlock()
		if !ok {
			continue
		}

		cursor := req.Cursor
		hasMore := true
		for hasMore {
			records, nextCursor, err := driver.GetRecords(m.ctx, dataType, cursor, int(req.Limit))
			if err != nil {
				m.sendSyncErrorResponse(req.RequestId, targetGroup, fmt.Sprintf("failed to get records: %v", err))
				return
			}

			pbRecords := make([]*pb.DataRecord, 0, len(records))
			for _, record := range records {
				pbRecords = append(pbRecords, convertToPBDataRecord(record))
			}

			hasMore = nextCursor != ""
			cursor = nextCursor
			msg := &pb.SyncMessage{
				MessageId: generateRequestID(),
				Type:      pb.SyncMessage_FULL,
				Records:   pbRecords,
				Cursor:    cursor,
				HasMore:   hasMore,
			}
			if err := m.SendSyncMessage(m.ctx, targetGroup, msg); err != nil {
				m.sendSyncErrorResponse(req.RequestId, targetGroup, fmt.Sprintf("failed to send sync message: %v", err))
				return
			}
			if !hasMore {
				break
			}
		}
	}

	resp := &pb.SyncResponse{RequestId: req.RequestId, Success: true}
	if err := m.sendSyncResponse(targetGroup, resp); err != nil {
		m.sendSyncErrorResponse(req.RequestId, targetGroup, fmt.Sprintf("failed to send success response: %v", err))
	}
}

func (m *Manager) processIncrementalSyncRequest(req *pb.SyncRequest, targetGroup datasync.GroupID, dataTypes []string) {
	var since time.Time
	if req.Since != nil {
		since = req.Since.AsTime()
	}

	for _, dataType := range dataTypes {
		m.driversMutex.RLock()
		driver, ok := m.drivers[dataType]
		m.driversMutex.RUnlock()
		if !ok {
			continue
		}

		changes, err := driver.GetChanges(m.ctx, dataType, since, int(req.Limit))
		if err != nil {
			m.sendSyncErrorResponse(req.RequestId, targetGroup, fmt.Sprintf("failed to get changes: %v", err))
			return
		}

		pbRecords := make([]*pb.DataRecord, 0, len(changes))
		for _, record := range changes {
			pbRecords = append(pbRecords, convertToPBDataRecord(record))
		}

		msg := &pb.SyncMessage{
			MessageId: generateRequestID(),
			Type:      pb.SyncMessage_INCREMENTAL,
			Records:   pbRecords,
			HasMore:   false,
		}
		if err := m.SendSyncMessage(m.ctx, targetGroup, msg); err != nil {
			m.sendSyncErrorResponse(req.RequestId, targetGroup, fmt.Sprintf("failed to send sync message: %v", err))
			return
		}
	}

	resp := &pb.SyncResponse{RequestId: req.RequestId, Success: true}
	if err := m.sendSyncResponse(targetGroup, resp); err != nil {
		m.sendSyncErrorResponse(req.RequestId, targetGroup, fmt.Sprintf("failed to send success response: %v", err))
	}
}

func (m *Manager) processSyncMessage(msg *pb.SyncMessage) {
	m.msgMutex.Lock()
	m.processedMessages++
	m.msgMutex.Unlock()

	for _, pbRecord := range msg.Records {
		record := convertFromPBDataRecord(pbRecord)

		m.driversMutex.RLock()
		driver, ok := m.drivers[record.Type]
		m.driversMutex.RUnlock()
		if !ok {
			continue
		}

		if err := driver.ApplyRecord(m.ctx, record); err != nil {
			continue
		}
	}
}

func (m *Manager) processSyncResponse(resp *pb.SyncResponse) {
	m.msgMutex.Lock()
	m.processedMessages++
	m.msgMutex.Unlock()

	m.requestMutex.Lock()
	statusKey, ok := m.requestMap[resp.RequestId]
	if ok {
		delete(m.requestMap, resp.RequestId)
	}
	m.requestMutex.Unlock()
	if !ok {
		return
	}

	m.statusMutex.Lock()
	defer m.statusMutex.Unlock()

	status, exists := m.syncStatus[statusKey]
	if !exists {
		return
	}
	status.LastSyncTime = time.Now()
	status.NextSyncTime = time.Now().Add(5 * time.Minute)
	status.PendingRecords = 0
	if resp.Success {
		status.LastSyncError = ""
		return
	}
	status.LastSyncError = resp.ErrorMessage
}

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

// SetErrorHandler 设置同步管理器错误处理器。
func (m *Manager) SetErrorHandler(handler ErrorHandler) {
	m.errorHandler = handler
}

// TrackGroup 记录一个需要参与心跳和状态维护的组。
func (m *Manager) TrackGroup(groupID datasync.GroupID) {
	if groupID == "" {
		return
	}

	m.groupsMutex.Lock()
	defer m.groupsMutex.Unlock()
	m.knownGroups[groupID] = struct{}{}
}

func (m *Manager) sendSyncErrorResponse(requestID string, targetGroup datasync.GroupID, errorMsg string) {
	resp := &pb.SyncResponse{RequestId: requestID, Success: false, ErrorMessage: errorMsg}
	if err := m.sendSyncResponse(targetGroup, resp); err != nil && m.errorHandler != nil {
		m.errorHandler(m.ctx, fmt.Errorf("failed to send error response: %w", err))
	}
}

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

func (m *Manager) sendHeartbeat() {
	m.msgMutex.RLock()
	processedCount := m.processedMessages
	m.msgMutex.RUnlock()

	hb := &pb.Heartbeat{
		NodeId:            m.nodeID,
		GroupId:           string(m.groupID),
		IsLeader:          m.isLeader(),
		ProcessedMessages: processedCount,
	}

	data, err := m.codec.EncodeHeartbeat(hb)
	if err != nil {
		return
	}

	for _, targetGroup := range m.heartbeatTargets() {
		if err := m.transport.SendMessage(m.ctx, targetGroup, data); err != nil && m.errorHandler != nil {
			m.errorHandler(m.ctx, fmt.Errorf("failed to send heartbeat to %s: %w", targetGroup, err))
		}
	}
}

func generateRequestID() string {
	return fmt.Sprintf("req_%d", time.Now().UnixNano())
}

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

func (m *Manager) heartbeatTargets() []datasync.GroupID {
	m.groupsMutex.RLock()
	defer m.groupsMutex.RUnlock()

	targets := make([]datasync.GroupID, 0, len(m.knownGroups))
	for groupID := range m.knownGroups {
		targets = append(targets, groupID)
	}
	return targets
}

func (m *Manager) isLeader() bool {
	if m.leaderChecker == nil {
		return false
	}
	return m.leaderChecker()
}
