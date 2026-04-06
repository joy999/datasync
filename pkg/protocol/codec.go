package protocol

import (
	"context"
	"fmt"
	"time"

	pb "github.com/joy999/datasync/proto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Config 协议编解码器配置
type Config struct {
	// 节点身份信息
	NodeID    string
	GroupID   string
	AuthToken string // 认证令牌（可选）
}

// Codec 协议编解码器
type Codec struct {
	config *Config
}

// NewCodec 创建新的协议编解码器
func NewCodec(config *Config) *Codec {
	if config == nil {
		config = &Config{}
	}
	return &Codec{config: config}
}

// UpdateConfig 更新编解码器配置
func (c *Codec) UpdateConfig(config *Config) {
	c.config = config
}

// SetAuthToken 设置认证令牌
func (c *Codec) SetAuthToken(token string) {
	c.config.AuthToken = token
}

// buildEnvelope 构建信封（统一填充节点信息和认证信息）
func (c *Codec) buildEnvelope(messageID string) *pb.Envelope {
	return &pb.Envelope{
		Version:     1, // 协议版本
		SourceNode:  c.config.NodeID,
		SourceGroup: c.config.GroupID,
		AuthToken:   c.config.AuthToken,
		Timestamp:   timestamppb.New(time.Now()),
		MessageId:   messageID,
	}
}

// EncodeSyncMessage 编码同步消息
func (c *Codec) EncodeSyncMessage(msg *pb.SyncMessage) ([]byte, error) {
	env := c.buildEnvelope(msg.MessageId)
	env.Payload = &pb.Envelope_SyncMessage{SyncMessage: msg}

	data, err := proto.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal envelope with SyncMessage: %w", err)
	}
	return data, nil
}

// DecodeSyncMessage 解码同步消息
func (c *Codec) DecodeSyncMessage(data []byte) (*pb.Envelope, *pb.SyncMessage, error) {
	env, err := c.decodeEnvelope(data)
	if err != nil {
		return nil, nil, err
	}

	msg, ok := env.Payload.(*pb.Envelope_SyncMessage)
	if !ok {
		return nil, nil, fmt.Errorf("payload is not SyncMessage, got %T", env.Payload)
	}

	return env, msg.SyncMessage, nil
}

// EncodeSyncRequest 编码同步请求
func (c *Codec) EncodeSyncRequest(req *pb.SyncRequest) ([]byte, error) {
	env := c.buildEnvelope(req.RequestId)
	env.Payload = &pb.Envelope_SyncRequest{SyncRequest: req}

	data, err := proto.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal envelope with SyncRequest: %w", err)
	}
	return data, nil
}

// DecodeSyncRequest 解码同步请求
func (c *Codec) DecodeSyncRequest(data []byte) (*pb.Envelope, *pb.SyncRequest, error) {
	env, err := c.decodeEnvelope(data)
	if err != nil {
		return nil, nil, err
	}

	req, ok := env.Payload.(*pb.Envelope_SyncRequest)
	if !ok {
		return nil, nil, fmt.Errorf("payload is not SyncRequest, got %T", env.Payload)
	}

	return env, req.SyncRequest, nil
}

// EncodeSyncResponse 编码同步响应
func (c *Codec) EncodeSyncResponse(resp *pb.SyncResponse) ([]byte, error) {
	env := c.buildEnvelope(resp.RequestId)
	env.Payload = &pb.Envelope_SyncResponse{SyncResponse: resp}

	data, err := proto.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal envelope with SyncResponse: %w", err)
	}
	return data, nil
}

// DecodeSyncResponse 解码同步响应
func (c *Codec) DecodeSyncResponse(data []byte) (*pb.Envelope, *pb.SyncResponse, error) {
	env, err := c.decodeEnvelope(data)
	if err != nil {
		return nil, nil, err
	}

	res, ok := env.Payload.(*pb.Envelope_SyncResponse)
	if !ok {
		return nil, nil, fmt.Errorf("payload is not SyncResponse, got %T", env.Payload)
	}

	return env, res.SyncResponse, nil
}

// EncodeRaftMessage 编码 Raft 消息
func (c *Codec) EncodeRaftMessage(msg *pb.RaftMessage) ([]byte, error) {
	// Raft 消息使用 index 作为 message ID
	env := c.buildEnvelope(fmt.Sprintf("raft-%d-%d", msg.Term, msg.Index))
	env.Payload = &pb.Envelope_RaftMessage{RaftMessage: msg}

	data, err := proto.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal envelope with RaftMessage: %w", err)
	}
	return data, nil
}

// DecodeRaftMessage 解码 Raft 消息
func (c *Codec) DecodeRaftMessage(data []byte) (*pb.Envelope, *pb.RaftMessage, error) {
	env, err := c.decodeEnvelope(data)
	if err != nil {
		return nil, nil, err
	}

	msg, ok := env.Payload.(*pb.Envelope_RaftMessage)
	if !ok {
		return nil, nil, fmt.Errorf("payload is not RaftMessage, got %T", env.Payload)
	}

	return env, msg.RaftMessage, nil
}

// EncodeDiscoveryMessage 编码节点发现消息
func (c *Codec) EncodeDiscoveryMessage(msg *pb.DiscoveryMessage) ([]byte, error) {
	env := c.buildEnvelope("")
	env.Payload = &pb.Envelope_DiscoveryMessage{DiscoveryMessage: msg}

	data, err := proto.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal envelope with DiscoveryMessage: %w", err)
	}
	return data, nil
}

// DecodeDiscoveryMessage 解码节点发现消息
func (c *Codec) DecodeDiscoveryMessage(data []byte) (*pb.Envelope, *pb.DiscoveryMessage, error) {
	env, err := c.decodeEnvelope(data)
	if err != nil {
		return nil, nil, err
	}

	msg, ok := env.Payload.(*pb.Envelope_DiscoveryMessage)
	if !ok {
		return nil, nil, fmt.Errorf("payload is not DiscoveryMessage, got %T", env.Payload)
	}

	return env, msg.DiscoveryMessage, nil
}

// EncodeAuthMessage 编码认证消息
func (c *Codec) EncodeAuthMessage(msg *pb.AuthMessage) ([]byte, error) {
	env := c.buildEnvelope("")
	env.Payload = &pb.Envelope_AuthMessage{AuthMessage: msg}

	data, err := proto.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal envelope with AuthMessage: %w", err)
	}
	return data, nil
}

// DecodeAuthMessage 解码认证消息
func (c *Codec) DecodeAuthMessage(data []byte) (*pb.Envelope, *pb.AuthMessage, error) {
	env, err := c.decodeEnvelope(data)
	if err != nil {
		return nil, nil, err
	}

	msg, ok := env.Payload.(*pb.Envelope_AuthMessage)
	if !ok {
		return nil, nil, fmt.Errorf("payload is not AuthMessage, got %T", env.Payload)
	}

	return env, msg.AuthMessage, nil
}

// EncodeConfigChange 编码配置变更消息
func (c *Codec) EncodeConfigChange(msg *pb.ConfigChange) ([]byte, error) {
	env := c.buildEnvelope("")
	env.Payload = &pb.Envelope_ConfigChange{ConfigChange: msg}

	data, err := proto.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal envelope with ConfigChange: %w", err)
	}
	return data, nil
}

// DecodeConfigChange 解码配置变更消息
func (c *Codec) DecodeConfigChange(data []byte) (*pb.Envelope, *pb.ConfigChange, error) {
	env, err := c.decodeEnvelope(data)
	if err != nil {
		return nil, nil, err
	}

	msg, ok := env.Payload.(*pb.Envelope_ConfigChange)
	if !ok {
		return nil, nil, fmt.Errorf("payload is not ConfigChange, got %T", env.Payload)
	}

	return env, msg.ConfigChange, nil
}

// EncodeSnapshotData 编码快照数据
func (c *Codec) EncodeSnapshotData(data *pb.SnapshotData) ([]byte, error) {
	env := c.buildEnvelope(fmt.Sprintf("snapshot-%d-%d", data.Term, data.Index))
	env.Payload = &pb.Envelope_SnapshotData{SnapshotData: data}

	payload, err := proto.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal envelope with SnapshotData: %w", err)
	}
	return payload, nil
}

// DecodeSnapshotData 解码快照数据
func (c *Codec) DecodeSnapshotData(data []byte) (*pb.Envelope, *pb.SnapshotData, error) {
	env, err := c.decodeEnvelope(data)
	if err != nil {
		return nil, nil, err
	}

	msg, ok := env.Payload.(*pb.Envelope_SnapshotData)
	if !ok {
		return nil, nil, fmt.Errorf("payload is not SnapshotData, got %T", env.Payload)
	}

	return env, msg.SnapshotData, nil
}

// EncodeHeartbeat 编码心跳消息
func (c *Codec) EncodeHeartbeat(hb *pb.Heartbeat) ([]byte, error) {
	env := c.buildEnvelope(fmt.Sprintf("hb-%d", time.Now().UnixNano()))
	env.Payload = &pb.Envelope_Heartbeat{Heartbeat: hb}

	data, err := proto.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal envelope with Heartbeat: %w", err)
	}
	return data, nil
}

// DecodeHeartbeat 解码心跳消息
func (c *Codec) DecodeHeartbeat(data []byte) (*pb.Envelope, *pb.Heartbeat, error) {
	env, err := c.decodeEnvelope(data)
	if err != nil {
		return nil, nil, err
	}

	hb, ok := env.Payload.(*pb.Envelope_Heartbeat)
	if !ok {
		return nil, nil, fmt.Errorf("payload is not Heartbeat, got %T", env.Payload)
	}

	return env, hb.Heartbeat, nil
}

// DecodeEnvelope 解码信封（不解包具体消息类型）
func (c *Codec) DecodeEnvelope(data []byte) (*pb.Envelope, error) {
	return c.decodeEnvelope(data)
}

// decodeEnvelope 解码信封内部方法
func (c *Codec) decodeEnvelope(data []byte) (*pb.Envelope, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty data")
	}

	var env pb.Envelope
	if err := proto.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("failed to unmarshal envelope: %w", err)
	}

	// 验证协议版本
	if env.Version != 1 {
		return nil, fmt.Errorf("unsupported envelope version: %d", env.Version)
	}

	return &env, nil
}

// GetPayloadType 获取信封中的负载类型
func (c *Codec) GetPayloadType(env *pb.Envelope) string {
	switch env.Payload.(type) {
	case *pb.Envelope_SyncMessage:
		return "SyncMessage"
	case *pb.Envelope_SyncRequest:
		return "SyncRequest"
	case *pb.Envelope_SyncResponse:
		return "SyncResponse"
	case *pb.Envelope_RaftMessage:
		return "RaftMessage"
	case *pb.Envelope_DiscoveryMessage:
		return "DiscoveryMessage"
	case *pb.Envelope_AuthMessage:
		return "AuthMessage"
	case *pb.Envelope_ConfigChange:
		return "ConfigChange"
	case *pb.Envelope_SnapshotData:
		return "SnapshotData"
	case *pb.Envelope_Heartbeat:
		return "Heartbeat"
	default:
		return "Unknown"
	}
}

// ValidateAuth 验证信封中的认证信息（简单实现）
func (c *Codec) ValidateAuth(env *pb.Envelope, validator func(token string) bool) bool {
	if validator == nil {
		// 没有验证器，默认通过
		return true
	}
	return validator(env.AuthToken)
}

// EnvelopeContext 信封上下文，用于传递信封信息
type EnvelopeContext struct {
	context.Context
	Envelope *pb.Envelope
}

// NewEnvelopeContext 创建信封上下文
func NewEnvelopeContext(ctx context.Context, env *pb.Envelope) *EnvelopeContext {
	return &EnvelopeContext{
		Context:  ctx,
		Envelope: env,
	}
}

// GetNodeInfoFromEnvelope 从信封获取节点信息
func GetNodeInfoFromEnvelope(env *pb.Envelope) (nodeID, groupID string) {
	if env == nil {
		return "", ""
	}
	return env.SourceNode, env.SourceGroup
}

// GetAuthTokenFromEnvelope 从信封获取认证令牌
func GetAuthTokenFromEnvelope(env *pb.Envelope) string {
	if env == nil {
		return ""
	}
	return env.AuthToken
}

// GetTimestampFromEnvelope 从信封获取时间戳
func GetTimestampFromEnvelope(env *pb.Envelope) time.Time {
	if env == nil || env.Timestamp == nil {
		return time.Time{}
	}
	return env.Timestamp.AsTime()
}
