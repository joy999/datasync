package protocol

import (
	"context"
	"testing"

	pb "github.com/joy999/datasync/proto"
)

func TestCodec_WithConfig(t *testing.T) {
	config := &Config{
		NodeID:    "node-1",
		GroupID:   "group-a",
		AuthToken: "secret-token-123",
	}
	codec := NewCodec(config)

	msg := &pb.SyncMessage{
		MessageId: "msg-123",
		Type:      pb.SyncMessage_FULL,
	}

	encoded, err := codec.EncodeSyncMessage(msg)
	if err != nil {
		t.Fatalf("EncodeSyncMessage() error = %v", err)
	}

	// 解码并验证信封信息
	env, decodedMsg, err := codec.DecodeSyncMessage(encoded)
	if err != nil {
		t.Fatalf("DecodeSyncMessage() error = %v", err)
	}

	// 验证信封中的节点信息
	if env.SourceNode != "node-1" {
		t.Errorf("SourceNode = %s, want node-1", env.SourceNode)
	}
	if env.SourceGroup != "group-a" {
		t.Errorf("SourceGroup = %s, want group-a", env.SourceGroup)
	}
	if env.AuthToken != "secret-token-123" {
		t.Errorf("AuthToken = %s, want secret-token-123", env.AuthToken)
	}
	if env.Version != 1 {
		t.Errorf("Version = %d, want 1", env.Version)
	}

	// 验证消息内容
	if decodedMsg.MessageId != msg.MessageId {
		t.Errorf("MessageId = %s, want %s", decodedMsg.MessageId, msg.MessageId)
	}
}

func TestCodec_UpdateConfig(t *testing.T) {
	codec := NewCodec(nil)

	// 更新配置
	codec.UpdateConfig(&Config{
		NodeID:    "node-2",
		AuthToken: "new-token",
	})

	req := &pb.SyncRequest{RequestId: "req-456"}
	encoded, _ := codec.EncodeSyncRequest(req)

	env, _, _ := codec.DecodeSyncRequest(encoded)
	if env.SourceNode != "node-2" {
		t.Errorf("SourceNode = %s, want node-2", env.SourceNode)
	}
	if env.AuthToken != "new-token" {
		t.Errorf("AuthToken = %s, want new-token", env.AuthToken)
	}
}

func TestCodec_SetAuthToken(t *testing.T) {
	codec := NewCodec(&Config{NodeID: "node-1"})
	codec.SetAuthToken("dynamic-token")

	hb := &pb.Heartbeat{NodeId: "node-1"}
	encoded, _ := codec.EncodeHeartbeat(hb)

	env, _, _ := codec.DecodeHeartbeat(encoded)
	if env.AuthToken != "dynamic-token" {
		t.Errorf("AuthToken = %s, want dynamic-token", env.AuthToken)
	}
}

func TestCodec_EncodeDecodeSyncMessage(t *testing.T) {
	codec := NewCodec(&Config{NodeID: "node-1"})

	original := &pb.SyncMessage{
		MessageId: "msg-123",
		Type:      pb.SyncMessage_FULL,
		Records:   []*pb.DataRecord{},
		Cursor:    "cursor-1",
		HasMore:   true,
	}

	encoded, err := codec.EncodeSyncMessage(original)
	if err != nil {
		t.Fatalf("EncodeSyncMessage() error = %v", err)
	}

	env, decoded, err := codec.DecodeSyncMessage(encoded)
	if err != nil {
		t.Fatalf("DecodeSyncMessage() error = %v", err)
	}

	if decoded.MessageId != original.MessageId {
		t.Errorf("MessageId = %s, want %s", decoded.MessageId, original.MessageId)
	}
	if decoded.Type != original.Type {
		t.Errorf("Type = %v, want %v", decoded.Type, original.Type)
	}
	if decoded.HasMore != original.HasMore {
		t.Errorf("HasMore = %v, want %v", decoded.HasMore, original.HasMore)
	}

	_ = env // 可以使用 env 获取发送者信息
}

func TestCodec_EncodeDecodeSyncRequest(t *testing.T) {
	codec := NewCodec(&Config{NodeID: "node-1"})

	original := &pb.SyncRequest{
		RequestId: "req-456",
		SyncType:  pb.SyncMessage_INCREMENTAL,
		DataTypes: []string{"users", "orders"},
		Cursor:    "page-2",
		Limit:     100,
	}

	encoded, err := codec.EncodeSyncRequest(original)
	if err != nil {
		t.Fatalf("EncodeSyncRequest() error = %v", err)
	}

	env, decoded, err := codec.DecodeSyncRequest(encoded)
	if err != nil {
		t.Fatalf("DecodeSyncRequest() error = %v", err)
	}

	if decoded.RequestId != original.RequestId {
		t.Errorf("RequestId = %s, want %s", decoded.RequestId, original.RequestId)
	}
	if len(decoded.DataTypes) != len(original.DataTypes) {
		t.Errorf("DataTypes length = %d, want %d", len(decoded.DataTypes), len(original.DataTypes))
	}
	if decoded.Limit != original.Limit {
		t.Errorf("Limit = %d, want %d", decoded.Limit, original.Limit)
	}

	_ = env
}

func TestCodec_EncodeDecodeSyncResponse(t *testing.T) {
	codec := NewCodec(&Config{NodeID: "node-1"})

	original := &pb.SyncResponse{
		RequestId:    "req-789",
		Success:      true,
		ErrorMessage: "",
		Cursor:       "next-page",
		HasMore:      false,
	}

	encoded, err := codec.EncodeSyncResponse(original)
	if err != nil {
		t.Fatalf("EncodeSyncResponse() error = %v", err)
	}

	env, decoded, err := codec.DecodeSyncResponse(encoded)
	if err != nil {
		t.Fatalf("DecodeSyncResponse() error = %v", err)
	}

	if decoded.Success != original.Success {
		t.Errorf("Success = %v, want %v", decoded.Success, original.Success)
	}

	_ = env
}

func TestCodec_EncodeDecodeRaftMessage(t *testing.T) {
	codec := NewCodec(&Config{NodeID: "node-1"})

	original := &pb.RaftMessage{
		Type:  pb.RaftMessage_ENTRY,
		Data:  []byte("raft log entry data"),
		Term:  5,
		Index: 100,
	}

	encoded, err := codec.EncodeRaftMessage(original)
	if err != nil {
		t.Fatalf("EncodeRaftMessage() error = %v", err)
	}

	env, decoded, err := codec.DecodeRaftMessage(encoded)
	if err != nil {
		t.Fatalf("DecodeRaftMessage() error = %v", err)
	}

	if decoded.Term != original.Term {
		t.Errorf("Term = %d, want %d", decoded.Term, original.Term)
	}
	if decoded.Index != original.Index {
		t.Errorf("Index = %d, want %d", decoded.Index, original.Index)
	}

	_ = env
}

func TestCodec_EncodeDecodeHeartbeat(t *testing.T) {
	codec := NewCodec(&Config{NodeID: "node-1"})

	original := &pb.Heartbeat{
		NodeId:            "node-1",
		GroupId:           "group-a",
		IsLeader:          true,
		ProcessedMessages: 1000,
	}

	encoded, err := codec.EncodeHeartbeat(original)
	if err != nil {
		t.Fatalf("EncodeHeartbeat() error = %v", err)
	}

	env, decoded, err := codec.DecodeHeartbeat(encoded)
	if err != nil {
		t.Fatalf("DecodeHeartbeat() error = %v", err)
	}

	if decoded.NodeId != original.NodeId {
		t.Errorf("NodeId = %s, want %s", decoded.NodeId, original.NodeId)
	}
	if decoded.IsLeader != original.IsLeader {
		t.Errorf("IsLeader = %v, want %v", decoded.IsLeader, original.IsLeader)
	}

	_ = env
}

func TestCodec_WrongPayloadType(t *testing.T) {
	codec := NewCodec(&Config{NodeID: "node-1"})

	// 编码 SyncMessage
	msg := &pb.SyncMessage{MessageId: "test"}
	encoded, _ := codec.EncodeSyncMessage(msg)

	// 尝试解码为 SyncRequest（应该失败）
	_, _, err := codec.DecodeSyncRequest(encoded)
	if err == nil {
		t.Error("DecodeSyncRequest() should fail when given SyncMessage data")
	}
}

func TestCodec_DecodeEmpty(t *testing.T) {
	codec := NewCodec(&Config{NodeID: "node-1"})

	_, _, err := codec.DecodeSyncMessage([]byte{})
	if err == nil {
		t.Error("DecodeSyncMessage() should fail for empty data")
	}
}

func TestCodec_GetPayloadType(t *testing.T) {
	codec := NewCodec(&Config{NodeID: "node-1"})

	tests := []struct {
		name     string
		encode   func() []byte
		expected string
	}{
		{"SyncMessage", func() []byte { d, _ := codec.EncodeSyncMessage(&pb.SyncMessage{}); return d }, "SyncMessage"},
		{"SyncRequest", func() []byte { d, _ := codec.EncodeSyncRequest(&pb.SyncRequest{}); return d }, "SyncRequest"},
		{"RaftMessage", func() []byte { d, _ := codec.EncodeRaftMessage(&pb.RaftMessage{}); return d }, "RaftMessage"},
		{"Heartbeat", func() []byte { d, _ := codec.EncodeHeartbeat(&pb.Heartbeat{}); return d }, "Heartbeat"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := tt.encode()
			env, _ := codec.DecodeEnvelope(data)
			payloadType := codec.GetPayloadType(env)
			if payloadType != tt.expected {
				t.Errorf("GetPayloadType() = %s, want %s", payloadType, tt.expected)
			}
		})
	}
}

func TestCodec_ValidateAuth(t *testing.T) {
	config := &Config{
		NodeID:    "node-1",
		AuthToken: "valid-token",
	}
	codec := NewCodec(config)

	req := &pb.SyncRequest{RequestId: "req-123"}
	encoded, _ := codec.EncodeSyncRequest(req)
	env, _ := codec.DecodeEnvelope(encoded)

	// 验证通过的情况
	valid := codec.ValidateAuth(env, func(token string) bool {
		return token == "valid-token"
	})
	if !valid {
		t.Error("ValidateAuth() should return true for valid token")
	}

	// 验证失败的情况
	invalid := codec.ValidateAuth(env, func(token string) bool {
		return token == "wrong-token"
	})
	if invalid {
		t.Error("ValidateAuth() should return false for wrong token")
	}

	// 没有验证器时默认通过
	defaultValid := codec.ValidateAuth(env, nil)
	if !defaultValid {
		t.Error("ValidateAuth() should return true when validator is nil")
	}
}

func TestHelperFunctions(t *testing.T) {
	config := &Config{
		NodeID:    "test-node",
		GroupID:   "test-group",
		AuthToken: "test-token",
	}
	codec := NewCodec(config)

	req := &pb.SyncRequest{RequestId: "req-test"}
	encoded, _ := codec.EncodeSyncRequest(req)
	env, _ := codec.DecodeEnvelope(encoded)

	// 测试 GetNodeInfoFromEnvelope
	nodeID, groupID := GetNodeInfoFromEnvelope(env)
	if nodeID != "test-node" {
		t.Errorf("GetNodeInfoFromEnvelope nodeID = %s, want test-node", nodeID)
	}
	if groupID != "test-group" {
		t.Errorf("GetNodeInfoFromEnvelope groupID = %s, want test-group", groupID)
	}

	// 测试 GetAuthTokenFromEnvelope
	token := GetAuthTokenFromEnvelope(env)
	if token != "test-token" {
		t.Errorf("GetAuthTokenFromEnvelope = %s, want test-token", token)
	}

	// 测试 GetTimestampFromEnvelope
	ts := GetTimestampFromEnvelope(env)
	if ts.IsZero() {
		t.Error("GetTimestampFromEnvelope should not be zero")
	}

	// 测试 nil 处理
	nodeID, groupID = GetNodeInfoFromEnvelope(nil)
	if nodeID != "" || groupID != "" {
		t.Error("GetNodeInfoFromEnvelope(nil) should return empty strings")
	}

	token = GetAuthTokenFromEnvelope(nil)
	if token != "" {
		t.Error("GetAuthTokenFromEnvelope(nil) should return empty string")
	}
}

func TestEnvelopeContext(t *testing.T) {
	ctx := context.Background()
	config := &Config{NodeID: "node-1"}
	codec := NewCodec(config)

	req := &pb.SyncRequest{RequestId: "req-123"}
	encoded, _ := codec.EncodeSyncRequest(req)
	env, _ := codec.DecodeEnvelope(encoded)

	envCtx := NewEnvelopeContext(ctx, env)
	if envCtx.Envelope != env {
		t.Error("NewEnvelopeContext should store envelope correctly")
	}

	// 测试 Context 接口
	if envCtx.Context != ctx {
		t.Error("NewEnvelopeContext should store context correctly")
	}
}

func BenchmarkCodec_EncodeSyncMessage(b *testing.B) {
	codec := NewCodec(&Config{NodeID: "node-1"})
	msg := &pb.SyncMessage{
		MessageId: "benchmark-msg",
		Type:      pb.SyncMessage_FULL,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = codec.EncodeSyncMessage(msg)
	}
}

func BenchmarkCodec_DecodeSyncMessage(b *testing.B) {
	codec := NewCodec(&Config{NodeID: "node-1"})
	msg := &pb.SyncMessage{
		MessageId: "benchmark-msg",
		Type:      pb.SyncMessage_FULL,
	}
	encoded, _ := codec.EncodeSyncMessage(msg)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = codec.DecodeSyncMessage(encoded)
	}
}
