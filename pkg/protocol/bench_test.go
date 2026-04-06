package protocol

import (
	"fmt"
	"testing"

	pb "github.com/joy999/datasync/proto"
)

// BenchmarkEncodeSyncMessage 编码同步消息基准测试
func BenchmarkEncodeSyncMessage(b *testing.B) {
	codec := NewCodec(&Config{NodeID: "node-1"})
	msg := &pb.SyncMessage{
		MessageId: "benchmark-msg",
		Type:      pb.SyncMessage_FULL,
		Records:   []*pb.DataRecord{},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = codec.EncodeSyncMessage(msg)
	}
}

// BenchmarkDecodeSyncMessage 解码同步消息基准测试
func BenchmarkDecodeSyncMessage(b *testing.B) {
	codec := NewCodec(&Config{NodeID: "node-1"})
	msg := &pb.SyncMessage{
		MessageId: "benchmark-msg",
		Type:      pb.SyncMessage_FULL,
	}
	data, _ := codec.EncodeSyncMessage(msg)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = codec.DecodeSyncMessage(data)
	}
}

// BenchmarkEncodeSyncMessageWithRecords 带记录的编码基准测试
func BenchmarkEncodeSyncMessageWithRecords(b *testing.B) {
	codec := NewCodec(&Config{NodeID: "node-1"})

	// 创建包含多条记录的批次
	records := make([]*pb.DataRecord, 100)
	for i := 0; i < 100; i++ {
		records[i] = &pb.DataRecord{
			Id:      string(rune('a' + i%26)),
			Type:    "users",
			Version: int64(i),
			Payload: []byte(`{"name":"User","data":"some payload data here"}`),
		}
	}

	msg := &pb.SyncMessage{
		MessageId: "batch-msg",
		Type:      pb.SyncMessage_FULL,
		Records:   records,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = codec.EncodeSyncMessage(msg)
	}
}

// BenchmarkEncodeHeartbeat 编码心跳消息基准测试
func BenchmarkEncodeHeartbeat(b *testing.B) {
	codec := NewCodec(&Config{NodeID: "node-1"})
	hb := &pb.Heartbeat{
		NodeId:            "node-1",
		GroupId:           "group-a",
		IsLeader:          true,
		ProcessedMessages: 1000000,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = codec.EncodeHeartbeat(hb)
	}
}

// BenchmarkDecodeHeartbeat 解码心跳消息基准测试
func BenchmarkDecodeHeartbeat(b *testing.B) {
	codec := NewCodec(&Config{NodeID: "node-1"})
	hb := &pb.Heartbeat{
		NodeId:   "node-1",
		GroupId:  "group-a",
		IsLeader: true,
	}
	data, _ := codec.EncodeHeartbeat(hb)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = codec.DecodeHeartbeat(data)
	}
}

// BenchmarkDecodeEnvelope 解码信封基准测试
func BenchmarkDecodeEnvelope(b *testing.B) {
	codec := NewCodec(&Config{NodeID: "node-1"})
	msg := &pb.SyncMessage{MessageId: "test"}
	data, _ := codec.EncodeSyncMessage(msg)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = codec.DecodeEnvelope(data)
	}
}

// BenchmarkGetPayloadType 获取消息类型基准测试
func BenchmarkGetPayloadType(b *testing.B) {
	codec := NewCodec(&Config{NodeID: "node-1"})
	msg := &pb.SyncMessage{MessageId: "test"}
	data, _ := codec.EncodeSyncMessage(msg)
	env, _ := codec.DecodeEnvelope(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = codec.GetPayloadType(env)
	}
}

// BenchmarkGetNodeInfoFromEnvelope 从信封获取节点信息基准测试
func BenchmarkGetNodeInfoFromEnvelope(b *testing.B) {
	codec := NewCodec(&Config{
		NodeID:  "node-1",
		GroupID: "group-a",
	})
	msg := &pb.SyncMessage{MessageId: "test"}
	data, _ := codec.EncodeSyncMessage(msg)
	env, _, _ := codec.DecodeSyncMessage(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = GetNodeInfoFromEnvelope(env)
	}
}

// BenchmarkGetTimestampFromEnvelope 从信封获取时间戳基准测试
func BenchmarkGetTimestampFromEnvelope(b *testing.B) {
	codec := NewCodec(&Config{NodeID: "node-1"})
	msg := &pb.SyncMessage{MessageId: "test"}
	data, _ := codec.EncodeSyncMessage(msg)
	env, _, _ := codec.DecodeSyncMessage(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = GetTimestampFromEnvelope(env)
	}
}

// BenchmarkValidateAuth 验证认证基准测试
func BenchmarkValidateAuth(b *testing.B) {
	codec := NewCodec(&Config{
		NodeID:    "node-1",
		AuthToken: "valid-token",
	})
	msg := &pb.SyncMessage{MessageId: "test"}
	data, _ := codec.EncodeSyncMessage(msg)
	env, _ := codec.DecodeEnvelope(data)

	validator := func(token string) bool {
		return token == "valid-token"
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = codec.ValidateAuth(env, validator)
	}
}

// 不同消息类型的编码性能对比
func BenchmarkDifferentMessageTypes(b *testing.B) {
	codec := NewCodec(&Config{NodeID: "node-1"})

	b.Run("SyncMessage", func(b *testing.B) {
		msg := &pb.SyncMessage{MessageId: "test"}
		for i := 0; i < b.N; i++ {
			_, _ = codec.EncodeSyncMessage(msg)
		}
	})

	b.Run("SyncRequest", func(b *testing.B) {
		req := &pb.SyncRequest{RequestId: "test"}
		for i := 0; i < b.N; i++ {
			_, _ = codec.EncodeSyncRequest(req)
		}
	})

	b.Run("SyncResponse", func(b *testing.B) {
		resp := &pb.SyncResponse{RequestId: "test"}
		for i := 0; i < b.N; i++ {
			_, _ = codec.EncodeSyncResponse(resp)
		}
	})

	b.Run("RaftMessage", func(b *testing.B) {
		rm := &pb.RaftMessage{Term: 1, Index: 100}
		for i := 0; i < b.N; i++ {
			_, _ = codec.EncodeRaftMessage(rm)
		}
	})

	b.Run("Heartbeat", func(b *testing.B) {
		hb := &pb.Heartbeat{NodeId: "node-1"}
		for i := 0; i < b.N; i++ {
			_, _ = codec.EncodeHeartbeat(hb)
		}
	})
}

// 不同记录数量的编码性能
func BenchmarkSyncMessageWithDifferentRecordCounts(b *testing.B) {
	recordCounts := []int{1, 10, 100, 1000}

	for _, count := range recordCounts {
		b.Run(fmt.Sprintf("Records%d", count), func(b *testing.B) {
			codec := NewCodec(&Config{NodeID: "node-1"})

			records := make([]*pb.DataRecord, count)
			for i := 0; i < count; i++ {
				records[i] = &pb.DataRecord{
					Id:      string(rune('a' + i%26)),
					Type:    "users",
					Version: int64(i),
					Payload: []byte(`{"name":"User"}`),
				}
			}

			msg := &pb.SyncMessage{
				MessageId: "batch",
				Records:   records,
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = codec.EncodeSyncMessage(msg)
			}
		})
	}
}

// 并行编码基准测试
func BenchmarkEncodeSyncMessage_Parallel(b *testing.B) {
	codec := NewCodec(&Config{NodeID: "node-1"})
	msg := &pb.SyncMessage{
		MessageId: "parallel-test",
		Type:      pb.SyncMessage_FULL,
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = codec.EncodeSyncMessage(msg)
		}
	})
}

// 内存分配基准测试
func BenchmarkEncodeSyncMessage_Allocs(b *testing.B) {
	codec := NewCodec(&Config{NodeID: "node-1"})
	msg := &pb.SyncMessage{
		MessageId: "alloc-test",
		Type:      pb.SyncMessage_FULL,
		Records: []*pb.DataRecord{
			{
				Id:      "user-1",
				Type:    "users",
				Payload: []byte(`{"name":"Alice"}`),
			},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = codec.EncodeSyncMessage(msg)
	}
}

// UpdateTimestamp 更新信封时间戳基准测试
func BenchmarkUpdateConfig(b *testing.B) {
	config := &Config{
		NodeID:    "node-1",
		GroupID:   "group-a",
		AuthToken: "token",
	}
	codec := NewCodec(config)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		codec.UpdateConfig(&Config{
			NodeID:    "node-2",
			GroupID:   "group-b",
			AuthToken: "new-token",
		})
	}
}

// SetAuthToken 设置认证令牌基准测试
func BenchmarkSetAuthToken(b *testing.B) {
	codec := NewCodec(&Config{NodeID: "node-1"})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		codec.SetAuthToken("token")
	}
}
