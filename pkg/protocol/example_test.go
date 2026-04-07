package protocol_test

import (
	"context"
	"fmt"
	"log"

	"github.com/joy999/datasync/pkg/protocol"
	pb "github.com/joy999/datasync/proto"
)

// ExampleCodec_EncodeSyncMessage 展示如何编码同步消息
func ExampleCodec_EncodeSyncMessage() {
	// 创建编解码器
	codec := protocol.NewCodec(&protocol.Config{
		NodeID:    "node-1",
		GroupID:   "group-a",
		AuthToken: "secret-token",
	})

	// 创建同步消息
	msg := &pb.SyncMessage{
		MessageId: "msg-001",
		Type:      pb.SyncMessage_FULL,
		Records: []*pb.DataRecord{
			{
				Id:      "user-1",
				Type:    "users",
				Version: 1,
				Payload: []byte(`{"name":"Alice"}`),
			},
		},
		HasMore: false,
	}

	// 编码消息
	_, err := codec.EncodeSyncMessage(msg)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Encoded message successfully")
	// Output: Encoded message successfully
}

// ExampleCodec_DecodeSyncMessage 展示如何解码同步消息
func ExampleCodec_DecodeSyncMessage() {
	// 创建编解码器
	codec := protocol.NewCodec(&protocol.Config{
		NodeID:  "node-2",
		GroupID: "group-b",
	})

	// 假设这是接收到的数据
	// 实际场景中，这些数据来自网络传输
	encodedData := []byte{ /* ... */ }

	// 解码信封和消息
	env, msg, err := codec.DecodeSyncMessage(encodedData)
	if err != nil {
		// 在实际应用中处理错误
		fmt.Printf("Decode error: %v\n", err)
		return
	}

	// 获取发送者信息
	nodeID, groupID := protocol.GetNodeInfoFromEnvelope(env)
	fmt.Printf("Received message from %s in group %s\n", nodeID, groupID)
	fmt.Printf("Message type: %v\n", msg.Type)
}

// ExampleCodec_ValidateAuth 展示如何验证认证
func ExampleCodec_ValidateAuth() {
	codec := protocol.NewCodec(&protocol.Config{
		NodeID:    "node-1",
		AuthToken: "valid-token",
	})

	// 创建并编码消息
	msg := &pb.SyncMessage{MessageId: "test"}
	data, _ := codec.EncodeSyncMessage(msg)

	// 解码获取信封
	env, _, _ := codec.DecodeSyncMessage(data)

	// 验证认证令牌
	isValid := codec.ValidateAuth(env, func(token string) bool {
		return token == "valid-token"
	})

	if isValid {
		fmt.Println("Authentication valid")
	} else {
		fmt.Println("Authentication failed")
	}
	// Output: Authentication valid
}

// ExampleEnvelopeContext 展示如何使用信封上下文
func ExampleEnvelopeContext() {
	ctx := context.Background()

	// 创建编解码器并编码消息
	codec := protocol.NewCodec(&protocol.Config{
		NodeID:  "node-1",
		GroupID: "group-a",
	})

	msg := &pb.SyncMessage{MessageId: "test"}
	data, _ := codec.EncodeSyncMessage(msg)
	env, _, _ := codec.DecodeSyncMessage(data)

	// 创建信封上下文
	envCtx := protocol.NewEnvelopeContext(ctx, env)

	// 在上下文中传递信封信息
	fmt.Printf("Context contains envelope from node: %s\n", envCtx.Envelope.SourceNode)
	// Output: Context contains envelope from node: node-1
}

// ExampleCodec_GetPayloadType 展示如何获取消息类型
func ExampleCodec_GetPayloadType() {
	codec := protocol.NewCodec(&protocol.Config{NodeID: "node-1"})

	// 编码不同类型的消息
	syncMsg := &pb.SyncMessage{MessageId: "sync-1"}
	reqMsg := &pb.SyncRequest{RequestId: "req-1"}

	syncData, _ := codec.EncodeSyncMessage(syncMsg)
	reqData, _ := codec.EncodeSyncRequest(reqMsg)

	// 解码并获取类型
	env1, _ := codec.DecodeEnvelope(syncData)
	env2, _ := codec.DecodeEnvelope(reqData)

	fmt.Printf("Message 1 type: %s\n", codec.GetPayloadType(env1))
	fmt.Printf("Message 2 type: %s\n", codec.GetPayloadType(env2))

	// Output:
	// Message 1 type: SyncMessage
	// Message 2 type: SyncRequest
}
