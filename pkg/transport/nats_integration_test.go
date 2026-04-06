//go:build integration
// +build integration

package transport

import (
	"context"
	"os"
	"testing"
	"time"

	datasync "github.com/joy999/datasync/pkg"
)

// getNATSURL 从环境变量获取 NATS URL
func getNATSURL() string {
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = "nats://localhost:4222"
	}
	return url
}

// TestNATS_Connection 测试 NATS 连接
func TestNATS_Connection(t *testing.T) {
	config := &NATSConfig{
		Servers:           []string{getNATSURL()},
		ReconnectWait:     time.Second,
		MaxReconnects:     5,
		ConnectionTimeout: 5 * time.Second,
	}

	nats, err := NewNATS(config)
	if err != nil {
		t.Fatalf("Failed to create NATS transport: %v", err)
	}
	defer nats.Stop()

	ctx := context.Background()
	if err := nats.Start(ctx); err != nil {
		t.Fatalf("Failed to start NATS transport: %v", err)
	}
}

// TestNATS_SendAndSubscribe 测试发送和订阅消息
func TestNATS_SendAndSubscribe(t *testing.T) {
	config := &NATSConfig{
		Servers:           []string{getNATSURL()},
		ReconnectWait:     time.Second,
		MaxReconnects:     5,
		ConnectionTimeout: 5 * time.Second,
	}

	nats, err := NewNATS(config)
	if err != nil {
		t.Fatalf("Failed to create NATS transport: %v", err)
	}
	defer nats.Stop()

	ctx := context.Background()
	if err := nats.Start(ctx); err != nil {
		t.Fatalf("Failed to start NATS transport: %v", err)
	}

	// 测试消息
	testGroup := datasync.GroupID("test-group")
	testMessage := []byte("Hello, NATS!")
	received := make(chan []byte, 1)

	// 订阅消息
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = nats.Subscribe(ctx, testGroup, func(data []byte) {
		received <- data
	})
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	// 发送消息
	err = nats.SendMessage(ctx, testGroup, testMessage)
	if err != nil {
		t.Fatalf("Failed to send message: %v", err)
	}

	// 等待接收消息
	select {
	case data := <-received:
		if string(data) != string(testMessage) {
			t.Errorf("Received message mismatch: expected %s, got %s", testMessage, data)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message")
	}
}

// TestNATS_MultipleMessages 测试发送多个消息
func TestNATS_MultipleMessages(t *testing.T) {
	config := &NATSConfig{
		Servers:           []string{getNATSURL()},
		ReconnectWait:     time.Second,
		MaxReconnects:     5,
		ConnectionTimeout: 5 * time.Second,
	}

	nats, err := NewNATS(config)
	if err != nil {
		t.Fatalf("Failed to create NATS transport: %v", err)
	}
	defer nats.Stop()

	ctx := context.Background()
	if err := nats.Start(ctx); err != nil {
		t.Fatalf("Failed to start NATS transport: %v", err)
	}

	testGroup := datasync.GroupID("test-group-multi")
	received := make(chan []byte, 10)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_ = nats.Subscribe(ctx, testGroup, func(data []byte) {
		received <- data
	})

	// 发送多条消息
	messages := [][]byte{
		[]byte("Message 1"),
		[]byte("Message 2"),
		[]byte("Message 3"),
	}

	for _, msg := range messages {
		err = nats.SendMessage(ctx, testGroup, msg)
		if err != nil {
			t.Fatalf("Failed to send message: %v", err)
		}
	}

	// 验证所有消息都收到
	for i := 0; i < len(messages); i++ {
		select {
		case data := <-received:
			found := false
			for _, expected := range messages {
				if string(data) == string(expected) {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Received unexpected message: %s", data)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("Timeout waiting for message %d", i+1)
		}
	}
}

// TestNATS_LargeMessage 测试大消息传输
func TestNATS_LargeMessage(t *testing.T) {
	config := &NATSConfig{
		Servers:           []string{getNATSURL()},
		ReconnectWait:     time.Second,
		MaxReconnects:     5,
		ConnectionTimeout: 5 * time.Second,
	}

	nats, err := NewNATS(config)
	if err != nil {
		t.Fatalf("Failed to create NATS transport: %v", err)
	}
	defer nats.Stop()

	ctx := context.Background()
	if err := nats.Start(ctx); err != nil {
		t.Fatalf("Failed to start NATS transport: %v", err)
	}

	// 1MB 的消息
	largeMessage := make([]byte, 1024*1024)
	for i := range largeMessage {
		largeMessage[i] = byte(i % 256)
	}

	testGroup := datasync.GroupID("test-group-large")
	received := make(chan []byte, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_ = nats.Subscribe(ctx, testGroup, func(data []byte) {
		received <- data
	})

	err = nats.SendMessage(ctx, testGroup, largeMessage)
	if err != nil {
		t.Fatalf("Failed to send large message: %v", err)
	}

	select {
	case data := <-received:
		if len(data) != len(largeMessage) {
			t.Errorf("Received message size mismatch: expected %d, got %d", len(largeMessage), len(data))
		}
		// 验证内容
		for i := range largeMessage {
			if data[i] != largeMessage[i] {
				t.Errorf("Data mismatch at index %d", i)
				break
			}
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for large message")
	}
}
