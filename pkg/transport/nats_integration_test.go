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

// TestNATS_MultipleSubscribers 测试多个订阅者
func TestNATS_MultipleSubscribers(t *testing.T) {
	config := &NATSConfig{
		Servers:           []string{getNATSURL()},
		ReconnectWait:     time.Second,
		MaxReconnects:     5,
		ConnectionTimeout: 5 * time.Second,
	}

	nats1, err := NewNATS(config)
	if err != nil {
		t.Fatalf("Failed to create NATS transport 1: %v", err)
	}
	defer nats1.Stop()

	nats2, err := NewNATS(config)
	if err != nil {
		t.Fatalf("Failed to create NATS transport 2: %v", err)
	}
	defer nats2.Stop()

	ctx := context.Background()
	if err := nats1.Start(ctx); err != nil {
		t.Fatalf("Failed to start NATS transport 1: %v", err)
	}
	if err := nats2.Start(ctx); err != nil {
		t.Fatalf("Failed to start NATS transport 2: %v", err)
	}

	testGroup := datasync.GroupID("test-group-multi")
	testMessage := []byte("Broadcast message")

	received1 := make(chan []byte, 1)
	received2 := make(chan []byte, 1)

	// 两个订阅者
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_ = nats1.Subscribe(ctx, testGroup, func(data []byte) {
		received1 <- data
	})

	_ = nats2.Subscribe(ctx, testGroup, func(data []byte) {
		received2 <- data
	})

	// 发送消息
	err = nats1.SendMessage(ctx, testGroup, testMessage)
	if err != nil {
		t.Fatalf("Failed to send message: %v", err)
	}

	// 验证两个订阅者都收到消息
	select {
	case data := <-received1:
		if string(data) != string(testMessage) {
			t.Errorf("Subscriber 1 received wrong message: %s", data)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for subscriber 1")
	}

	select {
	case data := <-received2:
		if string(data) != string(testMessage) {
			t.Errorf("Subscriber 2 received wrong message: %s", data)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for subscriber 2")
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
