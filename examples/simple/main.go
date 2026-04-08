package main

import (
	"context"
	"fmt"
	"time"

	"github.com/joy999/datasync/drivers/memory"
	datasync "github.com/joy999/datasync/pkg"
	"github.com/joy999/datasync/pkg/node"
	"github.com/joy999/datasync/pkg/transport"
)

func main() {
	// 创建NATS传输层配置
	natsConfig := &transport.NATSConfig{
		Servers:           []string{"nats://localhost:4222"},
		ReconnectWait:     time.Second,
		MaxReconnects:     60,
		ConnectionTimeout: 10 * time.Second,
		EnableJetStream:   true,
		StreamName:        "RAFTSYNC",
	}

	// 创建NATS传输层
	natsTransport, err := transport.NewNATS(natsConfig)
	if err != nil {
		panic(fmt.Errorf("failed to create NATS transport: %w", err))
	}

	// 创建节点配置
	config := &node.Config{
		NodeID:    datasync.NodeID("node1"),
		GroupID:   datasync.GroupID("group-a"),
		Address:   "localhost:8080",
		DataDir:   "./data/node1",
		Transport: natsTransport,
	}

	// 创建节点
	n, err := node.New(config)
	if err != nil {
		panic(fmt.Errorf("failed to create node: %w", err))
	}

	// 启动节点
	ctx := context.Background()
	if err := n.Start(ctx); err != nil {
		panic(fmt.Errorf("failed to start node: %w", err))
	}
	defer func() {
		if err := n.Stop(); err != nil {
			panic(fmt.Errorf("failed to stop node: %w", err))
		}
	}()

	// 注册存储驱动
	driver := memory.NewDriver("users")
	if err := n.RegisterDriver("users", driver); err != nil {
		panic(fmt.Errorf("failed to register driver: %w", err))
	}

	// 插入数据
	record := &datasync.DataRecord{
		ID:        datasync.DataID("user-1"),
		Type:      "users",
		Version:   1,
		Timestamp: time.Now(),
		Payload:   []byte(`{"name":"Alice","email":"alice@example.com"}`),
		Metadata: map[string]string{
			"created_by": "example",
		},
	}

	if err := driver.SaveRecord(record); err != nil {
		panic(fmt.Errorf("failed to save record: %w", err))
	}

	fmt.Println("Node started and data saved")
	fmt.Println("Node ID:", n.GetNodeID())
	fmt.Println("Group ID:", n.GetGroupID())
	fmt.Println("Is Leader:", n.IsLeader())

	// 保持运行
	select {}
}
