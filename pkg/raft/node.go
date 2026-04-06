package raft

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	datasync "github.com/joy999/datasync/pkg"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// Config Raft配置
type Config struct {
	NodeID    datasync.NodeID      // 节点ID
	GroupID   datasync.GroupID     // 组ID
	DataDir   string               // 数据目录
	Transport datasync.Transport   // 传输层
	HeartbeatTick    int           // 心跳间隔（tick）
	ElectionTick     int           // 选举间隔（tick）
	SnapshotInterval time.Duration // 快照间隔
	MaxSnapshotFiles int           // 最大快照文件数
}

// Node Raft节点实现
type Node struct {
	config        *Config
	raftNode      raft.Node
	raftStorage   *raft.MemoryStorage
	peerIDs       []uint64
	dataDir       string
	transport     datasync.Transport
	isLeader      bool
	raftConfig    *raft.Config
	ctx           context.Context
	cancel        context.CancelFunc
}

// NewNode 创建新的Raft节点
func NewNode(config *Config) (*Node, error) {
	// 初始化上下文
	ctx, cancel := context.WithCancel(context.Background())

	// 确保数据目录存在
	dataDir := filepath.Join(config.DataDir, string(config.GroupID))
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	// 初始化存储
	storage := raft.NewMemoryStorage()

	// 配置Raft
	raftConfig := &raft.Config{
		ID:              uint64(config.NodeID[0]), // 简单处理，实际应该有更复杂的ID映射
		ElectionTick:    config.ElectionTick,
		HeartbeatTick:   config.HeartbeatTick,
		Storage:         storage,
		MaxSizePerMsg:   1024 * 1024, // 1MB
		MaxInflightMsgs: 256,
	}

	// 创建Raft节点
	peers := []raft.Peer{{ID: uint64(config.NodeID[0])}}
	node := raft.StartNode(raftConfig, peers)

	// 初始化Raft节点
	rn := &Node{
		config:      config,
		raftNode:    node,
		raftStorage: storage,
		peerIDs:     []uint64{uint64(config.NodeID[0])},
		dataDir:     dataDir,
		transport:   config.Transport,
		isLeader:    false,
		raftConfig:  raftConfig,
		ctx:         ctx,
		cancel:      cancel,
	}

	// 启动消息处理协程
	go rn.processMessages()

	return rn, nil
}

// Start 启动Raft节点
func (n *Node) Start(ctx context.Context) error {
	// Raft节点已经在NewNode中启动
	return nil
}

// Stop 停止Raft节点
func (n *Node) Stop() error {
	// 取消上下文
	n.cancel()

	// 停止Raft节点
	n.raftNode.Stop()

	return nil
}

// IsLeader 检查是否为Leader
func (n *Node) IsLeader() bool {
	return n.isLeader
}

// Apply 应用数据到Raft日志
func (n *Node) Apply(record *datasync.DataRecord) error {
	// 构建日志条目
	data, err := encodeRecord(record)
	if err != nil {
		return fmt.Errorf("failed to encode record: %w", err)
	}

	// 应用到Raft
	n.raftNode.Propose(n.ctx, data)

	return nil
}

// processMessages 处理Raft消息
func (n *Node) processMessages() {
	for {
		select {
		case <-n.ctx.Done():
			return

		case msg := <-n.raftNode.Ready():
			// 处理状态更新
			if msg.SoftState != nil {
				n.isLeader = msg.SoftState.Lead == n.raftConfig.ID
			}

			// 处理日志
			if len(msg.Entries) > 0 {
				n.raftStorage.Append(msg.Entries)
			}

			// 处理快照
			if msg.Snapshot.Metadata.Term != 0 || msg.Snapshot.Metadata.Index != 0 {
				n.raftStorage.ApplySnapshot(msg.Snapshot)
			}

			// 处理消息发送
			if len(msg.Messages) > 0 {
				n.sendMessages(msg.Messages)
			}

			// 推进状态机
			for _, entry := range msg.CommittedEntries {
				if entry.Type == raftpb.EntryNormal && len(entry.Data) > 0 {
					// 应用到状态机
					n.applyEntry(entry)
				}
			}

			// 通知Raft节点已处理完毕
			n.raftNode.Advance()
		}
	}
}

// sendMessages 发送Raft消息
func (n *Node) sendMessages(msgs []raftpb.Message) {
	for range msgs {
		// 这里可以实现消息发送逻辑
		// 例如，通过传输层发送到其他节点
	}
}

// applyEntry 应用日志条目到状态机
func (n *Node) applyEntry(entry raftpb.Entry) {
	// 解码记录
	_, err := decodeRecord(entry.Data)
	if err != nil {
		return
	}

	// 这里可以实现状态机应用逻辑
	// 例如，将数据存储到存储驱动
}

// encodeRecord 编码数据记录
func encodeRecord(record *datasync.DataRecord) ([]byte, error) {
	// 这里可以实现编码逻辑
	// 例如，使用JSON或Protocol Buffers
	return []byte(""), nil
}

// decodeRecord 解码数据记录
func decodeRecord(data []byte) (*datasync.DataRecord, error) {
	// 这里可以实现解码逻辑
	// 例如，使用JSON或Protocol Buffers
	return &datasync.DataRecord{}, nil
}
