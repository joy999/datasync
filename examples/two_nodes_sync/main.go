// Package main 展示两个节点（NodeA 和 NodeB）在同一个 Group 中进行数据同步的示例
//
// 这个示例演示了以下场景：
// 1. NodeA 和 NodeB 加入同一个 Group
// 2. NodeA 创建/更新数据记录
// 3. 数据自动同步到 NodeB
// 4. NodeB 读取同步过来的数据
//
// 运行方式：
//   终端1: go run main.go -node=node-a -port=8081
//   终端2: go run main.go -node=node-b -port=8082 -peers="localhost:8081"
//
// 然后在终端1输入数据，观察终端2自动同步
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	datasync "github.com/joy999/datasync/pkg"
	"github.com/joy999/datasync/pkg/group"
)

func main() {
	var (
		nodeID   = flag.String("node", "", "当前节点ID (node-a 或 node-b)")
		port     = flag.String("port", "8080", "节点监听端口")
		peers    = flag.String("peers", "", "其他节点地址，逗号分隔 (如: localhost:8081,localhost:8082)")
		groupID  = flag.String("group", "demo-group", "Group ID")
	)
	flag.Parse()

	if *nodeID == "" {
		fmt.Println("错误: 必须指定 -node 参数 (node-a 或 node-b)")
		fmt.Println("\n示例:")
		fmt.Println("  终端1: go run main.go -node=node-a -port=8081")
		fmt.Println("  终端2: go run main.go -node=node-b -port=8082 -peers=localhost:8081")
		os.Exit(1)
	}

	fmt.Printf("🚀 启动节点: %s\n", *nodeID)
	fmt.Printf("📡 监听端口: %s\n", *port)
	fmt.Printf("👥 Group ID: %s\n", *groupID)

	// 创建 Group 配置
	config := &group.Config{
		GroupID:     datasync.GroupID(*groupID),
		NodeID:      datasync.NodeID(*nodeID),
		BindAddress: ":" + *port,
		Peers:       parsePeers(*peers),
	}

	// 创建 Group 实例
	g, err := group.NewGroup(config)
	if err != nil {
		fmt.Printf("❌ 创建 Group 失败: %v\n", err)
		os.Exit(1)
	}

	// 启动 Group
	ctx := context.Background()
	if err := g.Start(ctx); err != nil {
		fmt.Printf("❌ 启动 Group 失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("✅ 节点 %s 已启动并加入 Group %s\n", *nodeID, *groupID)
	fmt.Println()

	// 设置数据变更回调
	g.SetChangeCallback(func(record *datasync.DataRecord) {
		fmt.Printf("\n📥 [%s] 收到同步数据:\n", time.Now().Format("15:04:05"))
		fmt.Printf("   来源节点: %s\n", record.SourceNode)
		fmt.Printf("   数据类型: %s\n", record.Type)
		fmt.Printf("   记录ID: %s\n", record.ID)
		fmt.Printf("   版本: %d\n", record.Version)
		fmt.Printf("   内容: %s\n", string(record.Payload))
		fmt.Println()
		fmt.Print("> ")
	})

	// 如果是 node-a，提供交互式输入
	if *nodeID == "node-a" {
		runInteractiveMode(g)
	} else {
		// node-b 只接收数据
		fmt.Println("📝 NodeB 正在等待同步数据...")
		fmt.Println("   在 NodeA 输入数据，这里会自动显示")
		fmt.Println("   按 Ctrl+C 退出")
		select {}
	}
}

// parsePeers 解析 peers 字符串
func parsePeers(peersStr string) []string {
	if peersStr == "" {
		return nil
	}
	var peers []string
	for _, p := range strings.Split(peersStr, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			peers = append(peers, p)
		}
	}
	return peers
}

// runInteractiveMode 运行交互模式（用于 node-a）
func runInteractiveMode(g *group.Group) {
	fmt.Println("📝 交互模式 - 输入数据发送到 Group")
	fmt.Println("   命令:")
	fmt.Println("     create <内容>  - 创建新记录")
	fmt.Println("     update <id> <内容> - 更新记录")
	fmt.Println("     list           - 列出所有记录")
	fmt.Println("     exit           - 退出")
	fmt.Println()

	reader := bufio.NewReader(os.Stdin)
	recordCounter := 0

	for {
		fmt.Print("> ")
		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("读取输入失败: %v\n", err)
			continue
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, " ", 2)
		cmd := parts[0]

		switch cmd {
		case "create":
			if len(parts) < 2 {
				fmt.Println("用法: create <内容>")
				continue
			}
			recordCounter++
			recordID := fmt.Sprintf("record-%d", recordCounter)
			
			record := &datasync.DataRecord{
				ID:        datasync.DataID(recordID),
				Type:      "message",
				Version:   1,
				Timestamp: time.Now().UTC(),
				Payload:   []byte(parts[1]),
				SourceNode: string(g.GetNodeID()),
			}

			if err := g.ApplyRecord(context.Background(), record); err != nil {
				fmt.Printf("❌ 创建记录失败: %v\n", err)
			} else {
				fmt.Printf("✅ 创建记录 %s: %s\n", recordID, parts[1])
			}

		case "update":
			updateParts := strings.SplitN(parts[1], " ", 2)
			if len(updateParts) < 2 {
				fmt.Println("用法: update <id> <内容>")
				continue
			}
			recordID := updateParts[0]
			
			record := &datasync.DataRecord{
				ID:        datasync.DataID(recordID),
				Type:      "message",
				Version:   2, // 更新版本号
				Timestamp: time.Now().UTC(),
				Payload:   []byte(updateParts[1]),
				SourceNode: string(g.GetNodeID()),
			}

			if err := g.ApplyRecord(context.Background(), record); err != nil {
				fmt.Printf("❌ 更新记录失败: %v\n", err)
			} else {
				fmt.Printf("✅ 更新记录 %s: %s\n", recordID, updateParts[1])
			}

		case "list":
			records, err := g.GetRecords(context.Background(), "message", "", 100)
			if err != nil {
				fmt.Printf("❌ 获取记录失败: %v\n", err)
				continue
			}
			
			fmt.Printf("\n📋 当前共有 %d 条记录:\n", len(records))
			for _, r := range records {
				fmt.Printf("   %s (v%d): %s\n", r.ID, r.Version, string(r.Payload))
			}
			fmt.Println()

		case "exit", "quit":
			fmt.Println("👋 再见!")
			os.Exit(0)

		default:
			fmt.Printf("未知命令: %s\n", cmd)
			fmt.Println("可用命令: create, update, list, exit")
		}
	}
}
