package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/joy999/datasync/drivers/postgres"
	datasync "github.com/joy999/datasync/pkg"
)

func main() {
	// 配置 PostgreSQL 连接
	config := postgres.PostgreSQLConfig{
		Host:     "localhost",
		Port:     5432,
		Database: "datasync_example",
		User:     "postgres",
		Password: "postgres",
		SSLMode:  "disable",
		NodeID:   "example-node-1",
		GroupID:  "example-group-1",
	}

	// 创建驱动实例
	driver := postgres.NewDriver(config)

	// 初始化上下文
	ctx := context.Background()

	// 初始化驱动（创建必要的表结构）
	if err := driver.Initialize(ctx, nil); err != nil {
		log.Fatalf("Failed to initialize driver: %v", err)
	}
	defer driver.Close()

	fmt.Println("✓ PostgreSQL driver initialized successfully")

	// 示例 1: 基本 CRUD 操作
	fmt.Println("\n=== 示例 1: 基本 CRUD 操作 ===")
	demoBasicCRUD(ctx, driver)

	// 示例 2: CDC (变更数据捕获)
	fmt.Println("\n=== 示例 2: CDC 变更数据捕获 ===")
	demoCDC(ctx, driver)

	// 示例 3: 断点续传
	fmt.Println("\n=== 示例 3: 断点续传机制 ===")
	demoCheckpoint(ctx, driver)

	// 示例 4: 同步状态查询
	fmt.Println("\n=== 示例 4: 同步状态查询 ===")
	demoSyncStatus(ctx, driver)
}

// demoBasicCRUD 演示基本 CRUD 操作
func demoBasicCRUD(ctx context.Context, driver *postgres.Driver) {
	dataType := "users"

	// 创建记录
	record := &datasync.DataRecord{
		ID:        datasync.DataID("user-001"),
		Type:      dataType,
		Version:   1,
		Timestamp: time.Now().UTC(),
		Payload:   []byte(`{"name":"张三","email":"zhangsan@example.com","age":28}`),
	}

	if err := driver.ApplyRecord(ctx, record); err != nil {
		log.Printf("Failed to create record: %v", err)
		return
	}
	fmt.Println("✓ Created record: user-001")

	// 读取记录
	retrieved, err := driver.GetRecord(ctx, dataType, record.ID)
	if err != nil {
		log.Printf("Failed to get record: %v", err)
		return
	}
	if retrieved != nil {
		fmt.Printf("✓ Retrieved record: ID=%s, Version=%d, Payload=%s\n",
			retrieved.ID, retrieved.Version, string(retrieved.Payload))
	}

	// 更新记录
	record.Version = 2
	record.Payload = []byte(`{"name":"张三","email":"zhangsan@example.com","age":29}`)
	if err := driver.ApplyRecord(ctx, record); err != nil {
		log.Printf("Failed to update record: %v", err)
		return
	}
	fmt.Println("✓ Updated record: user-001 (version 2)")

	// 分页查询
	records, nextCursor, err := driver.GetRecords(ctx, dataType, "", 10)
	if err != nil {
		log.Printf("Failed to get records: %v", err)
		return
	}
	fmt.Printf("✓ Queried %d records, next cursor: %s\n", len(records), nextCursor)

	// 获取数据类型列表
	dataTypes, err := driver.GetDataTypes(ctx)
	if err != nil {
		log.Printf("Failed to get data types: %v", err)
		return
	}
	fmt.Printf("✓ Available data types: %v\n", dataTypes)
}

// demoCDC 演示 CDC 功能
func demoCDC(ctx context.Context, driver *postgres.Driver) {
	dataType := "orders"
	startTime := time.Now().UTC()

	// 创建订单记录
	orders := []*datasync.DataRecord{
		{
			ID:        datasync.DataID("order-001"),
			Type:      dataType,
			Version:   1,
			Timestamp: time.Now().UTC(),
			Payload:   []byte(`{"product":"iPhone","amount":5999}`),
		},
		{
			ID:        datasync.DataID("order-002"),
			Type:      dataType,
			Version:   1,
			Timestamp: time.Now().UTC(),
			Payload:   []byte(`{"product":"MacBook","amount":12999}`),
		},
	}

	for _, order := range orders {
		if err := driver.ApplyRecord(ctx, order); err != nil {
			log.Printf("Failed to create order: %v", err)
			return
		}
	}
	fmt.Println("✓ Created 2 orders")

	// 更新订单
	orders[0].Version = 2
	orders[0].Payload = []byte(`{"product":"iPhone 15","amount":6999}`)
	if err := driver.ApplyRecord(ctx, orders[0]); err != nil {
		log.Printf("Failed to update order: %v", err)
		return
	}
	fmt.Println("✓ Updated order-001")

	// 查询变更
	changes, err := driver.GetChanges(ctx, dataType, startTime, 10)
	if err != nil {
		log.Printf("Failed to get changes: %v", err)
		return
	}

	fmt.Printf("✓ Found %d changes since %v:\n", len(changes), startTime)
	for _, change := range changes {
		changeType := "UNKNOWN"
		if change.Metadata != nil {
			changeType = change.Metadata["change_type"]
		}
		fmt.Printf("  - ID: %s, Type: %s, Version: %d, ChangeType: %s\n",
			change.ID, change.Type, change.Version, changeType)
	}

	// 获取最新 CDC 序列号
	latestSeq, err := driver.GetLatestCDCSequence(ctx)
	if err != nil {
		log.Printf("Failed to get latest CDC sequence: %v", err)
		return
	}
	fmt.Printf("✓ Latest CDC sequence: %d\n", latestSeq)
}

// demoCheckpoint 演示断点续传功能
func demoCheckpoint(ctx context.Context, driver *postgres.Driver) {
	dataType := "products"

	// 初始断点
	seq, err := driver.GetCheckpoint(ctx, dataType)
	if err != nil {
		log.Printf("Failed to get checkpoint: %v", err)
		return
	}
	fmt.Printf("✓ Initial checkpoint for '%s': %d\n", dataType, seq)

	// 创建产品记录
	for i := 1; i <= 5; i++ {
		record := &datasync.DataRecord{
			ID:        datasync.DataID(fmt.Sprintf("product-%03d", i)),
			Type:      dataType,
			Version:   1,
			Timestamp: time.Now().UTC(),
			Payload:   []byte(fmt.Sprintf(`{"name":"Product %d","price":%d}`, i, i*100)),
		}
		if err := driver.ApplyRecord(ctx, record); err != nil {
			log.Printf("Failed to create product: %v", err)
			return
		}
	}
	fmt.Println("✓ Created 5 products")

	// 获取最新 CDC 序列号
	latestSeq, err := driver.GetLatestCDCSequence(ctx)
	if err != nil {
		log.Printf("Failed to get latest CDC sequence: %v", err)
		return
	}

	// 设置断点（模拟同步完成）
	if err := driver.SetCheckpoint(ctx, dataType, latestSeq); err != nil {
		log.Printf("Failed to set checkpoint: %v", err)
		return
	}
	fmt.Printf("✓ Set checkpoint to: %d\n", latestSeq)

	// 验证断点
	newSeq, err := driver.GetCheckpoint(ctx, dataType)
	if err != nil {
		log.Printf("Failed to get checkpoint: %v", err)
		return
	}
	fmt.Printf("✓ Verified checkpoint: %d\n", newSeq)

	// 查询从断点开始的 CDC 记录
	cdcRecords, err := driver.GetCDCRecordsBySequence(ctx, newSeq, 10)
	if err != nil {
		log.Printf("Failed to get CDC records by sequence: %v", err)
		return
	}
	fmt.Printf("✓ CDC records after checkpoint %d: %d\n", newSeq, len(cdcRecords))
}

// demoSyncStatus 演示同步状态查询
func demoSyncStatus(ctx context.Context, driver *postgres.Driver) {
	dataType := "inventory"

	// 创建库存记录
	for i := 1; i <= 3; i++ {
		record := &datasync.DataRecord{
			ID:        datasync.DataID(fmt.Sprintf("item-%03d", i)),
			Type:      dataType,
			Version:   1,
			Timestamp: time.Now().UTC(),
			Payload:   []byte(fmt.Sprintf(`{"sku":"SKU%d","quantity":%d}`, i, i*10)),
		}
		if err := driver.ApplyRecord(ctx, record); err != nil {
			log.Printf("Failed to create inventory: %v", err)
			return
		}
	}
	fmt.Println("✓ Created 3 inventory items")

	// 获取同步状态
	status, err := driver.GetSyncStatus(ctx, dataType)
	if err != nil {
		log.Printf("Failed to get sync status: %v", err)
		return
	}

	fmt.Printf("✓ Sync status for '%s':\n", dataType)
	fmt.Printf("  - Last Sequence: %d\n", status.LastSequence)
	fmt.Printf("  - Latest Sequence: %d\n", status.LatestSequence)
	fmt.Printf("  - Pending Count: %d\n", status.PendingCount)
	fmt.Printf("  - Last Updated: %v\n", status.LastUpdatedAt)

	// 设置断点后再次查询
	if err := driver.SetCheckpoint(ctx, dataType, status.LatestSequence); err != nil {
		log.Printf("Failed to set checkpoint: %v", err)
		return
	}

	status, err = driver.GetSyncStatus(ctx, dataType)
	if err != nil {
		log.Printf("Failed to get sync status after checkpoint: %v", err)
		return
	}

	fmt.Printf("✓ Sync status after checkpoint:\n")
	fmt.Printf("  - Last Sequence: %d\n", status.LastSequence)
	fmt.Printf("  - Latest Sequence: %d\n", status.LatestSequence)
	fmt.Printf("  - Pending Count: %d (should be 0)\n", status.PendingCount)
}
