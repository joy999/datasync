package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	datasync "github.com/joy999/datasync/pkg"
)

// getTestConfig 获取测试配置
func getTestConfig() PostgreSQLConfig {
	// 从环境变量读取配置，如果没有则使用默认值
	config := PostgreSQLConfig{
		Host:     getEnv("TEST_PG_HOST", "localhost"),
		Port:     getEnvInt("TEST_PG_PORT", 5432),
		Database: getEnv("TEST_PG_DB", "datasync_test"),
		User:     getEnv("TEST_PG_USER", "postgres"),
		Password: getEnv("TEST_PG_PASSWORD", "postgres"),
		SSLMode:  getEnv("TEST_PG_SSLMODE", "disable"),
		NodeID:   "test-node-1",
		GroupID:  "test-group-1",
	}
	return config
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	// 简化处理，直接使用默认值
	return defaultValue
}

// skipIfNoPostgres 如果没有 PostgreSQL 则跳过测试
func skipIfNoPostgres(t *testing.T) {
	if os.Getenv("TEST_PG_HOST") == "" && os.Getenv("SKIP_PG_TEST") != "" {
		t.Skip("Skipping PostgreSQL tests (set TEST_PG_HOST to enable)")
	}
}

// TestDriver_Initialize 测试驱动初始化
func TestDriver_Initialize(t *testing.T) {
	skipIfNoPostgres(t)

	config := getTestConfig()
	driver := NewDriver(config)

	ctx := context.Background()
	err := driver.Initialize(ctx, nil)
	if err != nil {
		t.Fatalf("Failed to initialize driver: %v", err)
	}
	defer driver.Close()

	// 验证 DriverID
	if driver.GetDriverID() != DriverIDValue {
		t.Errorf("Expected DriverID %d, got %d", DriverIDValue, driver.GetDriverID())
	}
}

// TestDriver_BasicCRUD 测试基础 CRUD 操作
func TestDriver_BasicCRUD(t *testing.T) {
	skipIfNoPostgres(t)

	config := getTestConfig()
	driver := NewDriver(config)

	ctx := context.Background()
	if err := driver.Initialize(ctx, nil); err != nil {
		t.Fatalf("Failed to initialize driver: %v", err)
	}
	defer driver.Close()

	dataType := "test_users"

	// 测试 ApplyRecord（创建）
	record := &datasync.DataRecord{
		ID:        datasync.DataID("user-001"),
		Type:      dataType,
		Version:   1,
		Timestamp: time.Now().UTC(),
		Payload:   []byte(`{"name":"John","age":30}`),
	}

	if err := driver.ApplyRecord(ctx, record); err != nil {
		t.Fatalf("Failed to apply record: %v", err)
	}

	// 测试 GetRecord
	retrieved, err := driver.GetRecord(ctx, dataType, record.ID)
	if err != nil {
		t.Fatalf("Failed to get record: %v", err)
	}
	if retrieved == nil {
		t.Fatal("Retrieved record is nil")
	}
	if string(retrieved.ID) != string(record.ID) {
		t.Errorf("Expected ID %s, got %s", record.ID, retrieved.ID)
	}

	// 测试 ApplyRecord（更新）
	record.Version = 2
	record.Payload = []byte(`{"name":"John","age":31}`)
	if err := driver.ApplyRecord(ctx, record); err != nil {
		t.Fatalf("Failed to update record: %v", err)
	}

	// 验证更新
	updated, err := driver.GetRecord(ctx, dataType, record.ID)
	if err != nil {
		t.Fatalf("Failed to get updated record: %v", err)
	}
	if updated.Version != 2 {
		t.Errorf("Expected version 2, got %d", updated.Version)
	}

	// 测试 DeleteRecord
	if err := driver.DeleteRecord(ctx, dataType, record.ID); err != nil {
		t.Fatalf("Failed to delete record: %v", err)
	}

	// 验证删除
	deleted, err := driver.GetRecord(ctx, dataType, record.ID)
	if err != nil {
		t.Fatalf("Failed to get record after delete: %v", err)
	}
	if deleted != nil {
		t.Error("Record should be nil after deletion")
	}
}

// TestDriver_GetRecords 测试分页获取记录
func TestDriver_GetRecords(t *testing.T) {
	skipIfNoPostgres(t)

	config := getTestConfig()
	driver := NewDriver(config)

	ctx := context.Background()
	if err := driver.Initialize(ctx, nil); err != nil {
		t.Fatalf("Failed to initialize driver: %v", err)
	}
	defer driver.Close()

	dataType := "test_products"

	// 创建多个记录
	for i := 0; i < 10; i++ {
		record := &datasync.DataRecord{
			ID:        datasync.DataID(fmt.Sprintf("product-%03d", i)),
			Type:      dataType,
			Version:   1,
			Timestamp: time.Now().UTC(),
			Payload:   []byte(fmt.Sprintf(`{"name":"Product %d","price":%d}`, i, i*10)),
		}
		if err := driver.ApplyRecord(ctx, record); err != nil {
			t.Fatalf("Failed to apply record: %v", err)
		}
	}

	// 测试分页获取
	records, nextCursor, err := driver.GetRecords(ctx, dataType, "", 5)
	if err != nil {
		t.Fatalf("Failed to get records: %v", err)
	}
	if len(records) != 5 {
		t.Errorf("Expected 5 records, got %d", len(records))
	}
	if nextCursor == "" {
		t.Error("Expected non-empty nextCursor")
	}

	// 获取下一页
	records2, nextCursor2, err := driver.GetRecords(ctx, dataType, nextCursor, 5)
	if err != nil {
		t.Fatalf("Failed to get records page 2: %v", err)
	}
	if len(records2) != 5 {
		t.Errorf("Expected 5 records on page 2, got %d", len(records2))
	}
	if nextCursor2 != "" {
		t.Error("Expected empty nextCursor on last page")
	}
}

// TestDriver_CDC 测试 CDC 机制
func TestDriver_CDC(t *testing.T) {
	skipIfNoPostgres(t)

	config := getTestConfig()
	driver := NewDriver(config)

	ctx := context.Background()
	if err := driver.Initialize(ctx, nil); err != nil {
		t.Fatalf("Failed to initialize driver: %v", err)
	}
	defer driver.Close()

	dataType := "test_orders"
	startTime := time.Now().UTC()

	// 创建记录
	record1 := &datasync.DataRecord{
		ID:        datasync.DataID("order-001"),
		Type:      dataType,
		Version:   1,
		Timestamp: time.Now().UTC(),
		Payload:   []byte(`{"amount":100}`),
	}
	if err := driver.ApplyRecord(ctx, record1); err != nil {
		t.Fatalf("Failed to apply record: %v", err)
	}

	// 更新记录
	record1.Version = 2
	record1.Payload = []byte(`{"amount":150}`)
	if err := driver.ApplyRecord(ctx, record1); err != nil {
		t.Fatalf("Failed to update record: %v", err)
	}

	// 获取变更
	changes, err := driver.GetChanges(ctx, dataType, startTime, 10)
	if err != nil {
		t.Fatalf("Failed to get changes: %v", err)
	}
	if len(changes) < 2 {
		t.Errorf("Expected at least 2 changes, got %d", len(changes))
	}

	// 验证 CDC 记录包含正确的元数据
	for _, change := range changes {
		if change.Metadata == nil {
			t.Error("Expected Metadata to be set")
			continue
		}
		if change.Metadata["change_type"] == "" {
			t.Error("Expected change_type in metadata")
		}
	}
}

// TestDriver_Checkpoint 测试断点续传
func TestDriver_Checkpoint(t *testing.T) {
	skipIfNoPostgres(t)

	config := getTestConfig()
	driver := NewDriver(config)

	ctx := context.Background()
	if err := driver.Initialize(ctx, nil); err != nil {
		t.Fatalf("Failed to initialize driver: %v", err)
	}
	defer driver.Close()

	dataType := "test_items"

	// 初始断点应为 0
	seq, err := driver.GetCheckpoint(ctx, dataType)
	if err != nil {
		t.Fatalf("Failed to get checkpoint: %v", err)
	}
	if seq != 0 {
		t.Errorf("Expected initial checkpoint 0, got %d", seq)
	}

	// 设置断点
	if err := driver.SetCheckpoint(ctx, dataType, 100); err != nil {
		t.Fatalf("Failed to set checkpoint: %v", err)
	}

	// 验证断点
	seq, err = driver.GetCheckpoint(ctx, dataType)
	if err != nil {
		t.Fatalf("Failed to get checkpoint after set: %v", err)
	}
	if seq != 100 {
		t.Errorf("Expected checkpoint 100, got %d", seq)
	}

	// 更新断点
	if err := driver.SetCheckpoint(ctx, dataType, 200); err != nil {
		t.Fatalf("Failed to update checkpoint: %v", err)
	}

	seq, err = driver.GetCheckpoint(ctx, dataType)
	if err != nil {
		t.Fatalf("Failed to get checkpoint after update: %v", err)
	}
	if seq != 200 {
		t.Errorf("Expected checkpoint 200, got %d", seq)
	}

	// 重置断点
	if err := driver.ResetCheckpoint(ctx, dataType); err != nil {
		t.Fatalf("Failed to reset checkpoint: %v", err)
	}

	seq, err = driver.GetCheckpoint(ctx, dataType)
	if err != nil {
		t.Fatalf("Failed to get checkpoint after reset: %v", err)
	}
	if seq != 0 {
		t.Errorf("Expected checkpoint 0 after reset, got %d", seq)
	}
}

// TestDriver_MarshalUnmarshal 测试序列化
func TestDriver_MarshalUnmarshal(t *testing.T) {
	driver := NewDriver(getTestConfig())

	record := &datasync.DataRecord{
		ID:        datasync.DataID("test-001"),
		Type:      "test",
		Version:   1,
		Timestamp: time.Now().UTC(),
		Payload:   []byte(`{"key":"value"}`),
		Metadata:  map[string]string{"meta": "data"},
	}

	// 序列化
	data, err := driver.Marshal(record)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}
	if len(data) == 0 {
		t.Error("Marshaled data is empty")
	}

	// 反序列化
	unmarshaled, err := driver.Unmarshal(data)
	if err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	// 验证
	if unmarshaled.ID != record.ID {
		t.Errorf("Expected ID %s, got %s", record.ID, unmarshaled.ID)
	}
	if unmarshaled.Type != record.Type {
		t.Errorf("Expected Type %s, got %s", record.Type, unmarshaled.Type)
	}
	if unmarshaled.Version != record.Version {
		t.Errorf("Expected Version %d, got %d", record.Version, unmarshaled.Version)
	}
}

// TestDriver_GetDataTypes 测试获取数据类型
func TestDriver_GetDataTypes(t *testing.T) {
	skipIfNoPostgres(t)

	config := getTestConfig()
	driver := NewDriver(config)

	ctx := context.Background()
	if err := driver.Initialize(ctx, nil); err != nil {
		t.Fatalf("Failed to initialize driver: %v", err)
	}
	defer driver.Close()

	// 创建一些记录
	record := &datasync.DataRecord{
		ID:        datasync.DataID("type-test-001"),
		Type:      "test_type_a",
		Version:   1,
		Timestamp: time.Now().UTC(),
		Payload:   []byte(`{}`),
	}
	if err := driver.ApplyRecord(ctx, record); err != nil {
		t.Fatalf("Failed to apply record: %v", err)
	}

	// 获取数据类型
	types, err := driver.GetDataTypes(ctx)
	if err != nil {
		t.Fatalf("Failed to get data types: %v", err)
	}

	// 验证包含我们创建的类型
	found := false
	for _, t := range types {
		if t == "test_type_a" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected to find 'test_type_a' in data types, got %v", types)
	}
}

// TestDriver_GetLatestVersion 测试获取最新版本
func TestDriver_GetLatestVersion(t *testing.T) {
	skipIfNoPostgres(t)

	config := getTestConfig()
	driver := NewDriver(config)

	ctx := context.Background()
	if err := driver.Initialize(ctx, nil); err != nil {
		t.Fatalf("Failed to initialize driver: %v", err)
	}
	defer driver.Close()

	dataType := "test_versions"
	recordID := datasync.DataID("version-test-001")

	// 不存在的记录版本应为 0
	version, err := driver.GetLatestVersion(ctx, dataType, recordID)
	if err != nil {
		t.Fatalf("Failed to get latest version: %v", err)
	}
	if version != 0 {
		t.Errorf("Expected version 0 for non-existent record, got %d", version)
	}

	// 创建记录
	record := &datasync.DataRecord{
		ID:        recordID,
		Type:      dataType,
		Version:   5,
		Timestamp: time.Now().UTC(),
		Payload:   []byte(`{}`),
	}
	if err := driver.ApplyRecord(ctx, record); err != nil {
		t.Fatalf("Failed to apply record: %v", err)
	}

	// 验证版本
	version, err = driver.GetLatestVersion(ctx, dataType, recordID)
	if err != nil {
		t.Fatalf("Failed to get latest version: %v", err)
	}
	if version != 5 {
		t.Errorf("Expected version 5, got %d", version)
	}
}

// TestDriver_ChangeCallback 测试变更回调
func TestDriver_ChangeCallback(t *testing.T) {
	skipIfNoPostgres(t)

	config := getTestConfig()
	driver := NewDriver(config)

	ctx := context.Background()
	if err := driver.Initialize(ctx, nil); err != nil {
		t.Fatalf("Failed to initialize driver: %v", err)
	}
	defer driver.Close()

	// 设置回调
	callbackCalled := false
	var callbackRecord *datasync.DataRecord
	driver.SetChangeCallback(func(record *datasync.DataRecord) {
		callbackCalled = true
		callbackRecord = record
	})

	// 创建记录
	record := &datasync.DataRecord{
		ID:        datasync.DataID("callback-test-001"),
		Type:      "test_callback",
		Version:   1,
		Timestamp: time.Now().UTC(),
		Payload:   []byte(`{"test":true}`),
	}
	if err := driver.ApplyRecord(ctx, record); err != nil {
		t.Fatalf("Failed to apply record: %v", err)
	}

	// 验证回调被调用
	if !callbackCalled {
		t.Error("Change callback was not called")
	}
	if callbackRecord == nil {
		t.Fatal("Callback record is nil")
	}
	if callbackRecord.ID != record.ID {
		t.Errorf("Expected callback ID %s, got %s", record.ID, callbackRecord.ID)
	}
}

// TestDriver_CDCBySequence 测试按序列号查询 CDC
func TestDriver_CDCBySequence(t *testing.T) {
	skipIfNoPostgres(t)

	config := getTestConfig()
	driver := NewDriver(config)

	ctx := context.Background()
	if err := driver.Initialize(ctx, nil); err != nil {
		t.Fatalf("Failed to initialize driver: %v", err)
	}
	defer driver.Close()

	dataType := "test_seq_cdc"

	// 创建多个记录
	for i := 0; i < 5; i++ {
		record := &datasync.DataRecord{
			ID:        datasync.DataID(fmt.Sprintf("seq-test-%03d", i)),
			Type:      dataType,
			Version:   1,
			Timestamp: time.Now().UTC(),
			Payload:   []byte(fmt.Sprintf(`{"idx":%d}`, i)),
		}
		if err := driver.ApplyRecord(ctx, record); err != nil {
			t.Fatalf("Failed to apply record: %v", err)
		}
	}

	// 获取最新序列号
	latestSeq, err := driver.GetLatestCDCSequence(ctx)
	if err != nil {
		t.Fatalf("Failed to get latest CDC sequence: %v", err)
	}
	if latestSeq == 0 {
		t.Error("Expected non-zero latest sequence")
	}

	// 从中间序列号查询
	midSeq := latestSeq - 2
	cdcRecords, err := driver.GetCDCRecordsBySequence(ctx, midSeq, 10)
	if err != nil {
		t.Fatalf("Failed to get CDC records by sequence: %v", err)
	}
	if len(cdcRecords) == 0 {
		t.Error("Expected some CDC records")
	}
}

// TestDriver_SyncStatus 测试同步状态
func TestDriver_SyncStatus(t *testing.T) {
	skipIfNoPostgres(t)

	config := getTestConfig()
	driver := NewDriver(config)

	ctx := context.Background()
	if err := driver.Initialize(ctx, nil); err != nil {
		t.Fatalf("Failed to initialize driver: %v", err)
	}
	defer driver.Close()

	dataType := "test_sync_status"

	// 创建记录
	record := &datasync.DataRecord{
		ID:        datasync.DataID("sync-status-001"),
		Type:      dataType,
		Version:   1,
		Timestamp: time.Now().UTC(),
		Payload:   []byte(`{}`),
	}
	if err := driver.ApplyRecord(ctx, record); err != nil {
		t.Fatalf("Failed to apply record: %v", err)
	}

	// 获取同步状态
	status, err := driver.GetSyncStatus(ctx, dataType)
	if err != nil {
		t.Fatalf("Failed to get sync status: %v", err)
	}
	if status.DataType != dataType {
		t.Errorf("Expected data type %s, got %s", dataType, status.DataType)
	}

	// 设置断点后再检查
	if err := driver.SetCheckpoint(ctx, dataType, status.LatestSequence); err != nil {
		t.Fatalf("Failed to set checkpoint: %v", err)
	}

	status, err = driver.GetSyncStatus(ctx, dataType)
	if err != nil {
		t.Fatalf("Failed to get sync status after checkpoint: %v", err)
	}
	if status.PendingCount != 0 {
		t.Errorf("Expected 0 pending after checkpoint, got %d", status.PendingCount)
	}
}
