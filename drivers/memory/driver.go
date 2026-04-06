package memory

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	datasync "github.com/joy999/datasync/pkg"
)

// DriverIDValue 内存驱动唯一标识
const DriverIDValue datasync.DriverID = 0x01

// Driver 内存存储驱动实现
type Driver struct {
	dataType       string
	records        map[datasync.DataID]*datasync.DataRecord
	recordsMutex   sync.RWMutex
	changeCallback func(*datasync.DataRecord)
	ctx            context.Context
}

// NewDriver 创建内存存储驱动
func NewDriver(dataType string) *Driver {
	return &Driver{
		dataType:       dataType,
		records:        make(map[datasync.DataID]*datasync.DataRecord),
		changeCallback: nil,
	}
}

// Initialize 初始化驱动
func (d *Driver) Initialize(ctx context.Context, config map[string]interface{}) error {
	d.ctx = ctx
	return nil
}

// Close 关闭驱动
func (d *Driver) Close() error {
	// 内存驱动不需要特殊关闭操作
	return nil
}

// GetRecords 分页获取记录
func (d *Driver) GetRecords(ctx context.Context, dataType string, cursor string, limit int) ([]*datasync.DataRecord, string, error) {
	d.recordsMutex.RLock()
	defer d.recordsMutex.RUnlock()

	// 构建记录列表
	records := make([]*datasync.DataRecord, 0, len(d.records))
	for _, record := range d.records {
		if record.Type == dataType {
			records = append(records, record)
		}
	}

	// 处理分页
	start := 0
	_ = cursor // cursor 预留用于分页，当前未使用
	_ = start

	end := start + limit
	if end > len(records) {
		end = len(records)
	}

	var nextCursor string
	if end < len(records) {
		// 这里可以生成下一个cursor
		nextCursor = "next"
	}

	if start >= len(records) {
		return []*datasync.DataRecord{}, "", nil
	}

	return records[start:end], nextCursor, nil
}

// GetRecord 获取单条记录
func (d *Driver) GetRecord(ctx context.Context, dataType string, id datasync.DataID) (*datasync.DataRecord, error) {
	d.recordsMutex.RLock()
	defer d.recordsMutex.RUnlock()

	record, ok := d.records[id]
	if !ok || record.Type != dataType {
		return nil, nil
	}

	return record, nil
}

// ApplyRecord 应用同步过来的数据
func (d *Driver) ApplyRecord(ctx context.Context, record *datasync.DataRecord) error {
	d.recordsMutex.Lock()
	defer d.recordsMutex.Unlock()

	// 检查版本号
	existingRecord, ok := d.records[record.ID]
	if ok && existingRecord.Version >= record.Version {
		// 跳过旧版本
		return nil
	}

	// 保存记录
	d.records[record.ID] = record

	// 触发变更回调
	if d.changeCallback != nil {
		d.changeCallback(record)
	}

	return nil
}

// GetChanges 获取变更记录（用于增量同步）
func (d *Driver) GetChanges(ctx context.Context, dataType string, since time.Time, limit int) ([]*datasync.DataRecord, error) {
	d.recordsMutex.RLock()
	defer d.recordsMutex.RUnlock()

	// 构建变更记录列表
	changes := make([]*datasync.DataRecord, 0)
	for _, record := range d.records {
		if record.Type == dataType && record.Timestamp.After(since) {
			changes = append(changes, record)
			if len(changes) >= limit {
				break
			}
		}
	}

	return changes, nil
}

// GetDataTypes 获取支持的数据类型
func (d *Driver) GetDataTypes(ctx context.Context) ([]string, error) {
	return []string{d.dataType}, nil
}

// GetLatestVersion 获取最新版本号
func (d *Driver) GetLatestVersion(ctx context.Context, dataType string, id datasync.DataID) (int64, error) {
	d.recordsMutex.RLock()
	defer d.recordsMutex.RUnlock()

	record, ok := d.records[id]
	if !ok || record.Type != dataType {
		return 0, nil
	}

	return record.Version, nil
}

// SetChangeCallback 设置变更回调
func (d *Driver) SetChangeCallback(callback func(*datasync.DataRecord)) {
	d.changeCallback = callback
}

// SaveRecord 保存记录（用于示例）
func (d *Driver) SaveRecord(record *datasync.DataRecord) error {
	// 确保记录有版本号和时间戳
	if record.Version == 0 {
		record.Version = 1
	}
	if record.Timestamp.IsZero() {
		record.Timestamp = time.Now()
	}

	return d.ApplyRecord(d.ctx, record)
}

// GetDriverID 获取驱动唯一标识
func (d *Driver) GetDriverID() datasync.DriverID {
	return DriverIDValue
}

// Marshal 将 DataRecord 序列化为字节（使用 JSON）
func (d *Driver) Marshal(record *datasync.DataRecord) ([]byte, error) {
	// 使用 JSON 序列化
	return json.Marshal(record)
}

// Unmarshal 将字节反序列化为 DataRecord（使用 JSON）
func (d *Driver) Unmarshal(data []byte) (*datasync.DataRecord, error) {
	var record datasync.DataRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, err
	}
	return &record, nil
}
