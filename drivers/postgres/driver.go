package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"sync"
	"time"

	datasync "github.com/joy999/datasync/pkg"
	_ "github.com/lib/pq"
)

// DriverIDValue PostgreSQL 驱动唯一标识
const DriverIDValue datasync.DriverID = 0x02

// PostgreSQLConfig 连接配置
type PostgreSQLConfig struct {
	Host     string
	Port     int
	Database string
	User     string
	Password string
	SSLMode  string
	// NodeID 当前节点ID
	NodeID string
	// GroupID 当前组ID
	GroupID string
}

// Driver PostgreSQL 存储驱动实现
type Driver struct {
	db             *sql.DB
	config         PostgreSQLConfig
	changeCallback func(*datasync.DataRecord)
	mu             sync.RWMutex
	closed         bool
}

// NewDriver 创建 PostgreSQL 驱动实例
func NewDriver(config PostgreSQLConfig) *Driver {
	if config.SSLMode == "" {
		config.SSLMode = "disable"
	}
	return &Driver{
		config: config,
	}
}

// Initialize 初始化驱动，创建必要的表结构
func (d *Driver) Initialize(ctx context.Context, initConfig map[string]interface{}) error {
	// 构建连接字符串
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		d.config.Host, d.config.Port, d.config.User, d.config.Password, d.config.Database, d.config.SSLMode)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return fmt.Errorf("failed to ping database: %w", err)
	}

	d.db = db
	d.closed = false

	// 创建 CDC 表
	if err := d.createCDCTable(ctx); err != nil {
		_ = d.db.Close()
		return fmt.Errorf("failed to create CDC table: %w", err)
	}

	// 创建 Checkpoint 表
	if err := d.createCheckpointTable(ctx); err != nil {
		_ = d.db.Close()
		return fmt.Errorf("failed to create checkpoint table: %w", err)
	}

	return nil
}

// Close 关闭驱动
func (d *Driver) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil
	}
	d.closed = true
	if d.db != nil {
		return d.db.Close()
	}
	return nil
}

// GetDriverID 获取驱动唯一标识
func (d *Driver) GetDriverID() datasync.DriverID {
	return DriverIDValue
}

// SetChangeCallback 设置变更回调函数
func (d *Driver) SetChangeCallback(callback func(*datasync.DataRecord)) {
	d.changeCallback = callback
}

// GetRecords 分页获取记录（用于全量同步）
func (d *Driver) GetRecords(ctx context.Context, dataType string, cursor string, limit int) ([]*datasync.DataRecord, string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return nil, "", fmt.Errorf("driver is closed")
	}

	tableName := d.sanitizeTableName(dataType)
	offset := 0
	if cursor != "" {
		_, _ = fmt.Sscanf(cursor, "%d", &offset)
	}

	//nolint:gosec // tableName is sanitized by sanitizeTableName
	query := fmt.Sprintf(`
		SELECT id, data, version, created_at, updated_at 
		FROM %s 
		ORDER BY id 
		LIMIT $1 OFFSET $2
	`, tableName)

	rows, err := d.db.QueryContext(ctx, query, limit, offset)
	if err != nil {
		return nil, "", fmt.Errorf("failed to query records: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	var records []*datasync.DataRecord
	for rows.Next() {
		record, err := d.scanRecord(rows, dataType)
		if err != nil {
			return nil, "", err
		}
		records = append(records, record)
	}

	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("rows iteration error: %w", err)
	}

	// 计算下一个 cursor
	// 查询多一条记录来判断是否有下一页
	nextCursor := ""
	if len(records) == limit {
		// 查询是否还有更多记录
		//nolint:gosec // tableName is sanitized by sanitizeTableName
		checkQuery := fmt.Sprintf("SELECT 1 FROM %s ORDER BY id LIMIT 1 OFFSET %d", tableName, offset+limit)
		var exists int
		err := d.db.QueryRowContext(ctx, checkQuery).Scan(&exists)
		if err == nil {
			nextCursor = fmt.Sprintf("%d", offset+limit)
		}
	}

	return records, nextCursor, nil
}

// GetRecord 获取单条记录
func (d *Driver) GetRecord(ctx context.Context, dataType string, id datasync.DataID) (*datasync.DataRecord, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return nil, fmt.Errorf("driver is closed")
	}

	tableName := d.sanitizeTableName(dataType)
	//nolint:gosec // tableName is sanitized by sanitizeTableName
	query := fmt.Sprintf(`
		SELECT id, data, version, created_at, updated_at 
		FROM %s 
		WHERE id = $1
	`, tableName)

	row := d.db.QueryRowContext(ctx, query, string(id))
	record, err := d.scanRecord(row, dataType)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get record: %w", err)
	}

	return record, nil
}

// ApplyRecord 应用数据记录（插入或更新）
func (d *Driver) ApplyRecord(ctx context.Context, record *datasync.DataRecord) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return fmt.Errorf("driver is closed")
	}

	tableName := d.sanitizeTableName(record.Type)

	// 确保表存在
	if err := d.createDataTableIfNotExists(ctx, tableName); err != nil {
		return fmt.Errorf("failed to create data table: %w", err)
	}

	// 开始事务
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	// 检查记录是否存在
	var exists bool
	//nolint:gosec // tableName is sanitized by sanitizeTableName
	checkQuery := fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE id = $1)", tableName)
	if err := tx.QueryRowContext(ctx, checkQuery, string(record.ID)).Scan(&exists); err != nil {
		return fmt.Errorf("failed to check record existence: %w", err)
	}

	// 序列化数据
	dataJSON, err := json.Marshal(record.Payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	changeType := "INSERT"
	now := time.Now().UTC()

	if exists {
		changeType = "UPDATE"
		// 更新记录
		//nolint:gosec // tableName is sanitized by sanitizeTableName
		updateQuery := fmt.Sprintf(`
			UPDATE %s 
			SET data = $1, version = $2, updated_at = $3 
			WHERE id = $4
		`, tableName)
		if _, err := tx.ExecContext(ctx, updateQuery, dataJSON, record.Version, now, string(record.ID)); err != nil {
			return fmt.Errorf("failed to update record: %w", err)
		}
	} else {
		// 插入记录
		//nolint:gosec // tableName is sanitized by sanitizeTableName
		insertQuery := fmt.Sprintf(`
			INSERT INTO %s (id, data, version, created_at, updated_at) 
			VALUES ($1, $2, $3, $4, $5)
		`, tableName)
		if _, err := tx.ExecContext(ctx, insertQuery, string(record.ID), dataJSON, record.Version, now, now); err != nil {
			return fmt.Errorf("failed to insert record: %w", err)
		}
	}

	// 写入 CDC
	if err := d.insertCDCRecordTx(ctx, tx, record, changeType); err != nil {
		return fmt.Errorf("failed to insert CDC record: %w", err)
	}

	// 提交事务
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// 触发回调
	if d.changeCallback != nil {
		d.changeCallback(record)
	}

	return nil
}

// GetChanges 获取变更记录（用于增量同步）
func (d *Driver) GetChanges(ctx context.Context, dataType string, since time.Time, limit int) ([]*datasync.DataRecord, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return nil, fmt.Errorf("driver is closed")
	}

	return d.queryCDCRecords(ctx, dataType, since, limit)
}

// GetDataTypes 获取支持的数据类型列表
func (d *Driver) GetDataTypes(ctx context.Context) ([]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return nil, fmt.Errorf("driver is closed")
	}

	query := `
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema = 'public' 
		AND table_type = 'BASE TABLE'
		AND table_name NOT LIKE '\_%' ESCAPE '\'
	`

	rows, err := d.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query table names: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	var types []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		types = append(types, tableName)
	}

	return types, rows.Err()
}

// GetLatestVersion 获取最新版本号
func (d *Driver) GetLatestVersion(ctx context.Context, dataType string, id datasync.DataID) (int64, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return 0, fmt.Errorf("driver is closed")
	}

	tableName := d.sanitizeTableName(dataType)
	//nolint:gosec // tableName is sanitized by sanitizeTableName
	query := fmt.Sprintf("SELECT COALESCE(MAX(version), 0) FROM %s WHERE id = $1", tableName)

	var version int64
	err := d.db.QueryRowContext(ctx, query, string(id)).Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("failed to get latest version: %w", err)
	}

	return version, nil
}

// Marshal 将 DataRecord 序列化为字节（使用 JSON）
func (d *Driver) Marshal(record *datasync.DataRecord) ([]byte, error) {
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

// sanitizeTableName 清理表名，防止 SQL 注入
func (d *Driver) sanitizeTableName(name string) string {
	// 只允许字母、数字和下划线
	re := regexp.MustCompile(`[^a-zA-Z0-9_]`)
	return re.ReplaceAllString(name, "_")
}

// createDataTableIfNotExists 创建数据表（如果不存在）
func (d *Driver) createDataTableIfNotExists(ctx context.Context, tableName string) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id VARCHAR(255) PRIMARY KEY,
			data JSONB NOT NULL,
			version BIGINT NOT NULL DEFAULT 1,
			created_at TIMESTAMP NOT NULL,
			updated_at TIMESTAMP NOT NULL
		)
	`, tableName)

	_, err := d.db.ExecContext(ctx, query)
	return err
}

// scanRecord 扫描记录行
type scanner interface {
	Scan(dest ...interface{}) error
}

func (d *Driver) scanRecord(sc scanner, dataType string) (*datasync.DataRecord, error) {
	var id string
	var dataJSON []byte
	var version int64
	var createdAt, updatedAt time.Time

	if err := sc.Scan(&id, &dataJSON, &version, &createdAt, &updatedAt); err != nil {
		return nil, err
	}

	// Payload 直接存储原始 JSON 数据
	return &datasync.DataRecord{
		ID:        datasync.DataID(id),
		Type:      dataType,
		Version:   version,
		Timestamp: updatedAt,
		Payload:   dataJSON,
	}, nil
}
