package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	datasync "github.com/joy999/datasync/pkg"
)

// CDC表名常量
const cdcTableName = "_datasync_cdc"

// ChangeType 变更类型
type ChangeType string

const (
	ChangeTypeInsert ChangeType = "INSERT"
	ChangeTypeUpdate ChangeType = "UPDATE"
	ChangeTypeDelete ChangeType = "DELETE"
)

// CDCRecord CDC 记录结构
type CDCRecord struct {
	ID         uint64
	DataType   string
	RecordID   string
	ChangeType ChangeType
	Payload    []byte
	Version    int64
	SourceNode string
	GroupID    string
	CreatedAt  time.Time
}

// ToDataRecord 将 CDC 记录转换为 DataRecord
func (c *CDCRecord) ToDataRecord() *datasync.DataRecord {
	return &datasync.DataRecord{
		ID:        datasync.DataID(c.RecordID),
		Type:      c.DataType,
		Version:   c.Version,
		Timestamp: c.CreatedAt,
		Payload:   c.Payload,
		Metadata: map[string]string{
			"change_type": string(c.ChangeType),
			"cdc_id":      fmt.Sprintf("%d", c.ID),
		},
	}
}

// createCDCTable 创建 CDC 表
func (d *Driver) createCDCTable(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id BIGSERIAL PRIMARY KEY,
			data_type VARCHAR(255) NOT NULL,
			record_id VARCHAR(255) NOT NULL,
			change_type VARCHAR(20) NOT NULL CHECK (change_type IN ('INSERT', 'UPDATE', 'DELETE')),
			payload JSONB,
			version BIGINT NOT NULL DEFAULT 1,
			source_node VARCHAR(255) NOT NULL,
			group_id VARCHAR(255) NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`, cdcTableName)

	if _, err := d.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to create CDC table: %w", err)
	}

	// 创建索引
	indexes := []string{
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_group_time ON %s(group_id, created_at)", cdcTableName, cdcTableName),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_data_type ON %s(data_type)", cdcTableName, cdcTableName),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_record ON %s(data_type, record_id)", cdcTableName, cdcTableName),
	}

	for _, idxQuery := range indexes {
		if _, err := d.db.ExecContext(ctx, idxQuery); err != nil {
			return fmt.Errorf("failed to create CDC index: %w", err)
		}
	}

	return nil
}

// insertCDCRecordTx 在事务中插入 CDC 记录
func (d *Driver) insertCDCRecordTx(ctx context.Context, tx *sql.Tx, record *datasync.DataRecord, changeType string) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (data_type, record_id, change_type, payload, version, source_node, group_id, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, cdcTableName)

	// 将 Payload 转换为 JSONB
	var payloadJSON []byte
	if record.Payload != nil {
		// 如果 Payload 已经是 JSON，直接使用；否则包装
		if json.Valid(record.Payload) {
			payloadJSON = record.Payload
		} else {
			var err error
			payloadJSON, err = json.Marshal(record.Payload)
			if err != nil {
				return fmt.Errorf("failed to marshal payload: %w", err)
			}
		}
	}

	_, err := tx.ExecContext(ctx, query,
		record.Type,
		string(record.ID),
		changeType,
		payloadJSON,
		record.Version,
		d.config.NodeID,
		d.config.GroupID,
		time.Now().UTC(),
	)

	return err
}

// queryCDCRecords 查询 CDC 记录
func (d *Driver) queryCDCRecords(ctx context.Context, dataType string, since time.Time, limit int) ([]*datasync.DataRecord, error) {
	query := fmt.Sprintf(`
		SELECT id, data_type, record_id, change_type, payload, version, created_at
		FROM %s
		WHERE group_id = $1 AND created_at > $2
	`, cdcTableName)

	args := []interface{}{d.config.GroupID, since}
	argIdx := 3

	// 如果指定了 dataType，添加过滤条件
	if dataType != "" {
		query += fmt.Sprintf(" AND data_type = $%d", argIdx)
		args = append(args, dataType)
		argIdx++
	}

	query += fmt.Sprintf(" ORDER BY id LIMIT $%d", argIdx)
	args = append(args, limit)

	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query CDC records: %w", err)
	}
	defer rows.Close()

	var records []*datasync.DataRecord
	for rows.Next() {
		var cdc CDCRecord
		var payloadJSON []byte
		var changeTypeStr string

		if err := rows.Scan(
			&cdc.ID,
			&cdc.DataType,
			&cdc.RecordID,
			&changeTypeStr,
			&payloadJSON,
			&cdc.Version,
			&cdc.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan CDC record: %w", err)
		}

		cdc.ChangeType = ChangeType(changeTypeStr)
		cdc.Payload = payloadJSON
		cdc.SourceNode = d.config.NodeID
		cdc.GroupID = d.config.GroupID

		records = append(records, cdc.ToDataRecord())
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("CDC rows iteration error: %w", err)
	}

	return records, nil
}

// GetCDCRecordsBySequence 按序列号查询 CDC 记录（用于断点续传）
func (d *Driver) GetCDCRecordsBySequence(ctx context.Context, sequence uint64, limit int) ([]*CDCRecord, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return nil, fmt.Errorf("driver is closed")
	}

	query := fmt.Sprintf(`
		SELECT id, data_type, record_id, change_type, payload, version, source_node, group_id, created_at
		FROM %s
		WHERE group_id = $1 AND id > $2
		ORDER BY id
		LIMIT $3
	`, cdcTableName)

	rows, err := d.db.QueryContext(ctx, query, d.config.GroupID, sequence, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query CDC records by sequence: %w", err)
	}
	defer rows.Close()

	var records []*CDCRecord
	for rows.Next() {
		var cdc CDCRecord
		var payloadJSON []byte
		var changeTypeStr string

		if err := rows.Scan(
			&cdc.ID,
			&cdc.DataType,
			&cdc.RecordID,
			&changeTypeStr,
			&payloadJSON,
			&cdc.Version,
			&cdc.SourceNode,
			&cdc.GroupID,
			&cdc.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan CDC record: %w", err)
		}

		cdc.ChangeType = ChangeType(changeTypeStr)
		cdc.Payload = payloadJSON
		records = append(records, &cdc)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("CDC rows iteration error: %w", err)
	}

	return records, nil
}

// GetLatestCDCSequence 获取最新的 CDC 序列号
func (d *Driver) GetLatestCDCSequence(ctx context.Context) (uint64, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return 0, fmt.Errorf("driver is closed")
	}

	query := fmt.Sprintf("SELECT COALESCE(MAX(id), 0) FROM %s WHERE group_id = $1", cdcTableName)

	var seq uint64
	err := d.db.QueryRowContext(ctx, query, d.config.GroupID).Scan(&seq)
	if err != nil {
		return 0, fmt.Errorf("failed to get latest CDC sequence: %w", err)
	}

	return seq, nil
}

// DeleteRecord 删除记录（软删除或硬删除，这里使用硬删除并记录 CDC）
func (d *Driver) DeleteRecord(ctx context.Context, dataType string, id datasync.DataID) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return fmt.Errorf("driver is closed")
	}

	tableName := d.sanitizeTableName(dataType)

	// 开始事务
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// 获取旧数据用于 CDC
	var oldDataJSON []byte
	selectQuery := fmt.Sprintf("SELECT data FROM %s WHERE id = $1", tableName)
	err = tx.QueryRowContext(ctx, selectQuery, string(id)).Scan(&oldDataJSON)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("failed to get old data: %w", err)
	}

	// 删除记录
	deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE id = $1", tableName)
	if _, err := tx.ExecContext(ctx, deleteQuery, string(id)); err != nil {
		return fmt.Errorf("failed to delete record: %w", err)
	}

	// 写入 CDC 记录
	cdcRecord := &datasync.DataRecord{
		ID:        id,
		Type:      dataType,
		Version:   0, // 删除操作的版本号
		Timestamp: time.Now().UTC(),
		Payload:   oldDataJSON,
	}

	if err := d.insertCDCRecordTx(ctx, tx, cdcRecord, string(ChangeTypeDelete)); err != nil {
		return fmt.Errorf("failed to insert CDC record for delete: %w", err)
	}

	// 提交事务
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// PurgeCDCRecords 清理旧的 CDC 记录（维护功能）
func (d *Driver) PurgeCDCRecords(ctx context.Context, before time.Time) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return 0, fmt.Errorf("driver is closed")
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE group_id = $1 AND created_at < $2", cdcTableName)
	result, err := d.db.ExecContext(ctx, query, d.config.GroupID, before)
	if err != nil {
		return 0, fmt.Errorf("failed to purge CDC records: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	return rowsAffected, nil
}
