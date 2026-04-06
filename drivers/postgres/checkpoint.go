package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// Checkpoint 表名常量
const checkpointTableName = "_datasync_checkpoint"

// Checkpoint 同步断点记录
type Checkpoint struct {
	ID        uint64
	GroupID   string
	DataType  string
	Sequence  uint64
	UpdatedAt time.Time
}

// createCheckpointTable 创建 Checkpoint 表
func (d *Driver) createCheckpointTable(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			group_id VARCHAR(255) NOT NULL,
			data_type VARCHAR(255) NOT NULL,
			sequence BIGINT NOT NULL DEFAULT 0,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(group_id, data_type)
		)
	`, checkpointTableName)

	if _, err := d.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to create checkpoint table: %w", err)
	}

	// 创建索引
	indexQuery := fmt.Sprintf(
		"CREATE INDEX IF NOT EXISTS idx_%s_group ON %s(group_id)",
		checkpointTableName, checkpointTableName,
	)
	if _, err := d.db.ExecContext(ctx, indexQuery); err != nil {
		return fmt.Errorf("failed to create checkpoint index: %w", err)
	}

	return nil
}

// GetCheckpoint 获取指定数据类型的同步断点
// 返回最后同步的 CDC 序列号，如果没有记录则返回 0
func (d *Driver) GetCheckpoint(ctx context.Context, dataType string) (uint64, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return 0, fmt.Errorf("driver is closed")
	}

	query := fmt.Sprintf(`
		SELECT sequence 
		FROM %s 
		WHERE group_id = $1 AND data_type = $2
	`, checkpointTableName)

	var sequence uint64
	err := d.db.QueryRowContext(ctx, query, d.config.GroupID, dataType).Scan(&sequence)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("failed to get checkpoint: %w", err)
	}

	return sequence, nil
}

// SetCheckpoint 设置指定数据类型的同步断点
func (d *Driver) SetCheckpoint(ctx context.Context, dataType string, sequence uint64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return fmt.Errorf("driver is closed")
	}

	query := fmt.Sprintf(`
		INSERT INTO %s (group_id, data_type, sequence, updated_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (group_id, data_type)
		DO UPDATE SET sequence = $3, updated_at = $4
	`, checkpointTableName)

	_, err := d.db.ExecContext(ctx, query, d.config.GroupID, dataType, sequence, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("failed to set checkpoint: %w", err)
	}

	return nil
}

// GetAllCheckpoints 获取所有数据类型的同步断点
func (d *Driver) GetAllCheckpoints(ctx context.Context) (map[string]uint64, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return nil, fmt.Errorf("driver is closed")
	}

	query := fmt.Sprintf(`
		SELECT data_type, sequence 
		FROM %s 
		WHERE group_id = $1
	`, checkpointTableName)

	rows, err := d.db.QueryContext(ctx, query, d.config.GroupID)
	if err != nil {
		return nil, fmt.Errorf("failed to get all checkpoints: %w", err)
	}
	defer rows.Close()

	checkpoints := make(map[string]uint64)
	for rows.Next() {
		var dataType string
		var sequence uint64
		if err := rows.Scan(&dataType, &sequence); err != nil {
			return nil, fmt.Errorf("failed to scan checkpoint: %w", err)
		}
		checkpoints[dataType] = sequence
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("checkpoint rows iteration error: %w", err)
	}

	return checkpoints, nil
}

// ResetCheckpoint 重置指定数据类型的同步断点
func (d *Driver) ResetCheckpoint(ctx context.Context, dataType string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return fmt.Errorf("driver is closed")
	}

	query := fmt.Sprintf(`
		UPDATE %s 
		SET sequence = 0, updated_at = $1
		WHERE group_id = $2 AND data_type = $3
	`, checkpointTableName)

	result, err := d.db.ExecContext(ctx, query, time.Now().UTC(), d.config.GroupID, dataType)
	if err != nil {
		return fmt.Errorf("failed to reset checkpoint: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		// 如果没有记录，创建一个初始值为 0 的记录
		return d.SetCheckpoint(ctx, dataType, 0)
	}

	return nil
}

// DeleteCheckpoint 删除指定数据类型的同步断点
func (d *Driver) DeleteCheckpoint(ctx context.Context, dataType string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return fmt.Errorf("driver is closed")
	}

	query := fmt.Sprintf(`
		DELETE FROM %s 
		WHERE group_id = $1 AND data_type = $2
	`, checkpointTableName)

	_, err := d.db.ExecContext(ctx, query, d.config.GroupID, dataType)
	if err != nil {
		return fmt.Errorf("failed to delete checkpoint: %w", err)
	}

	return nil
}

// GetCheckpointUpdatedAt 获取指定数据类型断点的最后更新时间
func (d *Driver) GetCheckpointUpdatedAt(ctx context.Context, dataType string) (time.Time, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return time.Time{}, fmt.Errorf("driver is closed")
	}

	query := fmt.Sprintf(`
		SELECT updated_at 
		FROM %s 
		WHERE group_id = $1 AND data_type = $2
	`, checkpointTableName)

	var updatedAt time.Time
	err := d.db.QueryRowContext(ctx, query, d.config.GroupID, dataType).Scan(&updatedAt)
	if err == sql.ErrNoRows {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get checkpoint updated_at: %w", err)
	}

	return updatedAt, nil
}

// SyncStatus 同步状态
type SyncStatus struct {
	DataType       string
	LastSequence   uint64
	LatestSequence uint64
	PendingCount   uint64
	LastUpdatedAt  time.Time
}

// GetSyncStatus 获取指定数据类型的同步状态
func (d *Driver) GetSyncStatus(ctx context.Context, dataType string) (*SyncStatus, error) {
	// 获取最后同步的断点
	lastSeq, err := d.GetCheckpoint(ctx, dataType)
	if err != nil {
		return nil, fmt.Errorf("failed to get checkpoint: %w", err)
	}

	// 获取最新的 CDC 序列号
	latestSeq, err := d.GetLatestCDCSequence(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest CDC sequence: %w", err)
	}

	// 获取更新时间
	updatedAt, err := d.GetCheckpointUpdatedAt(ctx, dataType)
	if err != nil {
		return nil, fmt.Errorf("failed to get checkpoint updated_at: %w", err)
	}

	// 计算待同步数量
	var pendingCount uint64
	if latestSeq > lastSeq {
		pendingCount = latestSeq - lastSeq
	}

	return &SyncStatus{
		DataType:       dataType,
		LastSequence:   lastSeq,
		LatestSequence: latestSeq,
		PendingCount:   pendingCount,
		LastUpdatedAt:  updatedAt,
	}, nil
}

// GetAllSyncStatus 获取所有数据类型的同步状态
func (d *Driver) GetAllSyncStatus(ctx context.Context) (map[string]*SyncStatus, error) {
	// 获取所有断点
	checkpoints, err := d.GetAllCheckpoints(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get all checkpoints: %w", err)
	}

	// 获取最新的 CDC 序列号
	latestSeq, err := d.GetLatestCDCSequence(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest CDC sequence: %w", err)
	}

	statusMap := make(map[string]*SyncStatus)
	for dataType, lastSeq := range checkpoints {
		var pendingCount uint64
		if latestSeq > lastSeq {
			pendingCount = latestSeq - lastSeq
		}

		updatedAt, _ := d.GetCheckpointUpdatedAt(ctx, dataType)

		statusMap[dataType] = &SyncStatus{
			DataType:       dataType,
			LastSequence:   lastSeq,
			LatestSequence: latestSeq,
			PendingCount:   pendingCount,
			LastUpdatedAt:  updatedAt,
		}
	}

	return statusMap, nil
}
