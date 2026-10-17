package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"

	"iptest/internal/config"
)

func sweepRange(body []byte) (config.SessidRange, error) {
	var task struct {
		Sessid config.SessidRange `json:"sessid_range"`
	}
	if err := json.Unmarshal(body, &task); err != nil {
		return config.SessidRange{}, err
	}
	if task.Sessid.Mode == "" {
		task.Sessid.Mode = config.SessidRandom
	}
	if task.Sessid.Min == 0 && task.Sessid.Max == 0 {
		task.Sessid.Min, task.Sessid.Max = 1_000_000, 9_000_000
	}
	return task.Sessid, nil
}

// ReserveSweepBlock 先持久化本块上界，崩溃后跳过未使用号码，避免重用当前块。
func (db *DB) ReserveSweepBlock(ctx context.Context, taskID, minimum, maximum, size int64) (start, end int64, err error) {
	if minimum < 0 || maximum < minimum || maximum == math.MaxInt64 || size <= 0 {
		return 0, 0, fmt.Errorf("invalid sweep reservation range or size")
	}
	err = db.withWriteTx(ctx, func(tx writeTransaction) error {
		var saved sql.NullInt64
		if err := tx.QueryRowContext(ctx, `SELECT sweep_next_sessid FROM tasks WHERE id=?`, taskID).Scan(&saved); err != nil {
			return err
		}
		start = minimum
		if saved.Valid {
			start = saved.Int64
		}
		if start == maximum+1 {
			start = minimum
		}
		if start < minimum || start > maximum {
			return fmt.Errorf("sweep cursor %d is outside [%d,%d]", start, minimum, maximum)
		}
		end = start + min(size, maximum-start+1)
		_, err := tx.ExecContext(ctx, `UPDATE tasks SET sweep_next_sessid=? WHERE id=?`, end, taskID)
		return err
	})
	if err != nil {
		return 0, 0, err
	}
	return start, end, nil
}

// SaveSweepCursor 在正常停止时将本块预留上界替换为精确待领取号码。
func (db *DB) SaveSweepCursor(ctx context.Context, taskID, next, reservedEnd int64) error {
	result, err := db.execWriteContext(ctx, `UPDATE tasks SET sweep_next_sessid=? WHERE id=? AND sweep_next_sessid=?`, next, taskID, reservedEnd)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return fmt.Errorf("task %d sweep reservation changed before saving cursor", taskID)
	}
	return nil
}
