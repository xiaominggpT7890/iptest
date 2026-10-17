package worker

import (
	"context"
	"sync"
	"sync/atomic"

	"iptest/internal/store"
)

type sweepCursor struct {
	mu          sync.Mutex
	db          *store.DB
	taskID      int64
	minimum     int64
	maximum     int64
	next        int64
	end         int64
	reserved    bool
	currentNext atomic.Int64
	initialized atomic.Bool
}

func (s *sweepCursor) take(ctx context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if !s.reserved || s.next == s.end {
		start, end, err := s.db.ReserveSweepBlock(ctx, s.taskID, s.minimum, s.maximum, 1000)
		if err != nil {
			return 0, err
		}
		s.next, s.end, s.reserved = start, end, true
	}
	sessid := s.next
	s.next++
	next := s.next
	if next > s.maximum {
		next = s.minimum
	}
	s.currentNext.Store(next)
	s.initialized.Store(true)
	return sessid, nil
}

func (s *sweepCursor) current() *int64 {
	if !s.initialized.Load() {
		return nil
	}
	next := s.currentNext.Load()
	return &next
}

func (s *sweepCursor) save(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.reserved {
		return nil
	}
	next := s.next
	if next > s.maximum {
		next = s.minimum
	}
	return s.db.SaveSweepCursor(ctx, s.taskID, next, s.end)
}
