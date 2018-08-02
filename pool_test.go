/*
 * Revision History:
 *     Initial: 2018/07/11        Tong Yuehong
 */

package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"
)

const (
	pSize = 4
	wSize = 2
)

func TestScheduler(t *testing.T) {
	counter := 0
	p := New(pSize, wSize)
	wg := &sync.WaitGroup{}

	wg.Add(pSize)
	for i := 0; i < pSize; i++ {
		p.Schedule(context.Background(), TaskFunc(func(ctx context.Context) error {
			counter++
			wg.Done()
			return nil
		}))
	}

	wg.Wait()

	p.Stop()

	if counter != pSize {
		t.Errorf("counter is expected as %d, actually %d", pSize, counter)
	}
}

func TestScheduleWithTimeout(t *testing.T) {
	counter := 0
	p := New(pSize, wSize)
	wg := &sync.WaitGroup{}

	f := func(ctx context.Context) error {
		counter++
		time.Sleep(2 * time.Second)
		wg.Done()
		return nil
	}

	wg.Add(pSize + wSize)
	for i := 0; i < wSize+pSize; i++ {
		p.Schedule(context.Background(), TaskFunc(f))
	}

	err := p.ScheduleWithTimeout(context.Background(), 1*time.Second, TaskFunc(f))
	if err == nil {
		t.Error("scheduler succeed")
	}

	wg.Wait()

	p.Stop()
}

func TestPoolStop(t *testing.T) {
	p := New(pSize, wSize)
	p.Stop()

	wg := sync.WaitGroup{}
	wg.Add(2)
	f := func(ctx context.Context) error {
		wg.Done()
		return nil
	}

	if err := p.Schedule(context.Background(), TaskFunc(f)); err == nil {
		t.Error("Schedule succeed, failure expected")
	}

	if err := p.ScheduleWithTimeout(context.Background(), 1*time.Second, TaskFunc(f)); err == nil {
		t.Error("ScheduleWithTimeout succeed, failure expected")
	}

	wg.Done()
}

func TestTaskCrash(t *testing.T) {
	counter := 0
	p := New(pSize, wSize)
	wg := &sync.WaitGroup{}

	wg.Add(pSize + wSize)
	for i := 0; i < pSize+wSize; i++ {
		p.Schedule(context.Background(), TaskFunc(func(ctx context.Context) error {
			counter++
			wg.Done()
			panic("panic")
			return nil
		}))
	}

	wg.Wait()

	p.Stop()

	if counter != pSize+wSize {
		t.Errorf("counter is expected as %d, actually %d", pSize+wSize, counter)
	}
}

func TestCancel(t *testing.T) {
	counter := 0
	p := New(pSize, wSize)
	wg := &sync.WaitGroup{}

	wg.Add(pSize + 1)

	f := func(ctx context.Context) error {
		time.Sleep(3 * time.Second)
		wg.Done()
		return nil
	}

	for i := 0; i < pSize; i++ {
		p.Schedule(context.Background(), TaskFunc(f))
	}

	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)

	p.Schedule(ctx, TaskFunc(func(ctx context.Context) error {
		select {
		case <-time.After(1 * time.Second):
			counter++
		case <-ctx.Done():
		}
		wg.Done()
		return nil
	}))

	wg.Done()

	wg.Wait()

	p.Stop()

	if counter != 0 {
		t.Errorf("counter is expected as %d, actually %d", 0, counter)
	}
}
