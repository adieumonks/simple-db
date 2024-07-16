package util

import (
	"context"
	"sync"
	"time"
)

// 通知されるか、タイムアウトするまで待機する
func Wait(cond *sync.Cond, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	stopf := context.AfterFunc(ctx, func() {
		cond.L.Lock()
		defer cond.L.Unlock()

		cond.Broadcast()
	})
	defer stopf()

	cond.Wait()
}
