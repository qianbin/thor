// Copyright (c) 2018 The VeChainThor developers

// Distributed under the GNU Lesser General Public License v3.0 software license, see the accompanying
// file LICENSE or <https://www.gnu.org/licenses/lgpl-3.0.html>

package co

import (
	"runtime"
	"sync/atomic"
)

var numCPU = runtime.NumCPU()

// Parallel assigns to run a batch of tasks using as many CPU as it can.
func Parallel(f func(q chan<- func()) interface{}) <-chan interface{} {
	taskQueue := make(chan func(), numCPU*16)
	done := make(chan interface{}, 1)

	go func() {
		done <- f(taskQueue)
		close(taskQueue)
	}()

	nGo := int32(numCPU)
	for i := 0; i < numCPU; i++ {
		go func() {
			for task := range taskQueue {
				task()
			}

			if atomic.AddInt32(&nGo, -1) == 0 {
				close(done)
			}
		}()
	}
	return done
}
