// Copyright (c) 2018 The VeChainThor developers

// Distributed under the GNU Lesser General Public License v3.0 software license, see the accompanying
// file LICENSE or <https://www.gnu.org/licenses/lgpl-3.0.html>

package co

import (
	"runtime"
)

var numCPU = runtime.NumCPU()

// Parallel assigns to run a batch of tasks using as many CPU as it can.
func Parallel(f func(q chan<- func())) {
	var (
		taskQueue = make(chan func(), numCPU*16)
		goes      Goes
	)

	defer func() {
		close(taskQueue)
		goes.Wait()
	}()

	for i := 0; i < numCPU; i++ {
		goes.Go(func() {
			for task := range taskQueue {
				task()
			}
		})
	}
	f(taskQueue)
}
