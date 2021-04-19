// Copyright 2021 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"github.com/ethersphere/bee-repair/cmd/migrations"
)

func main() {
	migrations.Run()
}
