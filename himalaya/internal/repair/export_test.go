// Copyright 2021 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package repair

import (
	"github.com/ethersphere/bee/pkg/storage"
)

func WithMockStore(st storage.Storer) Option {
	return func(r *Repairer) {
		r.store = st
	}
}
