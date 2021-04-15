package repair

import (
	"github.com/ethersphere/bee/pkg/storage"
)

func WithMockStore(st storage.Storer) Option {
	return func(r *Repairer) {
		r.store = st
	}
}
