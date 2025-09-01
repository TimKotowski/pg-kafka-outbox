package hash

import (
	"encoding/hex"
	"hash"
)

type Hash struct {
	hash hash.Hash
}

func NewHash(hash hash.Hash) *Hash {
	return &Hash{
		hash: hash,
	}
}

func (h *Hash) Key() string {
	return hex.EncodeToString(h.hash.Sum(nil))
}

func (h *Hash) Write(args ...[]byte) error {
	for _, arg := range args {
		_, err := h.hash.Write(arg)
		if err != nil {
			return err
		}
	}

	return nil
}
