package hash_test

import (
	"crypto/sha256"
	"encoding/json"
	"testing"

	"github.com/TimKotowski/pg-kafka-outbox/hash"
	"github.com/stretchr/testify/assert"
)

func TestGenerateGroupIdHashFromKey(t *testing.T) {
	t.Run("generate 256 hashes, make sure identical hashes", func(t *testing.T) {
		h1 := hash.NewHash(sha256.New())
		d := struct {
			Day   int
			Month int
			Year  int
		}{
			Day:   8,
			Month: 2,
			Year:  2025,
		}
		dataProto, err := json.Marshal(d)
		assert.NoError(t, err)
		err = h1.Write(dataProto)
		assert.NoError(t, err)

		h2 := hash.NewHash(sha256.New())
		d2 := struct {
			Day   int
			Month int
			Year  int
		}{
			Day:   8,
			Month: 2,
			Year:  2025,
		}
		dataProto2, err := json.Marshal(d2)
		assert.NoError(t, err)
		err = h2.Write(dataProto2)
		assert.NoError(t, err)

		assert.Equal(t, h1.Key(), h2.Key())
	})
}
