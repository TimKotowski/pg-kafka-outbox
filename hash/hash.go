package hash

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
)

// GenerateAdvisoryLockHash returns first 8 bytes as uint64 from the groupId hash as advisory lock.
// Allthough possible to have a collison, very unlikeyly but possble.
// Decodes the hexadecimal encoding from sha256 hash from kafka key, to get the group id for alias for fast loo ups.
// The impact wouldn't get big, since it would just not process theese messsages for the key.
// Worst case, messsages for group id dereived from the key will wait to be processed eventually.
func GenerateAdvisoryLockHash(key string) (uint64, error) {
	decodedHash, err := hex.DecodeString(key)
	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint64(decodedHash[:8]), nil
}

// GenerateGroupIDHashFromKey generates sha256 hash then to hexadecimal encoding of the sha256 hash.
// Used to create the groupID from the kafka key as an alias to use for faster looks ups from DB and
// better debugging, and traceability.
func GenerateGroupIDHashFromKey(key []byte) (string, error) {
	h := sha256.New()

	_, err := h.Write(key)
	if err != nil {
		return "", err
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}
