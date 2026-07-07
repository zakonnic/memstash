package main

import (
	"encoding/json"
	"math/rand"
)

// User is the record used by scenario 3: two fields, marshaled to JSON before being stored so the cache's CostFunc
// (which only knows about []byte) has something to measure.
type User struct {
	Number int    `json:"number"`
	Text   string `json:"text"`
}

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randomString(rng *rand.Rand, n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rng.Intn(len(letters))]
	}
	return string(b)
}

func randomBytes(rng *rand.Rand, n int) []byte {
	b := make([]byte, n)
	_, _ = rng.Read(b) // math/rand.Rand.Read never errors
	return b
}

// randomPayload is the value generator for scenarios 1 and 2: an opaque random blob, 64-256 bytes.
func randomPayload(rng *rand.Rand) []byte {
	return randomBytes(rng, 64+rng.Intn(193))
}

// randomUserJSON is the value generator for scenario 3: a random User marshaled to JSON.
func randomUserJSON(rng *rand.Rand) []byte {
	u := User{Number: rng.Intn(1_000_000), Text: randomString(rng, 16+rng.Intn(49))}
	b, err := json.Marshal(u)
	if err != nil {
		panic(err) // User always marshals; a failure here would mean a broken encoding/json
	}
	return b
}
