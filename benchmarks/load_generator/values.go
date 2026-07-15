package main

import "github.com/zakonnic/memstash/tests/workload"

// Values come from the workload package; its three scenario types, in declaration order, map onto the three
// scenarios: scenario-1 -> SessionScenario, scenario-2 -> CDNScenario, scenario-3 -> DBScenario. Each Value is a
// pure function of the key, so the source-of-truth map can verify Gets and values stay stable across runs.

// truthBlobSeed fixes the byte pool the values are sliced from, so it doesn't change between runs.
const truthBlobSeed = 20260715

var sharedBlob = workload.NewBlob(truthBlobSeed, workload.DefaultBlobSize)

// valueFunc returns the deterministic value bytes for a key.
type valueFunc func(key string) []byte

func sessionValue(key string) []byte { return workload.SessionScenario{}.Value(sharedBlob, key) }
func cdnValue(key string) []byte     { return workload.CDNScenario{}.Value(sharedBlob, key) }
func dbValue(key string) []byte      { return workload.DBScenario{}.Value(sharedBlob, key) }
