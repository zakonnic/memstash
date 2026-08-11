package load_generator

import "github.com/zakonnic/memstash/tests/workload"

// Ready-made Scenario.Value functions, from the workload package the benchmarks use. Each is a pure function of the
// key, so a Get can be verified against it and values stay the same across runs.

// blobSeed fixes the byte pool the values are sliced from, so it doesn't change between runs.
const blobSeed = 20260715

var (
	sharedBlob = workload.NewBlob(blobSeed, workload.DefaultBlobSize)

	sessionScenario = workload.SessionScenario{}
	cdnScenario     = workload.CDNScenario{}
	dbScenario      = workload.DBScenario{}
)

// SessionValue returns a web-session document, ~170-490 B of JSON.
func SessionValue(key string) []byte { return sessionScenario.Value(sharedBlob, key) }

// CDNValue returns a static asset, bimodal 0.6-64 KiB, ~7.4 KiB on average.
func CDNValue(key string) []byte { return cdnScenario.Value(sharedBlob, key) }

// DBValue returns a serialized DB row, ~250 B.
func DBValue(key string) []byte { return dbScenario.Value(sharedBlob, key) }
