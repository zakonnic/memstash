// Command load-generator drives three memstash caches under continuous, independent load until interrupted
// (Ctrl+C / SIGTERM), writing a per-scenario stats snapshot once a minute. Values come from the workload package
// and are verified against the scenario's Value function after every Get; errors land in errors.log. Scenarios,
// their Redis L2, and every knob are configurable via config.yaml (see buildScenarios for the built-in defaults).
//
// It is also the reference for using the package from your own main: build a []load_generator.Scenario, hand it to
// New, Start it.
//
// Build with `make load-generator`; run `./benchmarks/bin/load-generator -log-dir <dir>`.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	load_generator "github.com/zakonnic/memstash/benchmarks/load_generator"
)

func main() {
	logDir := flag.String("log-dir", ".", "directory to write the per-scenario JSON-lines log files into")
	configPath := flag.String("config", "config.yaml", "optional YAML file overriding per-scenario defaults")
	selfTest := flag.Bool("selftest", false,
		"write one synthetic error of every kind and exit - checks the console and errors.log without running any load")
	flag.Parse()

	if *selfTest {
		count, err := load_generator.SelfTest(load_generator.WithLogDir(*logDir))
		if err != nil {
			log.Fatalf("cannot run the self test: %v", err)
		}
		log.Printf("self test done: %d error(s) written to %s", count, filepath.Join(*logDir, "errors.log"))
		return
	}

	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("cannot load config %s: %v", *configPath, err)
	}
	scenarios, err := buildScenarios(cfg)
	if err != nil {
		log.Fatalf("cannot build scenarios: %v", err)
	}

	// Registered before the build so a Ctrl+C during it still shuts the process down through Start.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Println("building scenarios...")
	app, err := load_generator.New(scenarios, load_generator.WithLogDir(*logDir))
	if err != nil {
		log.Fatalf("cannot start the load generator: %v", err)
	}
	defer app.Shutdown()
	defer app.Recover("main") // registered after Shutdown, so it runs first and still has the error log

	// Everything printed goes through the console from here on, so nothing lands in the middle of the status block.
	log.SetOutput(app.Writer())
	app.PrintScenarios(app.Writer())

	log.Printf("load generator running %d scenario(s), logging to %s once a minute; press Ctrl+C to stop",
		len(scenarios), *logDir)

	app.Start(ctx)

	log.Println("shutting down, flushing final stats...")
	if err := app.Shutdown(); err != nil {
		log.Printf("close errors.log: %v", err)
	}
	log.Printf("stopped; %d error(s) logged to %s", app.Errors(), app.ErrorLogPath())
}
