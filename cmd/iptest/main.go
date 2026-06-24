package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"iptest/internal/api"
	"iptest/internal/store"
	"iptest/internal/task"
)

func main() {
	var (
		listen string
		dbPath string
	)
	flag.StringVar(&listen, "listen", envOr("IPTEST_LISTEN", ":8088"), "HTTP listen address")
	flag.StringVar(&dbPath, "db", envOr("IPTEST_DB", "iptest.db"), "SQLite database path")
	flag.Parse()

	abs, _ := filepath.Abs(dbPath)
	log.Printf("[iptest] database: %s", abs)
	log.Printf("[iptest] listen:   %s", listen)

	db, err := store.Open(dbPath)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	mgr := task.NewManager(db)
	srv := api.NewServer(db, mgr)

	httpSrv := &http.Server{
		Addr:              listen,
		Handler:           srv.Router(),
		ReadHeaderTimeout: 15 * time.Second,
	}
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http: %v", err)
		}
	}()
	log.Printf("[iptest] ready -> http://localhost%s", listen)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Printf("[iptest] shutting down...")
	mgr.Shutdown()
	_ = httpSrv.Close()
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

var _ = fmt.Sprintf
