package main

import (
	"fmt"
	"os"
	"time"

	log "github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/urfave/cli"

	"github.com/simonswine/covid-thanos-store/pkg/covidstore"
)

var (
	date    = "unknown"
	version = "unknown"
	commit  = "unknown"
)

func main() {
	logger := log.NewLogfmtLogger(os.Stderr)
	logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)

	store := covidstore.New().WithLogger(logger)

	app := &cli.App{
		Name:    "covid-thanos-store",
		Version: fmt.Sprintf("%s (commit=%s date=%s)", version, commit, date),
		Usage:   "expose Covid case data as Prometheus endpoint",

		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "cache-file",
				Value:       "cache.csv",
				Usage:       "cache path to file for storing latest raw data locally",
				Destination: &store.CachePath,
			},
			&cli.DurationFlag{
				Name:        "refresh-period",
				Value:       30 * time.Minute,
				Usage:       "refresh period how often new data should be pulled",
				Destination: &store.RefreshPeriod,
			},
			&cli.StringFlag{
				Name:        "source-url",
				Value:       covidstore.ECDCSourceURL,
				Usage:       "source URL for covid CSV data",
				Destination: &store.SourceURL,
			},
		},

		Action: func(c *cli.Context) error {

			return store.Run()
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		_ = level.Error(logger).Log("msg", err)
		os.Exit(1)
	}

}
