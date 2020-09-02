package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"time"

	log1 "github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"

	"encoding/csv"

	"github.com/jszwec/csvutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/prober"
	grpcserver "github.com/thanos-io/thanos/pkg/server/grpc"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

var _ = storepb.AggrChunk{}

const (
	grpcBindAddr = "0.0.0.0:9130"
)

type csvEntry struct {
	// [dateRep day month year cases deaths countriesAndTerritories geoId countryterritoryCode popData2019 continentExp Cumulative_number_for_14_days_of_COVID-19_cases_per_100000]
	Date   string `csv:"dateRep"`
	Cases  int64  `csv:"cases"`
	Deaths int64  `csv:"deaths"`
	Country
}

type Country struct {
	Code       string  `csv:"geoId"`
	Name       string  `csv:"countriesAndTerritories"`
	Continent  string  `csv:"continentExp"`
	Population *uint64 `csv:"popData2019"`
}

func newCovidStore(logger log1.Logger) *covidStore {
	if logger == nil {
		logger = log1.NewNopLogger()
	}
	return &covidStore{
		logger:         logger,
		registry:       prometheus.NewRegistry(),
		component:      component.UnknownStoreAPI,
		externalLabels: labels.Labels{
			/*
				labels.Label{
					Name:  "data",
					Value: "covid",
				},
				labels.Label{
					Name:  "source",
					Value: "ECDC",
				},
			*/
		},
	}
}

type covidStore struct {
	logger log1.Logger

	db             *tsdb.DB
	registry       *prometheus.Registry
	component      component.StoreAPI
	store          *store.TSDBStore
	externalLabels labels.Labels
}

func (s *covidStore) Open() error {
	db, err := tsdb.Open(
		"/tmp/path",
		s.logger,
		s.registry,
		&tsdb.Options{
			RetentionDuration: int64(10 * 365 * 24 * time.Hour / time.Millisecond),
			MinBlockDuration:  int64(2 * 365 * 24 * time.Hour / time.Millisecond),
			MaxBlockDuration:  int64(2 * 365 * 24 * time.Hour / time.Millisecond),
		},
	)
	if err != nil {
		return err
	}
	s.db = db

	s.store = store.NewTSDBStore(
		s.logger,
		s.registry,
		s.db,
		s.component,
		s.externalLabels,
	)
	return nil
}

func (s *covidStore) add(series map[string]*prompb.TimeSeries, e *csvEntry) error {
	metric := model.Metric{}
	metric[model.LabelName(model.MetricNameLabel)] = model.LabelValue("covid_cases_total")
	metric[model.LabelName("country_code")] = model.LabelValue(e.Country.Code)

	// retrieve existing metric
	values, ok := series[metric.String()]
	if !ok {
		values = &prompb.TimeSeries{}
		labelNames := make([]string, 0, len(metric))
		for k := range metric {
			labelNames = append(labelNames, string(k))
		}
		sort.Strings(labelNames) // Sort for unittests.
		for _, k := range labelNames {
			values.Labels = append(values.Labels, prompb.Label{Name: string(k), Value: string(metric[model.LabelName(k)])})
		}
		series[metric.String()] = values
	}

	timestamp, err := time.Parse("02/01/2006", e.Date)
	if err != nil {
		return err
	}

	sample := prompb.Sample{
		Timestamp: timestamp.UnixNano() / 1e6,
		Value:     float64(e.Cases),
	}

	if len(values.Samples) == 0 {
		values.Samples = append(values.Samples, sample)
		return nil
	}

	index := 0
	for pos, v := range values.Samples {
		if sample.Timestamp < v.Timestamp {
			index = pos
			break
		}
	}

	if len(values.Samples) == index {
		values.Samples = append(values.Samples, sample)
		return nil
	}

	values.Samples = append(values.Samples[:index+1], values.Samples[index:]...)
	values.Samples[index] = sample

	return nil
}

func (s *covidStore) ListenAndServe() error {
	grpcProbe := prober.NewGRPC()
	return grpcserver.New(
		s.logger,
		s.registry,
		nil, // tracer,
		s.component,
		grpcProbe,
		s.store,
		nil, // rulesProxy
		grpcserver.WithListen(grpcBindAddr),
	).ListenAndServe()
}

func labelsToPromLabels(lset []prompb.Label) labels.Labels {
	ret := make(labels.Labels, len(lset))
	for i, l := range lset {
		ret[i] = labels.Label{Name: l.Name, Value: l.Value}
	}
	return ret
}

func (s *covidStore) Process() error {
	f, err := os.Open("data.csv")
	if err != nil {
		return err
	}

	series := make(map[string]*prompb.TimeSeries)

	dec, err := csvutil.NewDecoder(csv.NewReader(f))
	if err != nil {
		return err
	}

	for {
		e := csvEntry{}
		if err := dec.Decode(&e); err == io.EOF {
			break
		} else if err != nil {
			continue
		}

		if err := s.add(series, &e); err != nil {
			log.Fatalf("failed to add entry: %v", err)
		}

		//log.Printf("entry=%+v", e) //date=%s cases=%s deaths=%s cc=%s cn=%s", record[datePos], record[casesPos], record[deathsPos], record[countryCodePos], record[countryNamePos])

	}

	for k := range series {

		labels := labelsToPromLabels(series[k].Labels)

		app := s.db.Appender()

		var sum float64
		for _, val := range series[k].Samples {
			usedValue := val.Value
			if val.Value < 0 {
				usedValue = 0
			}
			sum += usedValue

			level.Debug(s.logger).Log("usedValue", usedValue, "sum", sum, "ts", val.Timestamp)

			if _, err := app.Add(
				labels,
				val.Timestamp,
				sum,
			); err != nil {
				return fmt.Errorf("error appending value: %w", err)
			}
		}

		if err := app.Commit(); err != nil {
			return err
		}
	}

	if s.db.Compact(); err != nil {
		return err
	}

	return nil
}

type componentCovid struct {
}

func (componentCovid) String() string {
	return "covid"
}

func (componentCovid) implementsStoreAPI() {}

func (componentCovid) ToProto() storepb.StoreType {
	return storepb.StoreType_UNKNOWN
}

func main() {
	logger := log1.NewLogfmtLogger(os.Stderr)

	store := newCovidStore(logger)

	if err := store.Open(); err != nil {
		log.Fatalf("error opening TSDB: %v", err)
	}

	if err := store.Process(); err != nil {
		log.Fatalf("error processing data: %v", err)
	}

	if err := store.ListenAndServe(); err != nil {
		log.Fatalf("error listening: %v", err)
	}
}
