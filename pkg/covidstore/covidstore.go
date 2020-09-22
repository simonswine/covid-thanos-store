package covidstore

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	log "github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"

	"encoding/csv"
	"encoding/json"

	"github.com/jszwec/csvutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/pkg/value"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/server/grpc"
	grpcserver "github.com/thanos-io/thanos/pkg/server/grpc"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

var _ = storepb.AggrChunk{}

const (
	ECDCSourceURL           = "https://opendata.ecdc.europa.eu/covid19/casedistribution/csv/"
	grpcBindAddr            = "0.0.0.0:9130"
	metricCountryPopulation = "country_population"
)

type csvEntry struct {
	// [dateRep day month year cases deaths countriesAndTerritories geoId countryterritoryCode popData2019 continentExp Cumulative_number_for_14_days_of_COVID-19_cases_per_100000]
	Date   string `csv:"dateRep"`
	Cases  int64  `csv:"cases"`
	Deaths int64  `csv:"deaths"`
	Country
}

type Country struct {
	Code       string `csv:"geoId"`
	Name       string `csv:"countriesAndTerritories"`
	Continent  string `csv:"continentExp"`
	Population *int64 `csv:"popData2019"`
}

func New() *covidStore {
	return &covidStore{
		logger:    log.NewNopLogger(),
		component: component.UnknownStoreAPI,
		externalLabels: labels.Labels{
			labels.Label{
				Name:  "data",
				Value: "covid",
			},
			labels.Label{
				Name:  "source",
				Value: "ECDC",
			},
		},

		CachePath:     "cache.csv",
		RefreshPeriod: 30 * time.Minute,
		SourceURL:     ECDCSourceURL,
	}
}

type covidStore struct {
	logger log.Logger

	store         *store.TSDBStore
	storeDB       *tsdb.DB
	storeMetadata *metadata
	storeLock     sync.RWMutex

	component      component.StoreAPI
	externalLabels labels.Labels
	server         *grpc.Server

	CachePath     string
	RefreshPeriod time.Duration
	SourceURL     string
}

func (s *covidStore) WithLogger(l log.Logger) *covidStore {
	s.logger = l
	return s
}

func (s *covidStore) Run() error {
	stopCh := make(chan struct{})

	singalCh := make(chan os.Signal)
	signal.Notify(singalCh, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		sig := <-singalCh
		_ = level.Info(s.logger).Log("msg", fmt.Sprintf("got %s signal. exiting...", sig))

		// stop channel
		close(stopCh)

		// shutdown server
		if s.server != nil {
			s.server.Shutdown(nil)
		}

	}()

	if err := s.Process(); err != nil {
		return fmt.Errorf("error processing data: %w", err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(s.RefreshPeriod)
		for {
			select {
			case <-t.C:
				if err := s.Process(); err != nil {
					_ = level.Error(s.logger).Log("msg", "refresh of covid data failed", "err", err)
				}
			case <-stopCh:
				return
			}
		}
	}()

	if err := s.ListenAndServe(); err != nil {
		return fmt.Errorf("error listening: %w", err)
	}

	// cleanup TSDB
	if s.storeDB != nil {
		if err := cleanupTSDB(s.storeDB); err != nil {
			return err
		}
	}

	wg.Wait()

	return nil
}

func addElementToSeries(series map[string]*prompb.TimeSeries, e *csvEntry) error {
	defaultLabels := func(name string) func(*csvEntry) model.Metric {
		return func(e *csvEntry) model.Metric {
			return model.Metric{
				model.LabelName(model.MetricNameLabel): model.LabelValue(name),
				model.LabelName("country_code"):        model.LabelValue(e.Country.Code),
			}
		}
	}

	timestamp, err := time.Parse("02/01/2006", e.Date)
	if err != nil {
		return err
	}

	for _, m := range []struct {
		labels func(*csvEntry) model.Metric
		value  func(*csvEntry) float64
	}{
		{
			labels: defaultLabels("covid_cases_total"),
			value: func(e *csvEntry) float64 {
				if e.Cases < 0 {
					return 0.0
				}
				return float64(e.Cases)
			},
		},
		{
			labels: defaultLabels("covid_cases_corrections_total"),
			value: func(e *csvEntry) float64 {
				if e.Cases > 0 {
					return 0.0
				}
				return float64(-e.Cases)
			},
		},
		{
			labels: defaultLabels("covid_deaths_total"),
			value: func(e *csvEntry) float64 {
				if e.Deaths < 0 {
					return 0.0
				}
				return float64(e.Deaths)
			},
		},
		{
			labels: defaultLabels("covid_deaths_corrections_total"),
			value: func(e *csvEntry) float64 {
				if e.Deaths > 0 {
					return 0.0
				}
				return float64(-e.Deaths)
			},
		},
		{
			labels: func(e *csvEntry) model.Metric {
				m := defaultLabels(metricCountryPopulation)(e)
				m["country_name"] = model.LabelValue(e.Country.Name)
				m["continent"] = model.LabelValue(e.Country.Continent)
				return m
			},
			value: func(e *csvEntry) float64 {
				if e.Country.Population == nil {
					return float64(value.NormalNaN)
				}
				return float64(*e.Country.Population)
			},
		},
	} {
		metric := m.labels(e)
		values, ok := series[metric.String()]
		if !ok {
			values = &prompb.TimeSeries{}
			labelNames := make([]string, 0, len(metric))
			for k := range metric {
				labelNames = append(labelNames, string(k))
			}
			sort.Strings(labelNames)
			for _, k := range labelNames {
				values.Labels = append(values.Labels, prompb.Label{Name: string(k), Value: string(metric[model.LabelName(k)])})
			}
			series[metric.String()] = values
		}

		sample := prompb.Sample{
			Timestamp: timestamp.UnixNano() / 1e6,
			Value:     m.value(e),
		}

		if len(values.Samples) == 0 {
			values.Samples = append(values.Samples, sample)
			continue
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
			continue
		}

		values.Samples = append(values.Samples[:index+1], values.Samples[index:]...)
		values.Samples[index] = sample
	}

	return nil
}

func (s *covidStore) ListenAndServe() error {
	grpcProbe := prober.NewGRPC()
	s.server = grpcserver.New(
		s.logger,
		prometheus.NewRegistry(), // registry
		nil,                      // tracer,
		s.component,
		grpcProbe,
		s,
		nil, // rulesProxy
		grpcserver.WithListen(grpcBindAddr),
	)
	return s.server.ListenAndServe()
}

func labelsToPromLabels(lset []prompb.Label) labels.Labels {
	ret := make(labels.Labels, len(lset))
	for i, l := range lset {
		ret[i] = labels.Label{Name: l.Name, Value: l.Value}
	}
	return ret
}

type readerCloseWrapper struct {
	reader  io.Reader
	closers []io.Closer
}

func (r *readerCloseWrapper) Read(p []byte) (n int, err error) {
	return r.reader.Read(p)
}

func (r *readerCloseWrapper) Close() error {
	var errs []error
	for _, c := range r.closers {
		if err := c.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return errs[0]
}

type metadata struct {
	LastModified string `json:"last-modified"`
	ETag         string `json:"etag"`
}

func (s *covidStore) latestCSV() (io.ReadCloser, *metadata, error) {

	var cache *metadata

	cacheFile, err := os.Open(s.CachePath)
	if err == nil {
		cache = new(metadata)
		if err := json.NewDecoder(cacheFile).Decode(cache); err != nil {
			_ = level.Warn(s.logger).Log("msg", "failed reading metadata from cache, ignoring cache", "err", err)
			cache = nil
		}
	} else if !os.IsNotExist(err) {
		return nil, nil, fmt.Errorf("error opening cache file %s: %v", s.CachePath, err)
	}

	req, err := http.NewRequest("GET", s.SourceURL, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating request for raw covid data: %w", err)
	}

	if cache != nil {
		req.Header.Set("If-None-Match", cache.ETag)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("error downloading covid data: %w", err)
	}

	if resp.StatusCode == http.StatusNotModified {
		_ = level.Info(s.logger).Log("msg", "cached covid raw data is still the latest", "last-modified", cache.LastModified, "etag", cache.ETag)

		// cleanup connection
		if _, err := io.Copy(ioutil.Discard, resp.Body); err != nil {
			return nil, nil, fmt.Errorf("error discarding body: %w", err)
		}
		if err := resp.Body.Close(); err != nil {
			return nil, nil, fmt.Errorf("error closing body: %w", err)
		}

		// reset file to correct position
		_, err := cacheFile.Seek(0, 0)
		if err != nil {
			return nil, nil, fmt.Errorf("error moving to start of cache file: %w", err)
		}
		var b = make([]byte, 1)
		for {
			if _, err := cacheFile.Read(b); err != nil {
				return nil, nil, fmt.Errorf("error reading from cache: %w", err)
			}
			if b[0] == '\n' {
				break
			}
		}

		return cacheFile, cache, nil
	} else if resp.StatusCode/100 != 2 {
		return nil, nil, fmt.Errorf("error downloading covid data: unexpected status code %d", resp.StatusCode)
	}

	// close old cache file and open new one
	if cacheFile != nil {
		if err := cacheFile.Close(); err != nil {
			return nil, nil, err
		}
	}
	cacheFile, err = os.OpenFile(s.CachePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating cache file: %w", err)
	}

	cache = &metadata{
		ETag:         resp.Header.Get("etag"),
		LastModified: resp.Header.Get("last-modified"),
	}

	_ = level.Info(s.logger).Log("msg", "downloading latest covid raw data", "last-modified", cache.LastModified, "etag", cache.ETag)

	if err := json.NewEncoder(cacheFile).Encode(cache); err != nil {
		return nil, nil, fmt.Errorf("error writing cache metadata: %w", err)
	}

	return &readerCloseWrapper{
		reader:  io.TeeReader(resp.Body, cacheFile),
		closers: []io.Closer{resp.Body, cacheFile},
	}, cache, nil
}

func (s *covidStore) Process() error {
	return s.process(func() (io.ReadCloser, *metadata, error) {
		return s.latestCSV()
	})
}

/// Info returns meta information about a store e.g labels that makes that store unique as well as time range that is
/// available.
func (c *covidStore) Info(ctx context.Context, r *storepb.InfoRequest) (*storepb.InfoResponse, error) {
	c.storeLock.RLock()
	s := c.store
	c.storeLock.RUnlock()
	return s.Info(ctx, r)
}

/// Series streams each Series (Labels and chunk/downsampling chunk) for given label matchers and time range.
///
/// Series should strictly stream full series after series, optionally split by time. This means that a single frame can contain
/// partition of the single series, but once a new series is started to be streamed it means that no more data will
/// be sent for previous one.
/// Series has to be sorted.
///
/// There is no requirements on chunk sorting, however it is recommended to have chunk sorted by chunk min time.
/// This heavily optimizes the resource usage on Querier / Federated Queries.
func (c *covidStore) Series(r *storepb.SeriesRequest, srv storepb.Store_SeriesServer) error {
	c.storeLock.RLock()
	s := c.store
	c.storeLock.RUnlock()
	return s.Series(r, srv)
}

/// LabelNames returns all label names that is available.
/// Currently unimplemented in all Thanos implementations, because Query API does not implement this either.
func (c *covidStore) LabelNames(ctx context.Context, r *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	c.storeLock.RLock()
	s := c.store
	c.storeLock.RUnlock()
	return s.LabelNames(ctx, r)
}

/// LabelValues returns all label values for given label name.
func (c *covidStore) LabelValues(ctx context.Context, r *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	c.storeLock.RLock()
	s := c.store
	c.storeLock.RUnlock()
	return s.LabelValues(ctx, r)
}

func (s *covidStore) process(loadData func() (io.ReadCloser, *metadata, error)) error {
	f, m, err := loadData()
	if err != nil {
		return err
	}
	defer f.Close()

	if m != nil && s.storeMetadata != nil && m.ETag == s.storeMetadata.ETag {
		_ = level.Info(s.logger).Log("msg", "TSDB is already on the latest data")
		return nil
	}

	dir, err := ioutil.TempDir("", "covid-prometheus-data")
	if err != nil {
		return err
	}

	h, err := tsdb.NewHead(nil, s.logger, nil, 100*365*24*3600*1000, dir, nil, tsdb.DefaultStripeSize, nil)
	if err != nil {
		return err
	}

	if err := h.Init(math.MinInt64); err != nil {
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

		if err := addElementToSeries(series, &e); err != nil {
			return err
		}
	}

	for k := range series {

		labels := labelsToPromLabels(series[k].Labels)

		app := h.Appender()

		var sum float64
		sumUp := labels.Get(model.MetricNameLabel) != metricCountryPopulation
		for _, val := range series[k].Samples {
			var value float64
			if sumUp {
				sum += val.Value
				value = sum
			} else {
				value = val.Value
			}

			if _, err := app.Add(
				labels,
				val.Timestamp,
				value,
			); err != nil {
				return fmt.Errorf("error appending value: %w", err)
			}
		}

		if err := app.Commit(); err != nil {
			return err
		}
	}

	seriesCount := h.NumSeries()
	mint := h.MinTime()
	maxt := h.MaxTime() + 1

	_ = level.Info(s.logger).Log("msg", "flushing", "series_count", seriesCount, "mint", timestamp.Time(mint), "maxt", timestamp.Time(maxt), "dir", dir)

	// Flush head to disk as a block.
	compactor, err := tsdb.NewLeveledCompactor(
		context.Background(),
		nil,
		s.logger,
		[]int64{int64(1000 * (2 * time.Hour).Seconds())}, // Does not matter, used only for planning.
		chunkenc.NewPool())
	if err != nil {
		return fmt.Errorf("create leveled compactor: %w", err)
	}
	if _, err := compactor.Write(dir, h, mint, maxt, nil); err != nil {
		return fmt.Errorf("compactor write: %w", err)
	}

	if err := h.Close(); err != nil {
		return err
	}

	db, err := tsdb.Open(
		dir,
		s.logger,
		nil,
		&tsdb.Options{
			WALSegmentSize: -1,
		},
	)
	if err != nil {
		return err
	}

	s.storeLock.Lock()
	oldStoreDB := s.storeDB
	s.storeMetadata = m
	s.storeDB = db
	s.store = store.NewTSDBStore(
		s.logger,
		nil,
		db,
		s.component,
		s.externalLabels,
	)
	s.storeLock.Unlock()

	// clean up old store
	if oldStoreDB != nil {
		if err := cleanupTSDB(oldStoreDB); err != nil {
			return err
		}
	}

	return nil
}

func cleanupTSDB(db *tsdb.DB) error {
	if err := db.Close(); err != nil {
		return fmt.Errorf("error closing old db: %w", err)
	}
	if err := os.RemoveAll(db.Dir()); err != nil {
		return fmt.Errorf("error removing old db: %w", err)
	}
	return nil
}
