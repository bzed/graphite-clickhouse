package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"path"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/lomik/graphite-clickhouse/helper/client"
	"github.com/lomik/graphite-clickhouse/helper/datetime"

	"github.com/pelletier/go-toml"
)

var (
	preSQL = []string{
		"TRUNCATE TABLE IF EXISTS graphite_reverse",
		"TRUNCATE TABLE IF EXISTS graphite",
		"TRUNCATE TABLE IF EXISTS graphite_index",
		"TRUNCATE TABLE IF EXISTS graphite_tags",
	}
)

// PrometheusClient for making API calls
type PrometheusClient struct {
	BaseURL    string
	HTTPClient *http.Client
	Logger     *zap.Logger
}

func NewPrometheusClient(baseURL string, logger *zap.Logger) *PrometheusClient {
	return &PrometheusClient{
		BaseURL:    baseURL,
		HTTPClient: &http.Client{Timeout: 30 * time.Second},
		Logger:     logger,
	}
}

func (c *PrometheusClient) doRequest(method, path string, body io.Reader) ([]byte, int, error) {
	url := c.BaseURL + path
	if !strings.HasSuffix(c.BaseURL, "/") && !strings.HasPrefix(path, "/") {
		url = c.BaseURL + "/" + path
	}

	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("failed to read response: %w", err)
	}

	return responseBody, resp.StatusCode, nil
}

func (c *PrometheusClient) GetLabelValues(labelName string, start, end int64) ([]string, error) {
	path := fmt.Sprintf("/api/v1/label/%s/values?start=%d&end=%d", labelName, start, end)

	body, statusCode, err := c.doRequest("GET", path, nil)
	if err != nil {
		return nil, fmt.Errorf("label values request failed: %w", err)
	}
	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("label values request returned status %d: %s", statusCode, string(body))
	}

	var result struct {
		Status string   `json:"status"`
		Data   []string `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse label values response: %w", err)
	}
	if result.Status != "success" {
		return nil, fmt.Errorf("label values request failed with status: %s", result.Status)
	}

	return result.Data, nil
}

func (c *PrometheusClient) GetLabelNames(start, end int64) ([]string, error) {
	path := fmt.Sprintf("/api/v1/labels?start=%d&end=%d", start, end)

	body, statusCode, err := c.doRequest("GET", path, nil)
	if err != nil {
		return nil, fmt.Errorf("label names request failed: %w", err)
	}
	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("label names request returned status %d: %s", statusCode, string(body))
	}

	var result struct {
		Status string   `json:"status"`
		Data   []string `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse label names response: %w", err)
	}
	if result.Status != "success" {
		return nil, fmt.Errorf("label names request failed with status: %s", result.Status)
	}

	return result.Data, nil
}

func (c *PrometheusClient) GetSeries(matchers []string, start, end int64) ([]string, error) {
	// Convert matchers to query parameters
	matchParams := ""
	for i, matcher := range matchers {
		if i > 0 {
			matchParams += "&"
		}
		matchParams += fmt.Sprintf("match[]=%s", matcher)
	}
	path := fmt.Sprintf("/api/v1/series?%s&start=%d&end=%d", matchParams, start, end)

	body, statusCode, err := c.doRequest("GET", path, nil)
	if err != nil {
		return nil, fmt.Errorf("series request failed: %w", err)
	}
	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("series request returned status %d: %s", statusCode, string(body))
	}

	var result struct {
		Status string `json:"status"`
		Data   []struct {
			Labels     map[string]string `json:"__name__"`
			LabelPairs []struct {
				Name  string `json:"name"`
				Value string `json:"value"`
			} `json:"-"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse series response: %w", err)
	}
	if result.Status != "success" {
		return nil, fmt.Errorf("series request failed with status: %s", result.Status)
	}

	// Convert to string representation
	var seriesList []string
	for _, series := range result.Data {
		// Build Prometheus-style metric string
		metricStr := series.Labels["__name__"] + "{"
		var labelParts []string
		for _, pair := range series.LabelPairs {
			if pair.Name != "__name__" {
				labelParts = append(labelParts, fmt.Sprintf(`%s="%s"`, pair.Name, pair.Value))
			}
		}
		metricStr += strings.Join(labelParts, ",") + "}"
		seriesList = append(seriesList, metricStr)
	}

	return seriesList, nil
}

func (c *PrometheusClient) QueryInstant(query string, timestamp int64) ([]PromQueryResult, error) {
	path := fmt.Sprintf("/api/v1/query?query=%s&time=%d", query, timestamp)

	body, statusCode, err := c.doRequest("GET", path, nil)
	if err != nil {
		return nil, fmt.Errorf("instant query failed: %w", err)
	}
	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("instant query returned status %d: %s", statusCode, string(body))
	}

	var result struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string          `json:"resultType"`
			Result     json.RawMessage `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse instant query response: %w", err)
	}
	if result.Status != "success" {
		return nil, fmt.Errorf("instant query failed with status: %s", result.Status)
	}

	// Convert to our result format
	var promResults []PromQueryResult
	if result.Data.ResultType == "vector" {
		var vectorResult []PrometheusVectorResult
		if err := json.Unmarshal(result.Data.Result, &vectorResult); err != nil {
			return nil, fmt.Errorf("failed to parse vector result: %w", err)
		}
		for _, sample := range vectorResult {
			metricStr := sample.Metric.String()
			promResults = append(promResults, PromQueryResult{
				Metric: metricStr,
				Value:  []string{fmt.Sprintf("%d", sample.Timestamp), fmt.Sprintf("%f", sample.Value)},
			})
		}
	}

	return promResults, nil
}

func (c *PrometheusClient) QueryRange(query string, start, end int64, step string) ([]PromQueryRangeResult, error) {
	path := fmt.Sprintf("/api/v1/query_range?query=%s&start=%d&end=%d&step=%s", query, start, end, step)

	body, statusCode, err := c.doRequest("GET", path, nil)
	if err != nil {
		return nil, fmt.Errorf("range query failed: %w", err)
	}
	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("range query returned status %d: %s", statusCode, string(body))
	}

	var result struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string          `json:"resultType"`
			Result     json.RawMessage `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse range query response: %w", err)
	}
	if result.Status != "success" {
		return nil, fmt.Errorf("range query failed with status: %s", result.Status)
	}

	// Convert to our result format
	var promResults []PromQueryRangeResult
	if result.Data.ResultType == "matrix" {
		var matrixResult []PrometheusMatrixResult
		if err := json.Unmarshal(result.Data.Result, &matrixResult); err != nil {
			return nil, fmt.Errorf("failed to parse matrix result: %w", err)
		}
		for _, sample := range matrixResult {
			metricStr := sample.Metric.String()
			var values []string
			for _, value := range sample.Values {
				values = append(values, fmt.Sprintf("%d", value.Timestamp), fmt.Sprintf("%f", value.Value))
			}
			promResults = append(promResults, PromQueryRangeResult{
				Metric: metricStr,
				Values: values,
			})
		}
	}

	return promResults, nil
}

// Prometheus API response structures
type PrometheusQueryData struct {
	ResultType string          `json:"resultType"`
	Result     json.RawMessage `json:"result"`
}

type PrometheusSample struct {
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}

type PrometheusMetric struct {
	Labels map[string]string `json:"metric"`
}

// But the actual series response format is:
type PrometheusSeriesResult struct {
	Labels map[string]string `json:"metric"`
}

func (c *PrometheusClient) GetSeries(matchers []string, start, end int64) ([]string, error) {
	var result struct {
		Status string              `json:"status"`
		Data   []map[string]string `json:"data"` // ✅ Direct label maps
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse series response: %w", err)
	}

	var seriesList []string
	for _, labels := range result.Data {
		metricName := labels["__name__"]
		var labelParts []string
		for k, v := range labels {
			if k != "__name__" {
				labelParts = append(labelParts, fmt.Sprintf(`%s="%s"`, k, v))
			}
		}
		sort.Strings(labelParts) // Sort for consistency
		if len(labelParts) > 0 {
			seriesList = append(seriesList, metricName+"{"+strings.Join(labelParts, ",")+"}")
		} else {
			seriesList = append(seriesList, metricName)
		}
	}
	sort.Strings(seriesList)
	return seriesList, nil
}

func (m PrometheusMetric) String() string {
	metricName := m.Labels["__name__"]
	delete(m.Labels, "__name__")

	var parts []string
	for k, v := range m.Labels {
		parts = append(parts, fmt.Sprintf(`%s="%s"`, k, v))
	}

	if len(parts) == 0 {
		return metricName
	}
	return metricName + "{" + strings.Join(parts, ",") + "}"
}

type PrometheusVectorResult struct {
	Metric    PrometheusMetric `json:"metric"`
	Timestamp int64            `json:"timestamp"`
	Value     float64          `json:"value"`
}

type PrometheusMatrixResult struct {
	Metric PrometheusMetric   `json:"metric"`
	Values []PrometheusSample `json:"values"`
}

type Point struct {
	Value float64       `toml:"value"`
	Time  string        `toml:"time"`
	Delay time.Duration `toml:"delay"`

	time int64 `toml:"-"`
}

type InputMetric struct {
	Name   string        `toml:"name"`
	Points []Point       `toml:"points"`
	Round  time.Duration `toml:"round"`
}

type Metric struct {
	Name                    string    `toml:"name"`
	PathExpression          string    `toml:"path"`
	ConsolidationFunc       string    `toml:"consolidation"`
	StartTime               string    `toml:"start"`
	StopTime                string    `toml:"stop"`
	StepTime                int64     `toml:"step"`
	XFilesFactor            float32   `toml:"xfiles"`
	HighPrecisionTimestamps bool      `toml:"high_precision"`
	Values                  []float64 `toml:"values"`
	AppliedFunctions        []string  `toml:"applied_functions"`
	RequestStartTime        string    `toml:"req_start"`
	RequestStopTime         string    `toml:"req_stop"`
}

type RenderCheck struct {
	Name               string              `toml:"name"`
	Formats            []client.FormatType `toml:"formats"`
	From               string              `toml:"from"`
	Until              string              `toml:"until"`
	Targets            []string            `toml:"targets"`
	MaxDataPoints      int64               `toml:"max_data_points"`
	FilteringFunctions []string            `toml:"filtering_functions"`
	Timeout            time.Duration       `toml:"timeout"`
	DumpIfEmpty        []string            `toml:"dump_if_empty"`

	Optimize []string `toml:"optimize"` // optimize tables before run tests

	InCache  bool `toml:"in_cache"` // already in cache
	CacheTTL int  `toml:"cache_ttl"`

	ProxyDelay         time.Duration `toml:"proxy_delay"`
	ProxyBreakWithCode int           `toml:"proxy_break_with_code"`

	Result      []Metric `toml:"result"`
	ErrorRegexp string   `toml:"error_regexp"`

	from        int64           `toml:"-"`
	until       int64           `toml:"-"`
	errorRegexp *regexp.Regexp  `toml:"-"`
	result      []client.Metric `toml:"-"`
}

type MetricsFindCheck struct {
	Name    string              `toml:"name"`
	Formats []client.FormatType `toml:"formats"`
	From    string              `toml:"from"`
	Until   string              `toml:"until"`
	Query   string              `toml:"query"`
	Timeout time.Duration       `toml:"timeout"`

	DumpIfEmpty []string `toml:"dump_if_empty"`

	InCache  bool `toml:"in_cache"` // already in cache
	CacheTTL int  `toml:"cache_ttl"`

	ProxyDelay         time.Duration `toml:"proxy_delay"`
	ProxyBreakWithCode int           `toml:"proxy_break_with_code"`

	Result      []client.FindMatch `toml:"result"`
	ErrorRegexp string             `toml:"error_regexp"`

	from        int64          `toml:"-"`
	until       int64          `toml:"-"`
	errorRegexp *regexp.Regexp `toml:"-"`
}

type TagsCheck struct {
	Name    string              `toml:"name"`
	Names   bool                `toml:"names"` // TagNames or TagValues
	Formats []client.FormatType `toml:"formats"`
	From    string              `toml:"from"`
	Until   string              `toml:"until"`
	Query   string              `toml:"query"`
	Limits  uint64              `toml:"limits"`
	Timeout time.Duration       `toml:"timeout"`

	DumpIfEmpty []string `toml:"dump_if_empty"`

	InCache  bool `toml:"in_cache"` // already in cache
	CacheTTL int  `toml:"cache_ttl"`

	ProxyDelay         time.Duration `toml:"proxy_delay"`
	ProxyBreakWithCode int           `toml:"proxy_break_with_code"`

	Result      []string `toml:"result"`
	ErrorRegexp string   `toml:"error_regexp"`

	from        int64          `toml:"-"`
	until       int64          `toml:"-"`
	errorRegexp *regexp.Regexp `toml:"-"`
}

// Prometheus Label Values Check - Tests /api/v1/label/:name/values endpoint
type PromLabelValuesCheck struct {
	Name    string        `toml:"name"`
	Label   string        `toml:"label"`
	From    string        `toml:"from"`
	Until   string        `toml:"until"`
	Timeout time.Duration `toml:"timeout"`

	ProxyDelay         time.Duration `toml:"proxy_delay"`
	ProxyBreakWithCode int           `toml:"proxy_break_with_code"`

	Result      []string `toml:"result"`
	ErrorRegexp string   `toml:"error_regexp"`

	from        int64          `toml:"-"`
	until       int64          `toml:"-"`
	errorRegexp *regexp.Regexp `toml:"-"`
}

// Prometheus Label Names Check - Tests /api/v1/labels endpoint
type PromLabelNamesCheck struct {
	Name    string        `toml:"name"`
	From    string        `toml:"from"`
	Until   string        `toml:"until"`
	Timeout time.Duration `toml:"timeout"`

	ProxyDelay         time.Duration `toml:"proxy_delay"`
	ProxyBreakWithCode int           `toml:"proxy_break_with_code"`

	Result      []string `toml:"result"`
	ErrorRegexp string   `toml:"error_regexp"`

	from        int64          `toml:"-"`
	until       int64          `toml:"-"`
	errorRegexp *regexp.Regexp `toml:"-"`
}

// Prometheus Series Check - Tests /api/v1/series endpoint
type PromSeriesCheck struct {
	Name     string        `toml:"name"`
	Matchers []string      `toml:"matchers"`
	From     string        `toml:"from"`
	Until    string        `toml:"until"`
	Timeout  time.Duration `toml:"timeout"`

	ProxyDelay         time.Duration `toml:"proxy_delay"`
	ProxyBreakWithCode int           `toml:"proxy_break_with_code"`

	Result      []string `toml:"result"`
	ErrorRegexp string   `toml:"error_regexp"`

	from        int64          `toml:"-"`
	until       int64          `toml:"-"`
	errorRegexp *regexp.Regexp `toml:"-"`
}

// Prometheus Query Check - Tests /api/v1/query endpoint (instant queries)
type PromQueryCheck struct {
	Name    string        `toml:"name"`
	Query   string        `toml:"query"`
	From    string        `toml:"from"`
	Until   string        `toml:"until"`
	Timeout time.Duration `toml:"timeout"`

	ProxyDelay         time.Duration `toml:"proxy_delay"`
	ProxyBreakWithCode int           `toml:"proxy_break_with_code"`

	Result      []PromQueryResult `toml:"result"`
	ErrorRegexp string            `toml:"error_regexp"`

	from        int64          `toml:"-"`
	until       int64          `toml:"-"`
	errorRegexp *regexp.Regexp `toml:"-"`
}

// Prometheus Query Range Check - Tests /api/v1/query_range endpoint (range queries)
type PromQueryRangeCheck struct {
	Name    string        `toml:"name"`
	Query   string        `toml:"query"`
	From    string        `toml:"from"`
	Until   string        `toml:"until"`
	Step    string        `toml:"step"`
	Timeout time.Duration `toml:"timeout"`

	ProxyDelay         time.Duration `toml:"proxy_delay"`
	ProxyBreakWithCode int           `toml:"proxy_break_with_code"`

	Result      []PromQueryRangeResult `toml:"result"`
	ErrorRegexp string                 `toml:"error_regexp"`

	from        int64          `toml:"-"`
	until       int64          `toml:"-"`
	step        int64          `toml:"-"`
	errorRegexp *regexp.Regexp `toml:"-"`
}

// Prometheus Query Result - Result structure for instant queries
type PromQueryResult struct {
	Metric string   `toml:"metric"`
	Value  []string `toml:"value"` // [timestamp, value]
}

// Prometheus Query Range Result - Result structure for range queries
type PromQueryRangeResult struct {
	Metric string   `toml:"metric"`
	Values []string `toml:"values"` // [timestamp1, value1, timestamp2, value2, ...]
}

type TestSchema struct {
	Input      []InputMetric        `toml:"input"` // carbon-clickhouse input
	Clickhouse []Clickhouse         `toml:"clickhouse"`
	Proxy      HttpReverseProxy     `toml:"clickhouse_proxy"`
	Cch        CarbonClickhouse     `toml:"carbon_clickhouse"`
	Gch        []GraphiteClickhouse `toml:"graphite_clickhouse"`

	FindChecks            []*MetricsFindCheck     `toml:"find_checks"`
	TagsChecks            []*TagsCheck            `toml:"tags_checks"`
	RenderChecks          []*RenderCheck          `toml:"render_checks"`
	PromLabelValuesChecks []*PromLabelValuesCheck `toml:"prometheus_label_values_checks"`
	PromLabelNamesChecks  []*PromLabelNamesCheck  `toml:"prometheus_label_names_checks"`
	PromSeriesChecks      []*PromSeriesCheck      `toml:"prometheus_series_checks"`
	PromQueryChecks       []*PromQueryCheck       `toml:"prometheus_query_checks"`
	PromQueryRangeChecks  []*PromQueryRangeCheck  `toml:"prometheus_query_range_checks"`

	Precision time.Duration `toml:"precision"`

	dir        string          `toml:"-"`
	name       string          `toml:"-"` // test alias (from config name)
	chVersions map[string]bool `toml:"-"`
	// input map[string][]Point `toml:"-"`
}

func (schema *TestSchema) HasTLSSettings() bool {
	return strings.Contains(schema.dir, "tls")
}

func getFreeTCPPort(name string) (string, error) {
	if len(name) == 0 {
		name = "127.0.0.1:0"
	} else if !strings.Contains(name, ":") {
		name = name + ":0"
	}

	addr, err := net.ResolveTCPAddr("tcp", name)
	if err != nil {
		return name, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return name, err
	}

	defer l.Close()

	return l.Addr().String(), nil
}

func sendPlain(network, address string, metrics []InputMetric) error {
	if conn, err := net.DialTimeout(network, address, time.Second); err != nil {
		return err
	} else {
		bw := bufio.NewWriter(conn)

		for _, m := range metrics {
			conn.SetDeadline(time.Now().Add(time.Second))

			for _, point := range m.Points {
				if _, err = fmt.Fprintf(bw, "%s %f %d\n", m.Name, point.Value, point.time); err != nil {
					conn.Close()
					return err
				}

				if point.Delay > 0 {
					if err = bw.Flush(); err != nil {
						conn.Close()
						return err
					}

					time.Sleep(point.Delay)
				}
			}
		}

		if err = bw.Flush(); err != nil {
			conn.Close()
			return err
		}

		return conn.Close()
	}
}

func verifyGraphiteClickhouse(test *TestSchema, gch *GraphiteClickhouse, clickhouse *Clickhouse, testDir, clickhouseDir string, verbose, breakOnError bool, logger *zap.Logger) (testSuccess bool, verifyCount, verifyFailed int) {
	testSuccess = true

	err := gch.Start(testDir, clickhouse.URL(), test.Proxy.URL(), clickhouse.TLSURL(), logger)
	if err != nil {
		logger.Error("starting graphite-clickhouse",
			zap.String("config", test.name),
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouseDir),
			zap.String("graphite-clickhouse config", gch.ConfigTpl),
			zap.Error(err),
		)

		testSuccess = false

		return
	}

	for i := 100; i < 1000; i += 200 {
		time.Sleep(time.Duration(i) * time.Millisecond)

		if gch.Alive() {
			break
		}
	}

	// start tests
	for n, check := range test.FindChecks {
		verifyCount++

		test.Proxy.SetDelay(check.ProxyDelay)
		test.Proxy.SetBreakStatusCode(check.ProxyBreakWithCode)

		if len(check.Formats) == 0 {
			check.Formats = []client.FormatType{client.FormatPb_v3}
		}

		if errs := verifyMetricsFind(clickhouse, gch, check); len(errs) > 0 {
			verifyFailed++

			for _, e := range errs {
				fmt.Fprintln(os.Stderr, e)
			}

			logger.Error("verify metrics find",
				zap.String("config", test.name),
				zap.String("clickhouse version", clickhouse.Version),
				zap.String("clickhouse config", clickhouseDir),
				zap.String("graphite-clickhouse config", gch.ConfigTpl),
				zap.String("query", check.Query),
				zap.String("from_raw", check.From),
				zap.String("until_raw", check.Until),
				zap.Int64("from", check.from),
				zap.Int64("until", check.until),
				zap.String("name", check.Name+"["+strconv.Itoa(n)+"]"),
			)

			if breakOnError {
				debug(test, clickhouse, gch)
			}
		} else if verbose {
			logger.Info("verify metrics find",
				zap.String("config", test.name),
				zap.String("clickhouse version", clickhouse.Version),
				zap.String("clickhouse config", clickhouseDir),
				zap.String("graphite-clickhouse config", gch.ConfigTpl),
				zap.String("query", check.Query),
				zap.String("from_raw", check.From),
				zap.String("until_raw", check.Until),
				zap.Int64("from", check.from),
				zap.Int64("until", check.until),
				zap.String("name", check.Name+"["+strconv.Itoa(n)+"]"),
			)
		}
	}

	for n, check := range test.TagsChecks {
		verifyCount++

		test.Proxy.SetDelay(check.ProxyDelay)
		test.Proxy.SetBreakStatusCode(check.ProxyBreakWithCode)

		if len(check.Formats) == 0 {
			check.Formats = []client.FormatType{client.FormatJSON}
		}

		if errs := verifyTags(clickhouse, gch, check); len(errs) > 0 {
			verifyFailed++

			for _, e := range errs {
				fmt.Fprintln(os.Stderr, e)
			}

			logger.Error("verify tags",
				zap.String("config", test.name),
				zap.String("clickhouse version", clickhouse.Version),
				zap.String("clickhouse config", clickhouseDir),
				zap.String("graphite-clickhouse config", gch.ConfigTpl),
				zap.Bool("name", check.Names),
				zap.String("query", check.Query),
				zap.String("from_raw", check.From),
				zap.String("until_raw", check.Until),
				zap.Int64("from", check.from),
				zap.Int64("until", check.until),
				zap.String("name", check.Name+"["+strconv.Itoa(n)+"]"),
			)

			if breakOnError {
				debug(test, clickhouse, gch)
			}
		} else if verbose {
			logger.Info("verify tags",
				zap.String("config", test.name),
				zap.String("clickhouse version", clickhouse.Version),
				zap.String("clickhouse config", clickhouseDir),
				zap.String("graphite-clickhouse config", gch.ConfigTpl),
				zap.Bool("name", check.Names),
				zap.String("query", check.Query),
				zap.String("from_raw", check.From),
				zap.String("until_raw", check.Until),
				zap.Int64("from", check.from),
				zap.Int64("until", check.until),
				zap.String("name", check.Name+"["+strconv.Itoa(n)+"]"),
			)
		}
	}

	for n, check := range test.RenderChecks {
		verifyCount++

		test.Proxy.SetDelay(check.ProxyDelay)
		test.Proxy.SetBreakStatusCode(check.ProxyBreakWithCode)

		if len(check.Formats) == 0 {
			check.Formats = []client.FormatType{client.FormatPb_v3}
		}

		if len(check.Optimize) > 0 {
			for _, table := range check.Optimize {
				if success, out := clickhouse.Exec("OPTIMIZE TABLE " + table + " FINAL"); !success {
					logger.Error("optimize table",
						zap.String("config", test.name),
						zap.String("clickhouse version", clickhouse.Version),
						zap.String("clickhouse config", clickhouseDir),
						zap.String("graphite-clickhouse config", gch.ConfigTpl),
						zap.Strings("targets", check.Targets),
						zap.Strings("filtering_functions", check.FilteringFunctions),
						zap.String("from_raw", check.From),
						zap.String("until_raw", check.Until),
						zap.Int64("from", check.from),
						zap.Int64("until", check.until),
						zap.String("name", check.Name+"["+strconv.Itoa(n)+"]"),
						zap.String("table", table),
						zap.String("out", out),
					)
					time.Sleep(5 * time.Second)
				}
			}
		}

		if errs := verifyRender(clickhouse, gch, check, test.Precision); len(errs) > 0 {
			verifyFailed++

			for _, e := range errs {
				fmt.Fprintln(os.Stderr, e)
			}

			logger.Error("verify render",
				zap.String("config", test.name),
				zap.String("clickhouse version", clickhouse.Version),
				zap.String("clickhouse config", clickhouseDir),
				zap.String("graphite-clickhouse config", gch.ConfigTpl),
				zap.Strings("targets", check.Targets),
				zap.Strings("filtering_functions", check.FilteringFunctions),
				zap.String("from_raw", check.From),
				zap.String("until_raw", check.Until),
				zap.Int64("from", check.from),
				zap.Int64("until", check.until),
				zap.String("name", check.Name+"["+strconv.Itoa(n)+"]"),
			)

			if breakOnError {
				debug(test, clickhouse, gch)
			}
		} else if verbose {
			logger.Info("verify render",
				zap.String("config", test.name),
				zap.String("clickhouse version", clickhouse.Version),
				zap.String("clickhouse config", clickhouseDir),
				zap.String("graphite-clickhouse config", gch.ConfigTpl),
				zap.Strings("targets", check.Targets),
				zap.Strings("filtering_functions", check.FilteringFunctions),
				zap.String("from_raw", check.From),
				zap.String("until_raw", check.Until),
				zap.Int64("from", check.from),
				zap.Int64("until", check.until),
				zap.String("name", check.Name+"["+strconv.Itoa(n)+"]"),
			)
		}
	}

	// Execute Prometheus checks using helper function
	executePrometheusChecks(test, clickhouse, gch, test.PromLabelValuesChecks, "label values",
		func(check interface{}) (string, []zap.Field) {
			c := check.(*PromLabelValuesCheck)
			return "verify prom label values", []zap.Field{
				zap.String("label", c.Label),
			}
		}, clickhouseDir, logger, breakOnError, verbose)

	executePrometheusChecks(test, clickhouse, gch, test.PromLabelNamesChecks, "label names",
		func(check interface{}) (string, []zap.Field) {
			return "verify prom label names", []zap.Field{}
		}, clickhouseDir, logger, breakOnError, verbose)

	executePrometheusChecks(test, clickhouse, gch, test.PromSeriesChecks, "series",
		func(check interface{}) (string, []zap.Field) {
			c := check.(*PromSeriesCheck)
			return "verify prom series", []zap.Field{
				zap.Strings("matchers", c.Matchers),
			}
		}, clickhouseDir, logger, breakOnError, verbose)

	executePrometheusChecks(test, clickhouse, gch, test.PromQueryChecks, "query",
		func(check interface{}) (string, []zap.Field) {
			c := check.(*PromQueryCheck)
			return "verify prom query", []zap.Field{
				zap.String("query", c.Query),
			}
		}, clickhouseDir, logger, breakOnError, verbose)

	executePrometheusChecks(test, clickhouse, gch, test.PromQueryRangeChecks, "query range",
		func(check interface{}) (string, []zap.Field) {
			c := check.(*PromQueryRangeCheck)
			return "verify prom query range", []zap.Field{
				zap.String("query", c.Query),
				zap.String("step", c.Step),
				zap.Int64("step", c.step),
			}
		}, clickhouseDir, logger, breakOnError, verbose)

	if verifyFailed > 0 {
		testSuccess = false

		logger.Error("verify",
			zap.String("config", test.name),
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouseDir),
			zap.String("graphite-clickhouse config", gch.ConfigTpl),
			zap.Int64("count", int64(verifyCount)),
			zap.Int64("failed", int64(verifyFailed)),
		)
	}

	err = gch.Stop(true)
	if err != nil {
		logger.Error("stoping graphite-clickhouse",
			zap.String("config", test.name),
			zap.String("gch", gch.ConfigTpl),
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouseDir),
			zap.Error(err),
		)

		testSuccess = false
	}

	return
}

func testGraphiteClickhouse(test *TestSchema, clickhouse *Clickhouse, testDir, rootDir string, verbose, breakOnError bool, logger *zap.Logger) (testSuccess bool, verifyCount, verifyFailed int) {
	testSuccess = true

	for _, sql := range preSQL {
		if success, out := clickhouse.Exec(sql); !success {
			logger.Error("pre-execute",
				zap.String("config", test.name),
				zap.Any("clickhouse version", clickhouse.Version),
				zap.String("clickhouse config", clickhouse.Dir),
				zap.String("sql", sql),
				zap.String("out", out),
			)

			return
		}
	}

	if err := test.Proxy.Start(clickhouse.URL()); err != nil {
		logger.Error("starting clickhouse proxy",
			zap.String("config", test.name),
			zap.Any("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouse.Dir),
			zap.Error(err),
		)

		return
	}

	out, err := test.Cch.Start(testDir, "http://"+clickhouse.Container()+":8123")
	if err != nil {
		logger.Error("starting carbon-clickhouse",
			zap.String("config", test.name),
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouse.Dir),
			zap.Error(err),
			zap.String("out", out),
		)

		testSuccess = false
	}

	if testSuccess {
		logger.Info("starting e2e test",
			zap.String("config", test.name),
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouse.Dir),
		)
		time.Sleep(200 * time.Millisecond)
		// Populate test data
		err = sendPlain("tcp", test.Cch.address, test.Input)
		if err != nil {
			logger.Error("send plain to carbon-clickhouse",
				zap.String("config", test.name),
				zap.String("clickhouse version", clickhouse.Version),
				zap.String("clickhouse config", clickhouse.Dir),
				zap.Error(err),
			)

			testSuccess = false
		}

		if testSuccess {
			time.Sleep(2 * time.Second)
		}

		if testSuccess {
			for _, gch := range test.Gch {
				stepSuccess, vCount, vFailed := verifyGraphiteClickhouse(test, &gch, clickhouse, testDir, clickhouse.Dir, verbose, breakOnError, logger)
				verifyCount += vCount
				verifyFailed += vFailed

				if !stepSuccess {
					testSuccess = false
				}
			}
		}
	}

	out, err = test.Cch.Stop(true)
	if err != nil {
		logger.Error("stoping carbon-clickhouse",
			zap.String("config", test.name),
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouse.Dir),
			zap.Error(err),
			zap.String("out", out),
		)

		testSuccess = false
	}

	test.Proxy.Stop()

	if testSuccess {
		logger.Info("end e2e test",
			zap.String("config", test.name),
			zap.String("status", "success"),
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouse.Dir),
		)
	} else {
		logger.Error("end e2e test",
			zap.String("config", test.name),
			zap.String("status", "failed"),
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouse.Dir),
		)
	}

	return
}

func runTest(cfg *MainConfig, clickhouse *Clickhouse, rootDir string, now time.Time, verbose, breakOnError bool, logger *zap.Logger) (failed, total, verifyCount, verifyFailed int) {
	var isRunning bool

	total++

	if exist, out := containerExist(CchContainerName); exist {
		logger.Error("carbon-clickhouse already exist",
			zap.String("container", CchContainerName),
			zap.String("out", out),
		)

		isRunning = true
	}

	if isRunning {
		failed++
		return
	}

	success, vCount, vFailed := testGraphiteClickhouse(cfg.Test, clickhouse, cfg.Test.dir, rootDir, verbose, breakOnError, logger)
	if !success {
		failed++
	}

	verifyCount += vCount
	verifyFailed += vFailed

	return
}

func clickhouseStart(clickhouse *Clickhouse, logger *zap.Logger) bool {
	out, err := clickhouse.Start()
	if err != nil {
		logger.Error("starting clickhouse",
			zap.Any("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouse.Dir),
			zap.Error(err),
			zap.String("out", out),
		)
		clickhouse.Stop(true)

		return false
	}

	return true
}

func clickhouseStop(clickhouse *Clickhouse, logger *zap.Logger) (result bool) {
	result = true

	if !clickhouse.Alive() {
		clickhouse.CopyLog(os.TempDir(), 10)

		result = false
	}

	out, err := clickhouse.Stop(true)
	if err != nil {
		logger.Error("stoping clickhouse",
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouse.Dir),
			zap.Error(err),
			zap.String("out", out),
		)

		result = false
	}

	return result
}

// Prometheus API verification functions

func verifyPromLabelValues(clickhouse *Clickhouse, gch *GraphiteClickhouse, check *PromLabelValuesCheck) []string {
	var errs []string

	// Create Prometheus client
	promClient := NewPrometheusClient(fmt.Sprintf("http://%s", gch.PrometheusAddr()), gch.logger)

	// Make actual API call
	values, err := promClient.GetLabelValues(check.Label, check.from, check.until)
	if err != nil {
		errs = append(errs, fmt.Sprintf("Prometheus label values request failed: %v", err))
		return errs
	}

	// Compare with expected results
	if len(values) != len(check.Result) {
		errs = append(errs, fmt.Sprintf("Expected %d label values, got %d", len(check.Result), len(values)))
		return errs
	}

	// Check if all expected values are present
	for _, expected := range check.Result {
		found := false
		for _, actual := range values {
			if actual == expected {
				found = true
				break
			}
		}
		if !found {
			errs = append(errs, fmt.Sprintf("Expected label value '%s' not found in response", expected))
		}
	}

	return errs
}

func verifyPromLabelNames(clickhouse *Clickhouse, gch *GraphiteClickhouse, check *PromLabelNamesCheck) []string {
	var errs []string

	// Create Prometheus client
	promClient := NewPrometheusClient(fmt.Sprintf("http://%s", gch.PrometheusAddr()), gch.logger)

	// Make actual API call
	names, err := promClient.GetLabelNames(check.from, check.until)
	if err != nil {
		errs = append(errs, fmt.Sprintf("Prometheus label names request failed: %v", err))
		return errs
	}

	// Compare with expected results
	if len(names) != len(check.Result) {
		errs = append(errs, fmt.Sprintf("Expected %d label names, got %d", len(check.Result), len(names)))
		return errs
	}

	// Check if all expected names are present
	for _, expected := range check.Result {
		found := false
		for _, actual := range names {
			if actual == expected {
				found = true
				break
			}
		}
		if !found {
			errs = append(errs, fmt.Sprintf("Expected label name '%s' not found in response", expected))
		}
	}

	return errs
}

func verifyPromSeries(clickhouse *Clickhouse, gch *GraphiteClickhouse, check *PromSeriesCheck) []string {
	var errs []string

	// Create Prometheus client
	promClient := NewPrometheusClient(fmt.Sprintf("http://%s", gch.PrometheusAddr()), gch.logger)

	// Make actual API call
	series, err := promClient.GetSeries(check.Matchers, check.from, check.until)
	if err != nil {
		errs = append(errs, fmt.Sprintf("Prometheus series request failed: %v", err))
		return errs
	}

	// Compare with expected results
	if len(series) != len(check.Result) {
		errs = append(errs, fmt.Sprintf("Expected %d series, got %d", len(check.Result), len(series)))
		return errs
	}

	// Check if all expected series are present
	for _, expected := range check.Result {
		found := false
		for _, actual := range series {
			if actual == expected {
				found = true
				break
			}
		}
		if !found {
			errs = append(errs, fmt.Sprintf("Expected series '%s' not found in response", expected))
		}
	}

	return errs
}

func verifyPromQuery(clickhouse *Clickhouse, gch *GraphiteClickhouse, check *PromQueryCheck) []string {
	var errs []string

	// Create Prometheus client
	promClient := NewPrometheusClient(fmt.Sprintf("http://%s", gch.PrometheusAddr()), gch.logger)

	// Use the middle timestamp for instant query
	timestamp := (check.from + check.until) / 2

	// Make actual API call
	results, err := promClient.QueryInstant(check.Query, timestamp)
	if err != nil {
		errs = append(errs, fmt.Sprintf("Prometheus instant query failed: %v", err))
		return errs
	}

	// Compare with expected results
	if len(results) != len(check.Result) {
		errs = append(errs, fmt.Sprintf("Expected %d query results, got %d", len(check.Result), len(results)))
		return errs
	}

	// Check if all expected results are present
	for i, expected := range check.Result {
		if i >= len(results) {
			errs = append(errs, fmt.Sprintf("Missing result %d", i))
			continue
		}

		actual := results[i]
		if actual.Metric != expected.Metric {
			errs = append(errs, fmt.Sprintf("Expected metric '%s', got '%s'", expected.Metric, actual.Metric))
		}

		if len(actual.Value) != 2 {
			errs = append(errs, fmt.Sprintf("Expected value pair, got %d values", len(actual.Value)))
		} else if len(expected.Value) != 2 {
			errs = append(errs, fmt.Sprintf("Expected value pair in test data, got %d values", len(expected.Value)))
		} else {
			// Compare timestamps (allow some tolerance)
			expectedTime, err1 := strconv.ParseInt(expected.Value[0], 10, 64)
			actualTime, err2 := strconv.ParseInt(actual.Value[0], 10, 64)
			if err1 == nil && err2 == nil && absInt64(expectedTime-actualTime) > 60 {
				errs = append(errs, fmt.Sprintf("Timestamp mismatch for metric '%s': expected ~%s, got %s",
					expected.Metric, expected.Value[0], actual.Value[0]))
			}

			// Compare values
			expectedVal, err1 := strconv.ParseFloat(expected.Value[1], 64)
			actualVal, err2 := strconv.ParseFloat(actual.Value[1], 64)
			if err1 == nil && err2 == nil && math.Abs(expectedVal-actualVal) > 0.001 {
				errs = append(errs, fmt.Sprintf("Value mismatch for metric '%s': expected %s, got %s",
					expected.Metric, expected.Value[1], actual.Value[1]))
			}
		}
	}

	return errs
}

// Helper function for absolute value
func absInt64(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

// Generic helper function to execute Prometheus checks and reduce boilerplate
type CheckWithFields interface {
	GetTimeout() time.Duration
	GetProxyDelay() time.Duration
	GetProxyBreakWithCode() int
	GetFrom() string
	GetUntil() string
	GetFromEpoch() int64
	GetUntilEpoch() int64
	GetName() string
}

func (c *PromLabelValuesCheck) GetTimeout() time.Duration    { return c.Timeout }
func (c *PromLabelValuesCheck) GetProxyDelay() time.Duration { return c.ProxyDelay }
func (c *PromLabelValuesCheck) GetProxyBreakWithCode() int   { return c.ProxyBreakWithCode }
func (c *PromLabelValuesCheck) GetFrom() string              { return c.From }
func (c *PromLabelValuesCheck) GetUntil() string             { return c.Until }
func (c *PromLabelValuesCheck) GetFromEpoch() int64          { return c.from }
func (c *PromLabelValuesCheck) GetUntilEpoch() int64         { return c.until }
func (c *PromLabelValuesCheck) GetName() string              { return c.Name }

func (c *PromLabelNamesCheck) GetTimeout() time.Duration    { return c.Timeout }
func (c *PromLabelNamesCheck) GetProxyDelay() time.Duration { return c.ProxyDelay }
func (c *PromLabelNamesCheck) GetProxyBreakWithCode() int   { return c.ProxyBreakWithCode }
func (c *PromLabelNamesCheck) GetFrom() string              { return c.From }
func (c *PromLabelNamesCheck) GetUntil() string             { return c.Until }
func (c *PromLabelNamesCheck) GetFromEpoch() int64          { return c.from }
func (c *PromLabelNamesCheck) GetUntilEpoch() int64         { return c.until }
func (c *PromLabelNamesCheck) GetName() string              { return c.Name }

func (c *PromSeriesCheck) GetTimeout() time.Duration    { return c.Timeout }
func (c *PromSeriesCheck) GetProxyDelay() time.Duration { return c.ProxyDelay }
func (c *PromSeriesCheck) GetProxyBreakWithCode() int   { return c.ProxyBreakWithCode }
func (c *PromSeriesCheck) GetFrom() string              { return c.From }
func (c *PromSeriesCheck) GetUntil() string             { return c.Until }
func (c *PromSeriesCheck) GetFromEpoch() int64          { return c.from }
func (c *PromSeriesCheck) GetUntilEpoch() int64         { return c.until }
func (c *PromSeriesCheck) GetName() string              { return c.Name }

func (c *PromQueryCheck) GetTimeout() time.Duration    { return c.Timeout }
func (c *PromQueryCheck) GetProxyDelay() time.Duration { return c.ProxyDelay }
func (c *PromQueryCheck) GetProxyBreakWithCode() int   { return c.ProxyBreakWithCode }
func (c *PromQueryCheck) GetFrom() string              { return c.From }
func (c *PromQueryCheck) GetUntil() string             { return c.Until }
func (c *PromQueryCheck) GetFromEpoch() int64          { return c.from }
func (c *PromQueryCheck) GetUntilEpoch() int64         { return c.until }
func (c *PromQueryCheck) GetName() string              { return c.Name }

func (c *PromQueryRangeCheck) GetTimeout() time.Duration    { return c.Timeout }
func (c *PromQueryRangeCheck) GetProxyDelay() time.Duration { return c.ProxyDelay }
func (c *PromQueryRangeCheck) GetProxyBreakWithCode() int   { return c.ProxyBreakWithCode }
func (c *PromQueryRangeCheck) GetFrom() string              { return c.From }
func (c *PromQueryRangeCheck) GetUntil() string             { return c.Until }
func (c *PromQueryRangeCheck) GetFromEpoch() int64          { return c.from }
func (c *PromQueryRangeCheck) GetUntilEpoch() int64         { return c.until }
func (c *PromQueryRangeCheck) GetName() string              { return c.Name }

// executePrometheusChecks handles the common verification pattern for all Prometheus check types
func executePrometheusChecks(
	test *TestSchema,
	clickhouse *Clickhouse,
	gch *GraphiteClickhouse,
	checks interface{},
	checkType string,
	getLogFields func(interface{}) (string, []zap.Field),
	clickhouseDir string,
	logger *zap.Logger,
	breakOnError bool,
	verbose bool,
) {
	// Use reflection to handle different check types
	var (
		checksSlice reflect.Value
	)

	// Convert checks to reflect.Value
	checksValue := reflect.ValueOf(checks)
	if checksValue.Kind() == reflect.Ptr {
		checksValue = checksValue.Elem()
	}

	if checksValue.Kind() != reflect.Slice {
		return
	}

	checksSlice = checksValue
	totalChecks := checksSlice.Len()

	for i := 0; i < totalChecks; i++ {
		check := checksSlice.Index(i).Interface().(CheckWithFields)

		// Set proxy settings
		test.Proxy.SetDelay(check.GetProxyDelay())
		test.Proxy.SetBreakStatusCode(check.GetProxyBreakWithCode())

		// Get verification function based on type
		var verifyFunc func(*Clickhouse, *GraphiteClickhouse, interface{}) []string

		switch check.(type) {
		case *PromLabelValuesCheck:
			verifyFunc = func(c *Clickhouse, g *GraphiteClickhouse, ch interface{}) []string {
				return verifyPromLabelValues(c, g, ch.(*PromLabelValuesCheck))
			}
		case *PromLabelNamesCheck:
			verifyFunc = func(c *Clickhouse, g *GraphiteClickhouse, ch interface{}) []string {
				return verifyPromLabelNames(c, g, ch.(*PromLabelNamesCheck))
			}
		case *PromSeriesCheck:
			verifyFunc = func(c *Clickhouse, g *GraphiteClickhouse, ch interface{}) []string {
				return verifyPromSeries(c, g, ch.(*PromSeriesCheck))
			}
		case *PromQueryCheck:
			verifyFunc = func(c *Clickhouse, g *GraphiteClickhouse, ch interface{}) []string {
				return verifyPromQuery(c, g, ch.(*PromQueryCheck))
			}
		case *PromQueryRangeCheck:
			verifyFunc = func(c *Clickhouse, g *GraphiteClickhouse, ch interface{}) []string {
				return verifyPromQueryRange(c, g, ch.(*PromQueryRangeCheck))
			}
		default:
			continue
		}

		// Execute verification
		errs := verifyFunc(clickhouse, gch, check)
		logMessage, extraFields := getLogFields(check)

		if len(errs) > 0 {
			// Verification failed
			for _, e := range errs {
				fmt.Fprintln(os.Stderr, e)
			}

			// Log error with common fields
			fields := []zap.Field{
				zap.String("config", test.name),
				zap.String("clickhouse version", clickhouse.Version),
				zap.String("clickhouse config", clickhouseDir),
				zap.String("graphite-clickhouse config", gch.ConfigTpl),
				zap.String("from_raw", check.GetFrom()),
				zap.String("until_raw", check.GetUntil()),
				zap.Int64("from", check.GetFromEpoch()),
				zap.Int64("until", check.GetUntilEpoch()),
				zap.String("name", check.GetName()+"["+strconv.Itoa(i)+"]"),
			}
			fields = append(fields, extraFields...)

			logger.Error(logMessage, fields...)

			if breakOnError {
				debug(test, clickhouse, gch)
			}
		} else if verbose {
			// Verification succeeded - log info
			fields := []zap.Field{
				zap.String("config", test.name),
				zap.String("clickhouse version", clickhouse.Version),
				zap.String("clickhouse config", clickhouseDir),
				zap.String("graphite-clickhouse config", gch.ConfigTpl),
				zap.String("from_raw", check.GetFrom()),
				zap.String("until_raw", check.GetUntil()),
				zap.Int64("from", check.GetFromEpoch()),
				zap.Int64("until", check.GetUntilEpoch()),
				zap.String("name", check.GetName()+"["+strconv.Itoa(i)+"]"),
			}
			fields = append(fields, extraFields...)

			logger.Info(logMessage, fields...)
		}
	}
}

func verifyPromQueryRange(clickhouse *Clickhouse, gch *GraphiteClickhouse, check *PromQueryRangeCheck) []string {
	var errs []string

	// Create Prometheus client
	promClient := NewPrometheusClient(fmt.Sprintf("http://%s", gch.PrometheusAddr()), gch.logger)

	// Make actual API call
	results, err := promClient.QueryRange(check.Query, check.from, check.until, check.Step)
	if err != nil {
		errs = append(errs, fmt.Sprintf("Prometheus range query failed: %v", err))
		return errs
	}

	// Compare with expected results
	if len(results) != len(check.Result) {
		errs = append(errs, fmt.Sprintf("Expected %d query results, got %d", len(check.Result), len(results)))
		return errs
	}

	// Check if all expected results are present
	for i, expected := range check.Result {
		if i >= len(results) {
			errs = append(errs, fmt.Sprintf("Missing result %d", i))
			continue
		}

		actual := results[i]
		if actual.Metric != expected.Metric {
			errs = append(errs, fmt.Sprintf("Expected metric '%s', got '%s'", expected.Metric, actual.Metric))
		}

		// Values should come in pairs: [timestamp1, value1, timestamp2, value2, ...]
		if len(actual.Values)%2 != 0 {
			errs = append(errs, fmt.Sprintf("Expected even number of values for metric '%s', got %d", actual.Metric, len(actual.Values)))
			continue
		}

		if len(expected.Values)%2 != 0 {
			errs = append(errs, fmt.Sprintf("Expected even number of values in test data for metric '%s', got %d", expected.Metric, len(expected.Values)))
			continue
		}

		// Compare each timestamp-value pair
		if len(actual.Values) != len(expected.Values) {
			errs = append(errs, fmt.Sprintf("Expected %d values for metric '%s', got %d", len(expected.Values), actual.Metric, len(actual.Values)))
			continue
		}

		for j := 0; j < len(actual.Values); j += 2 {
			// Compare timestamps (allow some tolerance)
			expectedTime, err1 := strconv.ParseInt(expected.Values[j], 10, 64)
			actualTime, err2 := strconv.ParseInt(actual.Values[j], 10, 64)
			if err1 == nil && err2 == nil && absInt64(expectedTime-actualTime) > 60 {
				errs = append(errs, fmt.Sprintf("Timestamp mismatch for metric '%s' at position %d: expected ~%s, got %s",
					expected.Metric, j/2, expected.Values[j], actual.Values[j]))
			}

			// Compare values
			expectedVal, err1 := strconv.ParseFloat(expected.Values[j+1], 64)
			actualVal, err2 := strconv.ParseFloat(actual.Values[j+1], 64)
			if err1 == nil && err2 == nil && math.Abs(expectedVal-actualVal) > 0.001 {
				errs = append(errs, fmt.Sprintf("Value mismatch for metric '%s' at position %d: expected %s, got %s",
					expected.Metric, j/2, expected.Values[j+1], actual.Values[j+1]))
			}
		}
	}

	return errs
}

// Init functions for Prometheus checks
func (check *PromLabelValuesCheck) Init(now time.Time, precision time.Duration) error {
	var err error

	check.from = datetime.DateParamToEpoch(check.From, time.UTC, now, precision)
	if check.from == 0 && check.From != "" {
		return fmt.Errorf("failed to parse 'from': invalid timestamp")
	}

	check.until = datetime.DateParamToEpoch(check.Until, time.UTC, now, precision)
	if check.until == 0 && check.Until != "" {
		return fmt.Errorf("failed to parse 'until': invalid timestamp")
	}

	if check.ErrorRegexp != "" {
		check.errorRegexp, err = regexp.Compile(check.ErrorRegexp)
		if err != nil {
			return fmt.Errorf("failed to compile error regexp: %w", err)
		}
	}

	return nil
}

func (check *PromLabelNamesCheck) Init(now time.Time, precision time.Duration) error {
	var err error

	check.from = datetime.DateParamToEpoch(check.From, time.UTC, now, precision)
	if check.from == 0 && check.From != "" {
		return fmt.Errorf("failed to parse 'from': invalid timestamp")
	}

	check.until = datetime.DateParamToEpoch(check.Until, time.UTC, now, precision)
	if check.until == 0 && check.Until != "" {
		return fmt.Errorf("failed to parse 'until': invalid timestamp")
	}

	if check.ErrorRegexp != "" {
		check.errorRegexp, err = regexp.Compile(check.ErrorRegexp)
		if err != nil {
			return fmt.Errorf("failed to compile error regexp: %w", err)
		}
	}

	return nil
}

func (check *PromSeriesCheck) Init(now time.Time, precision time.Duration) error {
	var err error

	check.from = datetime.DateParamToEpoch(check.From, time.UTC, now, precision)
	if check.from == 0 && check.From != "" {
		return fmt.Errorf("failed to parse 'from': invalid timestamp")
	}

	check.until = datetime.DateParamToEpoch(check.Until, time.UTC, now, precision)
	if check.until == 0 && check.Until != "" {
		return fmt.Errorf("failed to parse 'until': invalid timestamp")
	}

	if check.ErrorRegexp != "" {
		check.errorRegexp, err = regexp.Compile(check.ErrorRegexp)
		if err != nil {
			return fmt.Errorf("failed to compile error regexp: %w", err)
		}
	}

	return nil
}

func (check *PromQueryCheck) Init(now time.Time, precision time.Duration) error {
	var err error

	check.from = datetime.DateParamToEpoch(check.From, time.UTC, now, precision)
	if check.from == 0 && check.From != "" {
		return fmt.Errorf("failed to parse 'from': invalid timestamp")
	}

	check.until = datetime.DateParamToEpoch(check.Until, time.UTC, now, precision)
	if check.until == 0 && check.Until != "" {
		return fmt.Errorf("failed to parse 'until': invalid timestamp")
	}

	if check.ErrorRegexp != "" {
		check.errorRegexp, err = regexp.Compile(check.ErrorRegexp)
		if err != nil {
			return fmt.Errorf("failed to compile error regexp: %w", err)
		}
	}

	return nil
}

func (check *PromQueryRangeCheck) Init(now time.Time, precision time.Duration) error {
	var err error

	check.from = datetime.DateParamToEpoch(check.From, time.UTC, now, precision)
	if check.from == 0 && check.From != "" {
		return fmt.Errorf("failed to parse 'from': invalid timestamp")
	}

	check.until = datetime.DateParamToEpoch(check.Until, time.UTC, now, precision)
	if check.until == 0 && check.Until != "" {
		return fmt.Errorf("failed to parse 'until': invalid timestamp")
	}

	if check.Step != "" {
		var dur time.Duration
		dur, err = time.ParseDuration(check.Step)
		if err != nil {
			return fmt.Errorf("failed to parse 'step': %w", err)
		}
		check.step = int64(dur.Seconds())
	}

	if check.ErrorRegexp != "" {
		check.errorRegexp, err = regexp.Compile(check.ErrorRegexp)
		if err != nil {
			return fmt.Errorf("failed to compile error regexp: %w", err)
		}
	}

	return nil
}

func initTest(cfg *MainConfig, rootDir string, now time.Time, verbose, breakOnError bool, logger *zap.Logger) bool {
	tz, err := datetime.Timezone("")
	if err != nil {
		fmt.Printf("can't get timezone: %s\n", err.Error())
		os.Exit(1)
	}

	// prepare
	for n, m := range cfg.Test.Input {
		for i := range m.Points {
			m.Points[i].time = datetime.DateParamToEpoch(m.Points[i].Time, tz, now, cfg.Test.Precision)
			if m.Points[i].time == 0 {
				err = ErrTimestampInvalid
			}

			if err != nil {
				logger.Error("failed to read config",
					zap.String("config", cfg.Test.name),
					zap.Error(err),
					zap.String("input", m.Name),
					zap.Int("metric", n),
					zap.Int("point", i),
					zap.String("time", m.Points[i].Time),
				)

				return false
			}
		}
	}

	for n, find := range cfg.Test.FindChecks {
		if find.Timeout == 0 {
			find.Timeout = 10 * time.Second
		}

		find.from = datetime.DateParamToEpoch(find.From, tz, now, cfg.Test.Precision)
		if find.from == 0 && find.From != "" {
			err = ErrTimestampInvalid
		}

		if err != nil {
			logger.Error("failed to read config",
				zap.String("config", cfg.Test.name),
				zap.Error(err),
				zap.String("query", find.Query),
				zap.String("from", find.From),
				zap.Int("step", n),
			)

			return false
		}

		find.until = datetime.DateParamToEpoch(find.Until, tz, now, cfg.Test.Precision)
		if find.until == 0 && find.Until != "" {
			err = ErrTimestampInvalid
		}

		if err != nil {
			logger.Error("failed to read config",
				zap.String("config", cfg.Test.name),
				zap.Error(err),
				zap.String("query", find.Query),
				zap.String("until", find.Until),
				zap.Int("step", n),
			)

			return false
		}

		if find.ErrorRegexp != "" {
			find.errorRegexp = regexp.MustCompile(find.ErrorRegexp)
		}
	}

	for n, tags := range cfg.Test.TagsChecks {
		if tags.Timeout == 0 {
			tags.Timeout = 10 * time.Second
		}

		tags.from = datetime.DateParamToEpoch(tags.From, tz, now, cfg.Test.Precision)
		if tags.from == 0 && tags.From != "" {
			err = ErrTimestampInvalid
		}

		if err != nil {
			logger.Error("failed to read config",
				zap.String("config", cfg.Test.name),
				zap.Error(err),
				zap.String("query", tags.Query),
				zap.String("from", tags.From),
				zap.Int("find", n),
			)

			return false
		}

		tags.until = datetime.DateParamToEpoch(tags.Until, tz, now, cfg.Test.Precision)
		if tags.until == 0 && tags.Until != "" {
			err = ErrTimestampInvalid
		}

		if err != nil {
			logger.Error("failed to read config",
				zap.String("config", cfg.Test.name),
				zap.Error(err),
				zap.String("query", tags.Query),
				zap.String("until", tags.Until),
				zap.Int("tags", n),
				zap.Bool("names", tags.Names),
			)

			return false
		}

		if tags.ErrorRegexp != "" {
			tags.errorRegexp = regexp.MustCompile(tags.ErrorRegexp)
		}
	}

	for n, r := range cfg.Test.RenderChecks {
		if r.Timeout == 0 {
			r.Timeout = 10 * time.Second
		}

		r.from = datetime.DateParamToEpoch(r.From, tz, now, cfg.Test.Precision)
		if r.from == 0 && r.From != "" {
			err = ErrTimestampInvalid
		}

		if err != nil {
			logger.Error("failed to read config",
				zap.String("config", cfg.Test.name),
				zap.Error(err),
				zap.Strings("targets", r.Targets),
				zap.String("from", r.From),
				zap.Int("render", n),
			)

			return false
		}

		r.until = datetime.DateParamToEpoch(r.Until, tz, now, cfg.Test.Precision)
		if r.until == 0 && r.Until != "" {
			err = ErrTimestampInvalid
		}

		if err != nil {
			logger.Error("failed to read config",
				zap.String("config", cfg.Test.name),
				zap.Error(err),
				zap.Strings("targets", r.Targets),
				zap.String("until", r.Until),
				zap.Int("render", n),
			)

			return false
		}

		if r.ErrorRegexp != "" {
			r.errorRegexp = regexp.MustCompile(r.ErrorRegexp)
		}

		sort.Slice(r.Result, func(i, j int) bool {
			return r.Result[i].Name < r.Result[j].Name
		})

		r.result = make([]client.Metric, len(r.Result))
		for i, result := range r.Result {
			r.result[i].StartTime = datetime.DateParamToEpoch(result.StartTime, tz, now, cfg.Test.Precision)
			if r.result[i].StartTime == 0 && result.StartTime != "" {
				err = ErrTimestampInvalid
			}

			if err != nil {
				logger.Error("failed to read config",
					zap.String("config", cfg.Test.name),
					zap.Error(err),
					zap.Strings("targets", r.Targets),
					zap.Int("render", n),
					zap.String("metric", result.Name),
					zap.String("start", result.StartTime),
				)

				return false
			}

			r.result[i].StopTime = datetime.DateParamToEpoch(result.StopTime, tz, now, cfg.Test.Precision)
			if r.result[i].StopTime == 0 && result.StopTime != "" {
				err = ErrTimestampInvalid
			}

			if err != nil {
				logger.Error("failed to read config",
					zap.String("config", cfg.Test.name),
					zap.Error(err),
					zap.Strings("targets", r.Targets),
					zap.Int("render", n),
					zap.String("metric", result.Name),
					zap.String("stop", result.StopTime),
				)

				return false
			}

			r.result[i].RequestStartTime = datetime.DateParamToEpoch(result.RequestStartTime, tz, now, cfg.Test.Precision)
			if r.result[i].RequestStartTime == 0 && result.RequestStartTime != "" {
				err = ErrTimestampInvalid
			}

			if err != nil {
				logger.Error("failed to read config",
					zap.String("config", cfg.Test.name),
					zap.Error(err),
					zap.Strings("targets", r.Targets),
					zap.Int("render", n),
					zap.String("metric", result.Name),
					zap.String("req_start", result.RequestStartTime),
				)

				return false
			}

			r.result[i].RequestStopTime = datetime.DateParamToEpoch(result.RequestStopTime, tz, now, cfg.Test.Precision)
			if r.result[i].RequestStopTime == 0 && result.RequestStopTime != "" {
				err = ErrTimestampInvalid
			}

			if err != nil {
				logger.Error("failed to read config",
					zap.String("config", cfg.Test.name),
					zap.Error(err),
					zap.Strings("targets", r.Targets),
					zap.Int("render", n),
					zap.String("metric", result.Name),
					zap.String("req_stop", result.RequestStopTime),
				)

				return false
			}

			r.result[i].StepTime = result.StepTime
			r.result[i].Name = result.Name
			r.result[i].PathExpression = result.PathExpression
			r.result[i].ConsolidationFunc = result.ConsolidationFunc
			r.result[i].XFilesFactor = result.XFilesFactor
			r.result[i].HighPrecisionTimestamps = result.HighPrecisionTimestamps
			r.result[i].AppliedFunctions = result.AppliedFunctions
			r.result[i].Values = result.Values
		}
	}

	// Initialize Prometheus checks
	for n, check := range cfg.Test.PromLabelValuesChecks {
		if check.Timeout == 0 {
			check.Timeout = 10 * time.Second
		}

		check.from = datetime.DateParamToEpoch(check.From, tz, now, cfg.Test.Precision)
		if check.from == 0 && check.From != "" {
			err = ErrTimestampInvalid
		}

		if err != nil {
			logger.Error("failed to read config",
				zap.String("config", cfg.Test.name),
				zap.Error(err),
				zap.String("label", check.Label),
				zap.String("from", check.From),
				zap.Int("label_values", n),
			)

			return false
		}

		check.until = datetime.DateParamToEpoch(check.Until, tz, now, cfg.Test.Precision)
		if check.until == 0 && check.Until != "" {
			err = ErrTimestampInvalid
		}

		if err != nil {
			logger.Error("failed to read config",
				zap.String("config", cfg.Test.name),
				zap.Error(err),
				zap.String("label", check.Label),
				zap.String("until", check.Until),
				zap.Int("label_values", n),
			)

			return false
		}

		if check.ErrorRegexp != "" {
			check.errorRegexp = regexp.MustCompile(check.ErrorRegexp)
		}
	}

	for n, check := range cfg.Test.PromLabelNamesChecks {
		if check.Timeout == 0 {
			check.Timeout = 10 * time.Second
		}

		check.from = datetime.DateParamToEpoch(check.From, tz, now, cfg.Test.Precision)
		if check.from == 0 && check.From != "" {
			err = ErrTimestampInvalid
		}

		if err != nil {
			logger.Error("failed to read config",
				zap.String("config", cfg.Test.name),
				zap.Error(err),
				zap.String("from", check.From),
				zap.Int("label_names", n),
			)

			return false
		}

		check.until = datetime.DateParamToEpoch(check.Until, tz, now, cfg.Test.Precision)
		if check.until == 0 && check.Until != "" {
			err = ErrTimestampInvalid
		}

		if err != nil {
			logger.Error("failed to read config",
				zap.String("config", cfg.Test.name),
				zap.Error(err),
				zap.String("until", check.Until),
				zap.Int("label_names", n),
			)

			return false
		}

		if check.ErrorRegexp != "" {
			check.errorRegexp = regexp.MustCompile(check.ErrorRegexp)
		}
	}

	for n, check := range cfg.Test.PromSeriesChecks {
		if check.Timeout == 0 {
			check.Timeout = 10 * time.Second
		}

		check.from = datetime.DateParamToEpoch(check.From, tz, now, cfg.Test.Precision)
		if check.from == 0 && check.From != "" {
			err = ErrTimestampInvalid
		}

		if err != nil {
			logger.Error("failed to read config",
				zap.String("config", cfg.Test.name),
				zap.Error(err),
				zap.Strings("matchers", check.Matchers),
				zap.String("from", check.From),
				zap.Int("series", n),
			)

			return false
		}

		check.until = datetime.DateParamToEpoch(check.Until, tz, now, cfg.Test.Precision)
		if check.until == 0 && check.Until != "" {
			err = ErrTimestampInvalid
		}

		if err != nil {
			logger.Error("failed to read config",
				zap.String("config", cfg.Test.name),
				zap.Error(err),
				zap.Strings("matchers", check.Matchers),
				zap.String("until", check.Until),
				zap.Int("series", n),
			)

			return false
		}

		if check.ErrorRegexp != "" {
			check.errorRegexp = regexp.MustCompile(check.ErrorRegexp)
		}
	}

	for n, check := range cfg.Test.PromQueryChecks {
		if check.Timeout == 0 {
			check.Timeout = 10 * time.Second
		}

		check.from = datetime.DateParamToEpoch(check.From, tz, now, cfg.Test.Precision)
		if check.from == 0 && check.From != "" {
			err = ErrTimestampInvalid
		}

		if err != nil {
			logger.Error("failed to read config",
				zap.String("config", cfg.Test.name),
				zap.Error(err),
				zap.String("query", check.Query),
				zap.String("from", check.From),
				zap.Int("query", n),
			)

			return false
		}

		check.until = datetime.DateParamToEpoch(check.Until, tz, now, cfg.Test.Precision)
		if check.until == 0 && check.Until != "" {
			err = ErrTimestampInvalid
		}

		if err != nil {
			logger.Error("failed to read config",
				zap.String("config", cfg.Test.name),
				zap.Error(err),
				zap.String("query", check.Query),
				zap.String("until", check.Until),
				zap.Int("query", n),
			)

			return false
		}

		if check.ErrorRegexp != "" {
			check.errorRegexp = regexp.MustCompile(check.ErrorRegexp)
		}
	}

	for n, check := range cfg.Test.PromQueryRangeChecks {
		if check.Timeout == 0 {
			check.Timeout = 10 * time.Second
		}

		check.from = datetime.DateParamToEpoch(check.From, tz, now, cfg.Test.Precision)
		if check.from == 0 && check.From != "" {
			err = ErrTimestampInvalid
		}

		if err != nil {
			logger.Error("failed to read config",
				zap.String("config", cfg.Test.name),
				zap.Error(err),
				zap.String("query", check.Query),
				zap.String("from", check.From),
				zap.Int("query_range", n),
			)

			return false
		}

		check.until = datetime.DateParamToEpoch(check.Until, tz, now, cfg.Test.Precision)
		if check.until == 0 && check.Until != "" {
			err = ErrTimestampInvalid
		}

		if err != nil {
			logger.Error("failed to read config",
				zap.String("config", cfg.Test.name),
				zap.Error(err),
				zap.String("query", check.Query),
				zap.String("until", check.Until),
				zap.Int("query_range", n),
			)

			return false
		}

		if check.Step != "" {
			var err error
			var dur time.Duration
			dur, err = time.ParseDuration(check.Step)
			if err != nil {
				logger.Error("failed to read config",
					zap.String("config", cfg.Test.name),
					zap.Error(err),
					zap.String("query", check.Query),
					zap.String("step", check.Step),
					zap.Int("query_range", n),
				)

				return false
			}
			check.step = int64(dur.Seconds())
		}

		if check.ErrorRegexp != "" {
			check.errorRegexp = regexp.MustCompile(check.ErrorRegexp)
		}
	}

	return true
}

func loadConfig(config string, rootDir string) (*MainConfig, error) {
	d, err := os.ReadFile(config)
	if err != nil {
		return nil, err
	}

	confShort := strings.ReplaceAll(config, rootDir+"/", "")

	var cfg = &MainConfig{}
	if err := toml.Unmarshal(d, cfg); err != nil {
		return nil, err
	}

	cfg.Test.name = confShort
	cfg.Test.dir = path.Dir(config)

	if cfg.Test == nil {
		return nil, ErrNoTest
	}

	cfg.Test.chVersions = make(map[string]bool)
	for i := range cfg.Test.Clickhouse {
		if err := cfg.Test.Clickhouse[i].CheckConfig(rootDir); err == nil {
			cfg.Test.chVersions[cfg.Test.Clickhouse[i].Key()] = true
		} else {
			return nil, fmt.Errorf("[%d] %s", i, err.Error())
		}
	}

	return cfg, nil
}
