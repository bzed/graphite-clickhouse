//go:build !noprom
// +build !noprom

package prometheus

import (
	"context"
	"testing"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"
)

// TestEmptyIterator tests the empty iterator implementation
func TestEmptyIterator(t *testing.T) {
	it := &emptyIterator{}

	// Test Next() - should always return ValNone
	result := it.Next()
	require.Equal(t, chunkenc.ValNone, result)

	// Test Seek() - should always return ValNone
	result = it.Seek(123456789)
	require.Equal(t, chunkenc.ValNone, result)

	// Test At() - should return zero values
	ts, val := it.At()
	require.Equal(t, int64(0), ts)
	require.Equal(t, float64(0), val)

	// Test AtHistogram() - should return zero values
	hist := &histogram.Histogram{}
	ts, histResult := it.AtHistogram(hist)
	require.Equal(t, int64(0), ts)
	require.Nil(t, histResult)

	// Test AtFloatHistogram() - should return zero values
	floatHist := &histogram.FloatHistogram{}
	ts, floatHistResult := it.AtFloatHistogram(floatHist)
	require.Equal(t, int64(0), ts)
	require.Nil(t, floatHistResult)

	// Test AtT() - should return zero timestamp
	ts = it.AtT()
	require.Equal(t, int64(0), ts)

	// Test Err() - should return nil error
	err := it.Err()
	require.NoError(t, err)
}

// TestNopExemplarQueryable tests the exemplar queryable implementation
func TestNopExemplarQueryable(t *testing.T) {
	ex := &nopExemplarQueryable{}

	// Test ExemplarQuerier() - should return nopExemplarQuerier
	querier, err := ex.ExemplarQuerier(context.Background())
	require.NoError(t, err)
	require.NotNil(t, querier)

	exq, ok := querier.(*nopExemplarQuerier)
	require.True(t, ok)

	// Test Select() - should return empty results
	results, err := exq.Select(123456789, 987654321)
	require.NoError(t, err)
	require.Empty(t, results)
}

// TestNopGatherer tests the gatherer implementation
func TestNopGatherer(t *testing.T) {
	g := &nopGatherer{}

	// Test Gather() - should return empty metric families
	metrics, err := g.Gather()
	require.NoError(t, err)
	require.NotNil(t, metrics)
	require.Empty(t, metrics)
}

// TestMetricsSet tests the metrics set implementation
func TestMetricsSet(t *testing.T) {
	metrics := []string{
		"cpu_usage_system?host=server1&instance=localhost",
		"memory_usage?host=server2&instance=localhost",
	}

	ms := newMetricsSet(metrics).(*metricsSet)

	// Test initial state
	require.Equal(t, -1, ms.current)
	require.Equal(t, metrics, ms.metrics)

	// Test Next() - should iterate through metrics
	require.True(t, ms.Next())
	require.Equal(t, 0, ms.current)

	require.True(t, ms.Next())
	require.Equal(t, 1, ms.current)

	require.False(t, ms.Next())
	require.Equal(t, 2, ms.current)

	// Test At() - should return current metric
	ms.current = 0
	metric := ms.At()
	require.NotNil(t, metric)

	// Test Iterator() - should return empty iterator
	it := metric.Iterator(nil)
	require.Equal(t, emptyIteratorValue, it)

	// Test Labels() - should parse metric name
	metricLabels := metric.Labels()
	require.NotNil(t, metricLabels)

	// Test Err() - should return nil
	err := ms.Err()
	require.NoError(t, err)

	// Test Warnings() - should return nil
	warnings := ms.Warnings()
	require.Nil(t, warnings)
}

// TestSeriesSet tests the series set implementation
func TestSeriesSet(t *testing.T) {
	// Test empty series set
	ss := emptySeriesSet()
	require.NotNil(t, ss)

	// Should be able to cast to seriesSet
	set, ok := ss.(*seriesSet)
	require.True(t, ok)
	require.Equal(t, 0, len(set.series))
}

// TestLocalStorage tests the local storage implementation
func TestLocalStorage(t *testing.T) {
	cfg := &config.Config{}
	s := newStorage(cfg)

	// Test CleanTombstones() - should return nil
	err := s.CleanTombstones()
	require.NoError(t, err)

	// Test Delete() - should return nil
	ctx := context.Background()
	err = s.Delete(ctx, 0, 0)
	require.NoError(t, err)

	// Test Snapshot() - should return error for unsupported operation
	err = s.Snapshot("/tmp", true)
	require.NoError(t, err)

	// Test Stats() - should return stats with empty postings
	stats, err := s.Stats("", 0)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.NotNil(t, stats.IndexPostingStats)

	// Test WALReplayStatus() - should return zero status
	status, err := s.WALReplayStatus()
	require.NoError(t, err)
	require.Equal(t, tsdb.WALReplayStatus{}, status)
}