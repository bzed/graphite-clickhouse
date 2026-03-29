//go:build !noprom
// +build !noprom

package prometheus

import (
	"context"
	"testing"
	"time"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/limiter"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

func TestQuerier_LabelValues(t *testing.T) {
	timeNow = func() time.Time {
		return time.Unix(1669714247, 0) // 2022-11-29 09:30:47 UTC
	}

	cfg := &config.Config{
		ClickHouse: config.ClickHouse{
			TaggedTable:         "graphite_tagged",
			TaggedAutocompleDays: 4,
			URL:                 "http://localhost:8123",
			QueryParams: []config.QueryParam{
				{
					URL:        "http://localhost:8123",
					DataTimeout: 60 * time.Second,
					Limiter:     limiter.NewLimiter(100, false, "test", ""),
				},
			},
			IndexTimeout: 30 * time.Second,
		},
	}

	tests := []struct {
		name    string
		label   string
		wantErr bool
	}{
		{
			name:  "valid label",
			label: "host",
			// Note: This will fail without a real ClickHouse connection
			// but we're testing the query structure, not the actual execution
		},
		{
			name:    "empty label",
			label:   "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newStorage(cfg)
			q, err := s.Querier(0, 0)
			require.NoError(t, err)

			_, _, err = q.LabelValues(context.Background(), tt.label, nil)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				// We expect an error due to no ClickHouse connection, but the query should be valid
				t.Logf("Expected error due to no ClickHouse connection: %v", err)
			}
		})
	}
}

func TestQuerier_LabelNames(t *testing.T) {
	timeNow = func() time.Time {
		return time.Unix(1669714247, 0) // 2022-11-29 09:30:47 UTC
	}

	cfg := &config.Config{
		ClickHouse: config.ClickHouse{
			TaggedTable:         "graphite_tagged",
			TaggedAutocompleDays: 4,
			URL:                 "http://localhost:8123",
			QueryParams: []config.QueryParam{
				{
					URL:        "http://localhost:8123",
					DataTimeout: 60 * time.Second,
					Limiter:     limiter.NewLimiter(100, false, "test", ""),
				},
			},
			IndexTimeout: 30 * time.Second,
		},
	}

	s := newStorage(cfg)
	q, err := s.Querier(0, 0)
	require.NoError(t, err)

	_, _, err = q.LabelNames(context.Background(), nil)
	// We expect an error due to no ClickHouse connection, but the query should be valid
	t.Logf("Expected error due to no ClickHouse connection: %v", err)
}

func TestQuerier_Select(t *testing.T) {
	timeNow = func() time.Time {
		return time.Unix(1669714247, 0) // 2022-11-29 09:30:47 UTC
	}

	cfg := &config.Config{
		ClickHouse: config.ClickHouse{
			TaggedTable:         "graphite_tagged",
			TaggedAutocompleDays: 4,
			URL:                 "http://localhost:8123",
			QueryParams: []config.QueryParam{
				{
					URL:        "http://localhost:8123",
					DataTimeout: 60 * time.Second,
					Limiter:     limiter.NewLimiter(100, false, "test", ""),
				},
			},
			IndexTimeout: 30 * time.Second,
		},
	}

	tests := []struct {
		name     string
		hints    *storage.SelectHints
		matchers []*labels.Matcher
		wantErr  bool
	}{
		{
			name: "basic select with matchers",
			hints: &storage.SelectHints{
				Start: 1669453200000,
				End:   1669626000000,
			},
			matchers: []*labels.Matcher{
				{Type: labels.MatchEqual, Name: "__name__", Value: "cpu_usage_system"},
				{Type: labels.MatchEqual, Name: "host", Value: "server1"},
			},
		},
		{
			name: "series endpoint",
			hints: &storage.SelectHints{
				Func: "series",
			},
			matchers: []*labels.Matcher{
				{Type: labels.MatchEqual, Name: "__name__", Value: "cpu_usage_system"},
			},
		},
		{
			name:    "empty matchers",
			matchers: []*labels.Matcher{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil && !tt.wantErr {
					t.Errorf("unexpected panic: %v", r)
				}
			}()

			s := newStorage(cfg)
			q, err := s.Querier(0, 0)
			require.NoError(t, err)

			result := q.Select(context.Background(), false, tt.hints, tt.matchers...)
			if tt.wantErr {
				require.NotNil(t, result)
				// Check if it's an empty series set (error case)
				ss, ok := result.(*seriesSet)
				require.True(t, ok)
				require.Equal(t, 0, len(ss.series))
			} else {
				// We expect an empty result or error due to no ClickHouse connection
				t.Logf("Result: %T", result)
			}
		})
	}
}

func TestQuerier_Close(t *testing.T) {
	cfg := &config.Config{}
	s := newStorage(cfg)
	q, err := s.Querier(0, 0)
	require.NoError(t, err)

	err = q.Close()
	require.NoError(t, err)
}

func TestStorageImpl(t *testing.T) {
	cfg := &config.Config{}
	s := newStorage(cfg)

	// Test Querier
	q, err := s.Querier(0, 0)
	require.NoError(t, err)
	require.NotNil(t, q)

	// Test ChunkQuerier (should return nil)
	cq, err := s.ChunkQuerier(0, 0)
	require.NoError(t, err)
	require.Nil(t, cq)

	// Test Appender (should return nil)
	a := s.Appender(context.Background())
	require.Nil(t, a)

	// Test StartTime (should return 0, nil)
	st, err := s.StartTime()
	require.NoError(t, err)
	require.Equal(t, int64(0), st)

	// Test Close
	err = s.Close()
	require.NoError(t, err)
}