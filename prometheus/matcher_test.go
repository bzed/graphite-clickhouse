//go:build !noprom
// +build !noprom

package prometheus

import (
	"testing"

	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
)

func TestMakeTaggedFromPromPB(t *testing.T) {
	tests := []struct {
		name     string
		matchers []*prompb.LabelMatcher
		want     []finder.TaggedTerm
		wantErr  bool
	}{
		{
			name:     "empty matchers",
			matchers: []*prompb.LabelMatcher{},
			want:     []finder.TaggedTerm{},
		},
		{
			name: "single equal matcher",
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "cpu_usage_system",
				},
			},
			want: []finder.TaggedTerm{
				{
					Key:   "__name__",
					Value: "cpu_usage_system",
					Op:    finder.TaggedTermEq,
				},
			},
		},
		{
			name: "multiple matchers",
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "cpu_usage_system",
				},
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "host",
					Value: "server.*",
				},
				{
					Type:  prompb.LabelMatcher_NEQ,
					Name:  "instance",
					Value: "localhost",
				},
			},
			want: []finder.TaggedTerm{
				{
					Key:   "__name__",
					Value: "cpu_usage_system",
					Op:    finder.TaggedTermEq,
				},
				{
					Key:   "host",
					Value: "server.*",
					Op:    finder.TaggedTermMatch,
				},
				{
					Key:   "instance",
					Value: "localhost",
					Op:    finder.TaggedTermNe,
				},
			},
		},
		{
			name: "unknown matcher type",
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_Type(999), // Invalid type
					Name:  "__name__",
					Value: "test",
				},
			},
			wantErr: true,
		},
		{
			name: "nil matcher",
			matchers: []*prompb.LabelMatcher{
				nil,
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "cpu_usage_system",
				},
			},
			want: []finder.TaggedTerm{
				{
					Key:   "__name__",
					Value: "cpu_usage_system",
					Op:    finder.TaggedTermEq,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := makeTaggedFromPromPB(tt.matchers)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
		})
	}
}

func TestMakeTaggedFromPromQL(t *testing.T) {
	tests := []struct {
		name     string
		matchers []*labels.Matcher
		want     []finder.TaggedTerm
		wantErr  bool
	}{
		{
			name:     "empty matchers",
			matchers: []*labels.Matcher{},
			want:     []finder.TaggedTerm{},
		},
		{
			name: "single equal matcher",
			matchers: []*labels.Matcher{
				{
					Type:  labels.MatchEqual,
					Name:  "__name__",
					Value: "cpu_usage_system",
				},
			},
			want: []finder.TaggedTerm{
				{
					Key:   "__name__",
					Value: "cpu_usage_system",
					Op:    finder.TaggedTermEq,
				},
			},
		},
		{
			name: "multiple matchers",
			matchers: []*labels.Matcher{
				{
					Type:  labels.MatchEqual,
					Name:  "__name__",
					Value: "cpu_usage_system",
				},
				{
					Type:  labels.MatchRegexp,
					Name:  "host",
					Value: "server.*",
				},
				{
					Type:  labels.MatchNotEqual,
					Name:  "instance",
					Value: "localhost",
				},
				{
					Type:  labels.MatchNotRegexp,
					Name:  "job",
					Value: "telegraf.*",
				},
			},
			want: []finder.TaggedTerm{
				{
					Key:   "__name__",
					Value: "cpu_usage_system",
					Op:    finder.TaggedTermEq,
				},
				{
					Key:   "host",
					Value: "server.*",
					Op:    finder.TaggedTermMatch,
				},
				{
					Key:   "instance",
					Value: "localhost",
					Op:    finder.TaggedTermNe,
				},
				{
					Key:   "job",
					Value: "telegraf.*",
					Op:    finder.TaggedTermNotMatch,
				},
			},
		},
		{
			name: "unknown matcher type",
			matchers: []*labels.Matcher{
				{
					Type:  labels.MatchType(999), // Invalid type
					Name:  "__name__",
					Value: "test",
				},
			},
			wantErr: true,
		},
		{
			name: "nil matcher",
			matchers: []*labels.Matcher{
				nil,
				{
					Type:  labels.MatchEqual,
					Name:  "__name__",
					Value: "cpu_usage_system",
				},
			},
			want: []finder.TaggedTerm{
				{
					Key:   "__name__",
					Value: "cpu_usage_system",
					Op:    finder.TaggedTermEq,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := makeTaggedFromPromQL(tt.matchers)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
		})
	}
}