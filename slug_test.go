package main

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSlugify(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"Hello World", "hello-world"},
		{"Re: Meeting Tomorrow!", "re-meeting-tomorrow"},
		{"", "no-subject"},
		{"café résumé", "cafe-resume"},
		{"---leading---", "leading"},
		{"a " + string(make([]byte, 200)), "a"},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			require.Equal(t, tt.want, slugify(tt.in))
		})
	}
}

func TestSlugifyTruncation(t *testing.T) {
	var b strings.Builder
	for i := range 20 {
		if i > 0 {
			b.WriteString("-")
		}
		b.WriteString("abcde")
	}
	long := b.String()
	got := slugify(long)
	require.LessOrEqual(t, len(got), 80)
}

func TestEmlFilename(t *testing.T) {
	ts := time.Date(2026, 2, 27, 9, 15, 0, 0, time.UTC)
	got := emlFilename(ts, "Hello World", "msg123")
	require.Equal(t, "2026-02-27T09:15:00-hello-world-msg123.eml", got)
}

func TestSanitizeLabelName(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"Inbox", "Inbox"},
		{"Work/Projects", "Work--Projects"},
		{"", "_unnamed_"},
		{"has\x00null", "hasnull"},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			require.Equal(t, tt.want, sanitizeLabelName(tt.in))
		})
	}
}
