package main

import (
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

var (
	reNonAlnum   = regexp.MustCompile(`[^a-z0-9]+`)
	reEdgeHyphen = regexp.MustCompile(`^-+|-+$`)
)

func slugify(s string) string {
	s = norm.NFKD.String(s)
	var b strings.Builder
	for _, r := range s {
		if r < 128 && unicode.IsPrint(r) {
			b.WriteRune(r)
		}
	}
	s = strings.ToLower(b.String())
	s = reNonAlnum.ReplaceAllString(s, "-")
	s = reEdgeHyphen.ReplaceAllString(s, "")
	if len(s) > 80 {
		// Truncate at a hyphen boundary if possible.
		cut := strings.LastIndex(s[:80], "-")
		if cut > 40 {
			s = s[:cut]
		} else {
			s = s[:80]
		}
	}
	if s == "" {
		s = "no-subject"
	}
	return s
}

// Format: 2026-02-15T14:30:00-<subject-slug>-<msgid>.eml
func emlFilename(t time.Time, subject, msgID string) string {
	ts := t.Format("2006-01-02T15:04:05")
	slug := slugify(subject)
	return fmt.Sprintf("%s-%s-%s.eml", ts, slug, msgID)
}

// Slashes are replaced with "--" to avoid directory nesting.
func sanitizeLabelName(name string) string {
	name = strings.ReplaceAll(name, "/", "--")
	var b strings.Builder
	for _, r := range name {
		switch r {
		case '/', '\x00':
			continue
		default:
			b.WriteRune(r)
		}
	}
	result := b.String()
	if result == "" {
		result = "_unnamed_"
	}
	return result
}
