package main

import (
	"testing"
)

func TestParsePartsRange(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    partsRange
		wantErr bool
	}{
		{
			name:    "range 0-10",
			input:   "0-10",
			want:    partsRange{lo: 0, hi: 10},
			wantErr: false,
		},
		{
			name:    "single value 7",
			input:   "7",
			want:    partsRange{lo: 7, hi: 7},
			wantErr: false,
		},
		{
			name:    "empty string",
			input:   "",
			want:    partsRange{},
			wantErr: true,
		},
		{
			name:    "invalid range 5-2 (hi < lo)",
			input:   "5-2",
			want:    partsRange{},
			wantErr: true,
		},
		{
			name:    "non-numeric a-b",
			input:   "a-b",
			want:    partsRange{},
			wantErr: true,
		},
		{
			name:    "negative number -1-2",
			input:   "-1-2",
			want:    partsRange{},
			wantErr: true,
		},
		{
			name:    "overflow 0-4294967296",
			input:   "0-4294967296",
			want:    partsRange{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parsePartsRange(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parsePartsRange() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("parsePartsRange() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValidateRunnerOpts(t *testing.T) {
	tests := []struct {
		name    string
		opts    runnerOpts
		wantErr bool
	}{
		{
			name:    "valid warcParallel",
			opts:    runnerOpts{parts: partsRange{lo: 0, hi: 10}, warcParallel: 4},
			wantErr: false,
		},
		{
			name:    "valid warcParallel minimum",
			opts:    runnerOpts{parts: partsRange{lo: 0, hi: 10}, warcParallel: 1},
			wantErr: false,
		},
		{
			name:    "zero warcParallel",
			opts:    runnerOpts{parts: partsRange{lo: 0, hi: 10}, warcParallel: 0},
			wantErr: true,
		},
		{
			name:    "negative warcParallel",
			opts:    runnerOpts{parts: partsRange{lo: 0, hi: 10}, warcParallel: -1},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateRunnerOpts(tt.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateRunnerOpts() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
