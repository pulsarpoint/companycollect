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
		cmd     string
		opts    runnerOpts
		wantErr bool
		errMsg  string
	}{
		// Valid cases for "tech"
		{
			name: "valid tech with remote mode",
			cmd:  "tech",
			opts: runnerOpts{
				parts:            partsRange{lo: 0, hi: 10},
				mode:             "remote",
				remoteMaxPages:   100,
				warcParallel:     4,
				downloadParallel: 2,
				processParallel:  2,
				maxWARCFiles:     5,
			},
			wantErr: false,
		},
		{
			name: "valid tech with local mode",
			cmd:  "tech",
			opts: runnerOpts{
				parts:            partsRange{lo: 0, hi: 10},
				mode:             "local",
				remoteMaxPages:   100,
				warcParallel:     4,
				downloadParallel: 2,
				processParallel:  2,
				maxWARCFiles:     5,
			},
			wantErr: false,
		},
		// Valid cases for "both"
		{
			name: "valid both with remote mode",
			cmd:  "both",
			opts: runnerOpts{
				parts:            partsRange{lo: 0, hi: 10},
				mode:             "remote",
				remoteMaxPages:   100,
				warcParallel:     4,
				downloadParallel: 2,
				processParallel:  2,
				maxWARCFiles:     5,
			},
			wantErr: false,
		},
		{
			name: "valid both with local mode",
			cmd:  "both",
			opts: runnerOpts{
				parts:            partsRange{lo: 0, hi: 10},
				mode:             "local",
				remoteMaxPages:   100,
				warcParallel:     4,
				downloadParallel: 2,
				processParallel:  2,
				maxWARCFiles:     5,
			},
			wantErr: false,
		},
		// Valid cases for "industry"
		{
			name: "valid industry with remote mode",
			cmd:  "industry",
			opts: runnerOpts{
				parts:            partsRange{lo: 0, hi: 10},
				mode:             "remote",
				remoteMaxPages:   100,
				warcParallel:     4,
				downloadParallel: 2,
				processParallel:  2,
				maxWARCFiles:     5,
			},
			wantErr: false,
		},
		// Valid cases for "embed"
		{
			name: "valid embed with remote mode",
			cmd:  "embed",
			opts: runnerOpts{
				parts:            partsRange{lo: 0, hi: 10},
				mode:             "remote",
				remoteMaxPages:   100,
				warcParallel:     4,
				downloadParallel: 2,
				processParallel:  2,
				maxWARCFiles:     5,
			},
			wantErr: false,
		},
		// Error case: industry with local mode
		{
			name: "error industry with local mode",
			cmd:  "industry",
			opts: runnerOpts{
				parts:            partsRange{lo: 0, hi: 10},
				mode:             "local",
				remoteMaxPages:   100,
				warcParallel:     4,
				downloadParallel: 2,
				processParallel:  2,
				maxWARCFiles:     5,
			},
			wantErr: true,
			errMsg:  "industry/embed selections are sparse; only --mode remote is supported",
		},
		// Error case: embed with local mode
		{
			name: "error embed with local mode",
			cmd:  "embed",
			opts: runnerOpts{
				parts:            partsRange{lo: 0, hi: 10},
				mode:             "local",
				remoteMaxPages:   100,
				warcParallel:     4,
				downloadParallel: 2,
				processParallel:  2,
				maxWARCFiles:     5,
			},
			wantErr: true,
			errMsg:  "industry/embed selections are sparse; only --mode remote is supported",
		},
		// Error case: invalid mode
		{
			name: "error invalid mode",
			cmd:  "tech",
			opts: runnerOpts{
				parts:            partsRange{lo: 0, hi: 10},
				mode:             "invalid",
				remoteMaxPages:   100,
				warcParallel:     4,
				downloadParallel: 2,
				processParallel:  2,
				maxWARCFiles:     5,
			},
			wantErr: true,
		},
		// Error case: tech with local mode but missing maxWARCFiles
		{
			name: "error tech local missing maxWARCFiles",
			cmd:  "tech",
			opts: runnerOpts{
				parts:            partsRange{lo: 0, hi: 10},
				mode:             "local",
				remoteMaxPages:   100,
				warcParallel:     4,
				downloadParallel: 2,
				processParallel:  2,
				maxWARCFiles:     0,
			},
			wantErr: true,
		},
		// Error case: tech with local mode but downloadParallel > maxWARCFiles
		{
			name: "error tech local downloadParallel > maxWARCFiles",
			cmd:  "tech",
			opts: runnerOpts{
				parts:            partsRange{lo: 0, hi: 10},
				mode:             "local",
				remoteMaxPages:   100,
				warcParallel:     4,
				downloadParallel: 5,
				processParallel:  2,
				maxWARCFiles:     2,
			},
			wantErr: true,
		},
		// Error case: tech/both require remoteMaxPages >= 1
		{
			name: "error tech remote missing remoteMaxPages",
			cmd:  "tech",
			opts: runnerOpts{
				parts:            partsRange{lo: 0, hi: 10},
				mode:             "remote",
				remoteMaxPages:   0,
				warcParallel:     4,
				downloadParallel: 2,
				processParallel:  2,
				maxWARCFiles:     5,
			},
			wantErr: true,
		},
		{
			name: "error both remote missing remoteMaxPages",
			cmd:  "both",
			opts: runnerOpts{
				parts:            partsRange{lo: 0, hi: 10},
				mode:             "remote",
				remoteMaxPages:   0,
				warcParallel:     4,
				downloadParallel: 2,
				processParallel:  2,
				maxWARCFiles:     5,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateRunnerOpts(tt.cmd, tt.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateRunnerOpts() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && tt.errMsg != "" && err != nil && err.Error() != tt.errMsg {
				t.Errorf("validateRunnerOpts() error message = %q, want %q", err.Error(), tt.errMsg)
			}
		})
	}
}
