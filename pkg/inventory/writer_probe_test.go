package inventory

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writable(id, serverID string) WriterCandidate {
	return WriterCandidate{ID: id, Status: WriterStatus{Writable: true, ServerID: serverID}}
}

func replicaOf(id, serverID, reason string, sources ...string) WriterCandidate {
	return WriterCandidate{ID: id, Status: WriterStatus{ReadOnlyReason: reason, ServerID: serverID, SourceIDs: sources}}
}

func TestSelectWriter(t *testing.T) {
	tests := []struct {
		name       string
		candidates []WriterCandidate
		want       int
		wantErr    []string
	}{
		{
			name:       "standby replicating from the writer",
			candidates: []WriterCandidate{writable("blue", "uuid-blue"), replicaOf("green", "uuid-green", "read_only=1", "uuid-blue")},
			want:       0,
		},
		{
			name:       "writer listed after its standby, after a switchover",
			candidates: []WriterCandidate{replicaOf("blue", "uuid-blue", "read_only=1", "uuid-green"), writable("green", "uuid-green")},
			want:       1,
		},
		{
			name: "standby replicating from the writer on one of several channels",
			candidates: []WriterCandidate{
				writable("blue", "uuid-blue"),
				replicaOf("green", "uuid-green", "read_only=1", "uuid-elsewhere", "uuid-blue"),
			},
			want: 0,
		},
		{
			name: "every other candidate replicates from the writer",
			candidates: []WriterCandidate{
				writable("blue", "uuid-blue"),
				replicaOf("green", "uuid-green", "read_only=1", "uuid-blue"),
				replicaOf("standby", "uuid-standby", "innodb_read_only=1", "uuid-blue"),
			},
			want: 0,
		},
		{
			name:       "two writable candidates",
			candidates: []WriterCandidate{writable("blue", "uuid-blue"), writable("green", "uuid-green")},
			wantErr:    []string{"2 candidates accept writes", "blue writable", "green writable"},
		},
		{
			name:       "no writable candidate",
			candidates: []WriterCandidate{replicaOf("blue", "uuid-blue", "read_only=1", "uuid-green"), replicaOf("green", "uuid-green", "innodb_read_only=1", "uuid-blue")},
			wantErr:    []string{"no candidate accepts writes", "blue read-only: read_only=1", "green read-only: innodb_read_only=1"},
		},
		{
			name:       "read-only candidate that does not replicate",
			candidates: []WriterCandidate{writable("real", "uuid-real"), replicaOf("stray", "uuid-stray", "read_only=1")},
			wantErr:    []string{"read-only candidate stray does not replicate from writable candidate real"},
		},
		{
			name:       "read-only candidate replicating from somewhere else",
			candidates: []WriterCandidate{writable("real", "uuid-real"), replicaOf("stray", "uuid-stray", "read_only=1", "uuid-other")},
			wantErr:    []string{"read-only candidate stray does not replicate from writable candidate real"},
		},
		{
			name:       "writable candidate with no identity",
			candidates: []WriterCandidate{writable("blue", ""), replicaOf("green", "uuid-green", "read_only=1", "")},
			wantErr:    []string{"writable candidate blue reported no server identity"},
		},
		{
			name:    "no candidates",
			wantErr: []string{"no candidates"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SelectWriter(tt.candidates)
			if len(tt.wantErr) > 0 {
				require.Error(t, err)
				assert.Equal(t, -1, got)
				for _, want := range tt.wantErr {
					assert.Contains(t, err.Error(), want)
				}
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
