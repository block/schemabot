package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOperationLease_Valid(t *testing.T) {
	tests := []struct {
		name  string
		lease OperationLease
		want  bool
	}{
		{
			name:  "fully populated",
			lease: OperationLease{ApplyID: 1, OperationID: 2, Owner: "driver-0", Token: "tok"},
			want:  true,
		},
		{
			name:  "missing token",
			lease: OperationLease{ApplyID: 1, OperationID: 2, Owner: "driver-0"},
			want:  false,
		},
		{
			name:  "missing operation id",
			lease: OperationLease{ApplyID: 1, Token: "tok"},
			want:  false,
		},
		{
			name:  "missing apply id",
			lease: OperationLease{OperationID: 2, Token: "tok"},
			want:  false,
		},
		{
			name:  "zero value",
			lease: OperationLease{},
			want:  false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.lease.Valid())
		})
	}
}

func TestOperationLeaseContextRoundTrip(t *testing.T) {
	lease := OperationLease{ApplyID: 7, OperationID: 9, Owner: "operator-3", Token: "abc123"}
	ctx := WithOperationLease(t.Context(), lease)

	got, ok := OperationLeaseFromContext(ctx)
	assert.True(t, ok)
	assert.Equal(t, lease, got)
}

func TestOperationLeaseFromContext_Absent(t *testing.T) {
	_, ok := OperationLeaseFromContext(t.Context())
	assert.False(t, ok)
}

// An operation lease must not be readable as an apply lease and vice versa:
// the two capabilities use distinct context keys so a driver can carry both at
// once without one masquerading as the other.
func TestOperationAndApplyLeasesAreDistinct(t *testing.T) {
	ctx := WithOperationLease(t.Context(), OperationLease{ApplyID: 1, OperationID: 2, Token: "op"})
	_, ok := ApplyLeaseFromContext(ctx)
	assert.False(t, ok, "operation lease must not satisfy ApplyLeaseFromContext")

	ctx = WithApplyLease(t.Context(), ApplyLease{ApplyID: 1, Token: "apply"})
	_, ok = OperationLeaseFromContext(ctx)
	assert.False(t, ok, "apply lease must not satisfy OperationLeaseFromContext")
}

func TestApplyOperation_Lease(t *testing.T) {
	op := &ApplyOperation{ID: 5, ApplyID: 3, LeaseOwner: "operator-1", LeaseToken: "tok-5"}
	lease := op.Lease()
	assert.Equal(t, int64(5), lease.OperationID)
	assert.Equal(t, int64(3), lease.ApplyID)
	assert.Equal(t, "operator-1", lease.Owner)
	assert.Equal(t, "tok-5", lease.Token)
	assert.True(t, lease.Valid())

	var nilOp *ApplyOperation
	assert.Equal(t, OperationLease{}, nilOp.Lease())
}
