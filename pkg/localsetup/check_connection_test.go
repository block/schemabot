package localsetup

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConnectionErrorsNeverExposeDSN(t *testing.T) {
	for _, engine := range []string{"mysql", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			secret := "private-password-do-not-print"
			dsn := "postgres://user:" + secret + "@%bad"
			err := CheckConnection(t.Context(), engine, dsn)
			require.Error(t, err)
			require.NotContains(t, err.Error(), secret)
			_, err = DiscoverNamespaces(t.Context(), engine, dsn)
			require.Error(t, err)
			require.NotContains(t, err.Error(), secret)
			require.Error(t, CheckConnection(t.Context(), engine, ""))
			_, err = DiscoverNamespaces(t.Context(), engine, "")
			require.Error(t, err)
		})
	}
}

func TestConnectionErrorRetainsCause(t *testing.T) {
	cause := errors.New("driver detail with private connection material")
	err := &setupConnectionError{message: "Check the connection", cause: cause}
	require.ErrorIs(t, err, cause)
	require.EqualError(t, err, "Check the connection")
}
