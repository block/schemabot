package localsetup

import (
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
			require.Error(t, CheckConnection(t.Context(), engine, ""))
		})
	}
}
