package secrets

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolve_EmptyWithFallback(t *testing.T) {
	t.Setenv("TEST_FALLBACK_VAR", "fallback-value")

	result, err := Resolve("", "TEST_FALLBACK_VAR")
	require.NoError(t, err)
	assert.Equal(t, "fallback-value", result)
}

func TestResolve_EmptyNoFallback(t *testing.T) {
	result, err := Resolve("", "NONEXISTENT_VAR_12345")
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestResolve_EnvPrefix(t *testing.T) {
	t.Setenv("MY_SECRET_TOKEN", "secret-token-value")

	result, err := Resolve("env:MY_SECRET_TOKEN", "")
	require.NoError(t, err)
	assert.Equal(t, "secret-token-value", result)
}

func TestResolve_EnvPrefix_NotSet(t *testing.T) {
	result, err := Resolve("env:NONEXISTENT_VAR_67890", "")
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestResolve_FilePrefix(t *testing.T) {
	dir := t.TempDir()
	secretFile := filepath.Join(dir, "secret.txt")
	require.NoError(t, os.WriteFile(secretFile, []byte("file-secret-value\n"), 0600))

	result, err := Resolve("file:"+secretFile, "")
	require.NoError(t, err)
	assert.Equal(t, "file-secret-value", result)
}

func TestResolve_FilePrefix_NotFound(t *testing.T) {
	_, err := Resolve("file:/nonexistent/path/to/secret", "")
	require.Error(t, err)
}

func TestResolve_LiteralValue(t *testing.T) {
	result, err := Resolve("just-a-literal-value", "")
	require.NoError(t, err)
	assert.Equal(t, "just-a-literal-value", result)
}

func TestResolve_LiteralWithColon(t *testing.T) {
	// Values with colons that don't match known prefixes are returned as-is
	result, err := Resolve("host:port:path", "")
	require.NoError(t, err)
	assert.Equal(t, "host:port:path", result)
}

// fakeSecretsManager serves secret strings by name.
type fakeSecretsManager struct {
	values map[string]string
}

func (f *fakeSecretsManager) GetSecretValue(_ context.Context, in *secretsmanager.GetSecretValueInput, _ ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
	value, ok := f.values[aws.ToString(in.SecretId)]
	if !ok {
		return nil, fmt.Errorf("secret %s not found", aws.ToString(in.SecretId))
	}
	return &secretsmanager.GetSecretValueOutput{SecretString: aws.String(value)}, nil
}

// useSecretsManagerBuilder makes build the way the resolver gets its client
// for the rest of the test, starting from no shared client, and restores the
// real builder afterwards.
func useSecretsManagerBuilder(t *testing.T, build func(context.Context) (secretGetter, error)) {
	t.Helper()
	secretsManagerMu.Lock()
	secretsManagerClient = nil
	secretsManagerBuild = nil
	newSecretsManagerClient = build
	secretsManagerMu.Unlock()
	t.Cleanup(func() {
		secretsManagerMu.Lock()
		secretsManagerClient = nil
		secretsManagerBuild = nil
		newSecretsManagerClient = loadSecretsManagerClient
		secretsManagerMu.Unlock()
	})
}

// A target's password is resolved again on every connection. The resolves
// share one client, so the AWS config and credentials are loaded once, while
// each resolve still reads the secret's current value and picks up a rotation.
func TestResolve_SecretsManagerReusesClientAndReadsCurrentValue(t *testing.T) {
	fake := &fakeSecretsManager{values: map[string]string{
		"/example/orders": `{"username":"schemabot","password":"first"}`,
	}}
	builds := 0
	useSecretsManagerBuilder(t, func(context.Context) (secretGetter, error) {
		builds++
		return fake, nil
	})

	password, err := ResolveContext(t.Context(), "secretsmanager:/example/orders#password", "")
	require.NoError(t, err)
	assert.Equal(t, "first", password)

	fake.values["/example/orders"] = `{"username":"schemabot","password":"rotated"}`
	password, err = ResolveContext(t.Context(), "secretsmanager:/example/orders#password", "")
	require.NoError(t, err)
	assert.Equal(t, "rotated", password, "a rotated secret takes effect on the next resolve")

	document, err := ResolveContext(t.Context(), "secretsmanager:/example/orders", "")
	require.NoError(t, err)
	assert.JSONEq(t, `{"username":"schemabot","password":"rotated"}`, document)

	assert.Equal(t, 1, builds, "every resolve uses the one shared client")
}

// A client that fails to build is not kept: the resolve that hit the failure
// returns it, and the next resolve builds the client again.
func TestResolve_SecretsManagerRetriesFailedClientBuild(t *testing.T) {
	fake := &fakeSecretsManager{values: map[string]string{"/example/orders": "s3cret"}}
	builds := 0
	useSecretsManagerBuilder(t, func(context.Context) (secretGetter, error) {
		builds++
		if builds == 1 {
			return nil, errors.New("load AWS config: no region")
		}
		return fake, nil
	})

	_, err := ResolveContext(t.Context(), "secretsmanager:/example/orders", "")
	require.ErrorContains(t, err, "load AWS config: no region")

	value, err := ResolveContext(t.Context(), "secretsmanager:/example/orders", "")
	require.NoError(t, err)
	assert.Equal(t, "s3cret", value)
	assert.Equal(t, 2, builds)
}

// While one resolve is still loading the AWS config, another resolve whose
// context is cancelled returns straight away instead of waiting for that load.
// When the load finishes, the first resolve gets its value and later resolves
// reuse the client it built.
func TestResolve_SecretsManagerWaiterHonorsItsContext(t *testing.T) {
	fake := &fakeSecretsManager{values: map[string]string{"/example/orders": "s3cret"}}
	building := make(chan struct{})
	release := make(chan struct{})
	var builds atomic.Int32
	useSecretsManagerBuilder(t, func(context.Context) (secretGetter, error) {
		builds.Add(1)
		close(building)
		<-release
		return fake, nil
	})

	type result struct {
		value string
		err   error
	}
	first := make(chan result, 1)
	go func() {
		value, err := ResolveContext(t.Context(), "secretsmanager:/example/orders", "")
		first <- result{value, err}
	}()
	<-building

	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := ResolveContext(cancelled, "secretsmanager:/example/orders", "")
	require.ErrorIs(t, err, context.Canceled)
	select {
	case <-first:
		t.Fatal("the first resolve finished before its client build was released")
	default:
	}

	close(release)
	got := <-first
	require.NoError(t, got.err)
	assert.Equal(t, "s3cret", got.value)

	value, err := ResolveContext(t.Context(), "secretsmanager:/example/orders", "")
	require.NoError(t, err)
	assert.Equal(t, "s3cret", value)
	assert.Equal(t, int32(1), builds.Load(), "the waiter did not build, and later resolves reuse the client")
}

// waitersBlocked reports how many goroutines are parked waiting on another
// resolve's client build.
func waitersBlocked() int {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	count := 0
	for g := range strings.SplitSeq(string(buf[:n]), "\n\n") {
		if strings.Contains(g, "[select") && strings.Contains(g, "sharedSecretsManagerClient") {
			count++
		}
	}
	return count
}

// Several connections start at once on a cold process. One resolve builds the
// client while the others wait with live contexts. When the build succeeds,
// every waiter uses that client; when it fails, the resolve that built it
// returns the error and exactly one waiter builds again, which the rest share.
func TestResolve_SecretsManagerWaitersFollowTheBuildOutcome(t *testing.T) {
	const waiters = 3
	for _, tc := range []struct {
		name       string
		firstFails bool
		wantBuilds int32
	}{
		{"build succeeds", false, 1},
		{"build fails", true, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeSecretsManager{values: map[string]string{"/example/orders": "s3cret"}}
			building := make(chan struct{})
			release := make(chan struct{})
			var builds atomic.Int32
			useSecretsManagerBuilder(t, func(context.Context) (secretGetter, error) {
				if builds.Add(1) == 1 {
					close(building)
					<-release
					if tc.firstFails {
						return nil, errors.New("load AWS config: transient")
					}
				}
				return fake, nil
			})

			type result struct {
				value string
				err   error
			}
			resolve := func() chan result {
				ch := make(chan result, 1)
				go func() {
					ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
					defer cancel()
					value, err := ResolveContext(ctx, "secretsmanager:/example/orders", "")
					ch <- result{value, err}
				}()
				return ch
			}
			first := resolve()
			<-building
			var waiting []chan result
			for range waiters {
				waiting = append(waiting, resolve())
			}
			require.Eventually(t, func() bool { return waitersBlocked() == waiters }, 5*time.Second, 5*time.Millisecond)

			close(release)
			gotFirst := <-first
			if tc.firstFails {
				require.ErrorContains(t, gotFirst.err, "transient")
			} else {
				require.NoError(t, gotFirst.err)
				assert.Equal(t, "s3cret", gotFirst.value)
			}
			for _, ch := range waiting {
				got := <-ch
				require.NoError(t, got.err)
				assert.Equal(t, "s3cret", got.value)
			}
			assert.Equal(t, tc.wantBuilds, builds.Load())
		})
	}
}

// A panic while building the client, recovered by its caller, does not leave
// the build marked as running: the next resolve builds the client and gets its
// value instead of waiting until its deadline.
func TestResolve_SecretsManagerRecoveredBuildPanicDoesNotBlockLaterResolves(t *testing.T) {
	fake := &fakeSecretsManager{values: map[string]string{"/example/orders": "s3cret"}}
	builds := 0
	useSecretsManagerBuilder(t, func(context.Context) (secretGetter, error) {
		builds++
		if builds == 1 {
			panic("config loader panicked")
		}
		return fake, nil
	})

	require.Panics(t, func() {
		_, _ = ResolveContext(t.Context(), "secretsmanager:/example/orders", "")
	})

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	value, err := ResolveContext(ctx, "secretsmanager:/example/orders", "")
	require.NoError(t, err)
	assert.Equal(t, "s3cret", value)
	assert.Equal(t, 2, builds)
}

// registerResolver registers resolver for prefix for the rest of the test.
func registerResolver(t *testing.T, prefix string, resolver ResolverFunc) {
	t.Helper()
	Register(prefix, resolver)
	t.Cleanup(func() { Unregister(prefix) })
}

// A registered resolver receives the reference with its prefix stripped, and
// its value and its error reach the caller unchanged.
func TestResolve_CustomResolverReceivesTheReferenceWithoutItsPrefix(t *testing.T) {
	var refs []string
	registerResolver(t, "vault", func(ref string) (string, error) {
		refs = append(refs, ref)
		if ref == "secret/data/missing" {
			return "", errors.New("vault: secret not found")
		}
		return "vault-password", nil
	})

	value, err := Resolve("vault:secret/data/myapp#password", "")
	require.NoError(t, err)
	assert.Equal(t, "vault-password", value)

	_, err = Resolve("vault:secret/data/missing", "")
	require.EqualError(t, err, "vault: secret not found")

	assert.Equal(t, []string{"secret/data/myapp#password", "secret/data/missing"}, refs)
}

// A registered resolver takes precedence over the built-in resolver for the
// same prefix, and unregistering it gives the prefix back to the built-in.
func TestResolve_CustomResolverOverridesABuiltInPrefixUntilUnregistered(t *testing.T) {
	t.Setenv("ORDERS_PASSWORD", "from-environment")
	registerResolver(t, "env", func(ref string) (string, error) {
		return "from-custom-resolver:" + ref, nil
	})

	value, err := Resolve("env:ORDERS_PASSWORD", "")
	require.NoError(t, err)
	assert.Equal(t, "from-custom-resolver:ORDERS_PASSWORD", value)

	Unregister("env")
	value, err = Resolve("env:ORDERS_PASSWORD", "")
	require.NoError(t, err)
	assert.Equal(t, "from-environment", value)
}

// deadlineRecordingSecretsManager records the deadline of the context each
// fetch runs under.
type deadlineRecordingSecretsManager struct {
	deadline    time.Time
	hasDeadline bool
}

func (f *deadlineRecordingSecretsManager) GetSecretValue(ctx context.Context, _ *secretsmanager.GetSecretValueInput, _ ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
	f.deadline, f.hasDeadline = ctx.Deadline()
	return &secretsmanager.GetSecretValueOutput{SecretString: aws.String("s3cret")}, nil
}

// A Secrets Manager lookup, the client build included, is bounded by the
// caller's deadline or a ten-second cap, whichever comes first. A caller with
// no deadline, such as Resolve, still cannot hang on an unreachable endpoint.
func TestResolve_SecretsManagerLookupIsBoundedByTheCallerOrTenSeconds(t *testing.T) {
	const lookupCap = 10 * time.Second

	tests := []struct {
		name           string
		callerDeadline time.Duration // zero means the caller sets no deadline
		wantCapped     bool
	}{
		{name: "caller without a deadline gets the cap", wantCapped: true},
		{name: "caller deadline later than the cap is shortened to it", callerDeadline: time.Minute, wantCapped: true},
		{name: "caller deadline sooner than the cap is kept", callerDeadline: 2 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &deadlineRecordingSecretsManager{}
			var buildDeadline time.Time
			var buildHasDeadline bool
			useSecretsManagerBuilder(t, func(ctx context.Context) (secretGetter, error) {
				buildDeadline, buildHasDeadline = ctx.Deadline()
				return fake, nil
			})

			ctx := t.Context()
			if tt.callerDeadline > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tt.callerDeadline)
				defer cancel()
			}
			callerDeadline, callerHasDeadline := ctx.Deadline()

			start := time.Now()
			value, err := ResolveContext(ctx, "secretsmanager:/example/orders", "")
			require.NoError(t, err)
			assert.Equal(t, "s3cret", value)

			require.True(t, fake.hasDeadline, "the fetch must run under a deadline")
			require.True(t, buildHasDeadline, "the client build must run under a deadline")
			assert.Equal(t, fake.deadline, buildDeadline, "the build and the fetch share one deadline")
			if tt.wantCapped {
				assert.WithinDuration(t, start.Add(lookupCap), fake.deadline, time.Second)
			} else {
				require.True(t, callerHasDeadline)
				assert.Equal(t, callerDeadline, fake.deadline)
			}
		})
	}
}

// A failed fetch names the secret, so an operator can tell which reference
// is wrong from the error alone.
func TestResolve_SecretsManagerFetchErrorNamesTheSecret(t *testing.T) {
	fake := &fakeSecretsManager{values: map[string]string{}}
	useSecretsManagerBuilder(t, func(context.Context) (secretGetter, error) {
		return fake, nil
	})

	_, err := ResolveContext(t.Context(), "secretsmanager:/example/missing#password", "")
	require.EqualError(t, err, `get secret "/example/missing": secret /example/missing not found`)
}

func TestValueFromGetSecretOutput(t *testing.T) {
	// The string form is used when present.
	got, err := ValueFromGetSecretOutput(&secretsmanager.GetSecretValueOutput{SecretString: aws.String("pw")}, "secret")
	require.NoError(t, err)
	assert.Equal(t, "pw", got)

	// A binary-only secret falls back to its bytes rather than being rejected.
	got, err = ValueFromGetSecretOutput(&secretsmanager.GetSecretValueOutput{SecretBinary: []byte("binary-pw")}, "secret")
	require.NoError(t, err)
	assert.Equal(t, "binary-pw", got)

	// An empty-but-present binary value round-trips as "" rather than erroring.
	got, err = ValueFromGetSecretOutput(&secretsmanager.GetSecretValueOutput{SecretBinary: []byte{}}, "secret")
	require.NoError(t, err)
	assert.Empty(t, got)

	// Neither form set is an error naming the secret.
	_, err = ValueFromGetSecretOutput(&secretsmanager.GetSecretValueOutput{}, "secret")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "has no value")
}

func TestSecretJSONKey(t *testing.T) {
	const secret = `{"app-id": "123456", "app_id": 1234567, "big": 12345678901234567890, "enabled": true, "pk": "-----BEGIN", "nested": {"a": 1}, "absent": null}`

	t.Run("string value is returned as is", func(t *testing.T) {
		got, err := secretJSONKey(secret, "github-app", "app-id")
		require.NoError(t, err)
		assert.Equal(t, "123456", got)
	})

	t.Run("number value keeps every digit", func(t *testing.T) {
		got, err := secretJSONKey(secret, "github-app", "app_id")
		require.NoError(t, err)
		assert.Equal(t, "1234567", got)

		got, err = secretJSONKey(secret, "github-app", "big")
		require.NoError(t, err)
		assert.Equal(t, "12345678901234567890", got)
	})

	t.Run("other values are their JSON literal", func(t *testing.T) {
		got, err := secretJSONKey(secret, "github-app", "enabled")
		require.NoError(t, err)
		assert.Equal(t, "true", got)

		got, err = secretJSONKey(secret, "github-app", "nested")
		require.NoError(t, err)
		assert.Equal(t, `{"a":1}`, got)
	})

	t.Run("null value is an error rather than a stand-in string", func(t *testing.T) {
		_, err := secretJSONKey(secret, "github-app", "absent")
		require.Error(t, err)
		assert.Contains(t, err.Error(), `key "absent" in secret "github-app" is null`)
	})

	t.Run("data after the object is rejected", func(t *testing.T) {
		for _, corrupt := range []string{
			`{"app-id": "1"} {"app-id": "2"}`,
			`{"app-id": "1"} junk`,
			`{"app-id": "1"}}`,
		} {
			_, err := secretJSONKey(corrupt, "github-app", "app-id")
			require.Error(t, err, corrupt)
			assert.Contains(t, err.Error(), `parse secret "github-app" as JSON: unexpected data after the JSON object`, corrupt)
		}
	})

	t.Run("trailing whitespace is not data", func(t *testing.T) {
		got, err := secretJSONKey(`{"app-id": "1"}`+"\n  \n", "github-app", "app-id")
		require.NoError(t, err)
		assert.Equal(t, "1", got)
	})

	t.Run("missing key names the key and the secret", func(t *testing.T) {
		_, err := secretJSONKey(secret, "github-app", "webhook-secret")
		require.Error(t, err)
		assert.Contains(t, err.Error(), `key "webhook-secret" not found in secret "github-app"`)
	})

	t.Run("non-JSON secret names the secret", func(t *testing.T) {
		_, err := secretJSONKey("not json", "github-app", "app-id")
		require.Error(t, err)
		assert.Contains(t, err.Error(), `parse secret "github-app" as JSON`)
	})
}
