package api

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
)

// The CLI encodes apitypes.PlanRequest while the server decodes its own
// PlanRequest with unknown fields refused, and the two structs are declared
// independently. A field present on the client side but missing (or renamed)
// on the server side therefore fails every request that carries it — so the
// client's JSON surface must decode on the server, field for field.
func TestPlanRequestClientFieldsDecodeOnTheServer(t *testing.T) {
	populated := reflect.New(reflect.TypeFor[apitypes.PlanRequest]()).Elem()
	for i := 0; i < populated.NumField(); i++ {
		field := populated.Type().Field(i)
		if field.Tag.Get("json") == "-" {
			continue
		}
		populated.Field(i).Set(populatedJSONValue(t, field.Type))
	}

	data, err := json.Marshal(populated.Interface())
	require.NoError(t, err, "marshal the fully-populated client request")

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var got PlanRequest
	require.NoError(t, decoder.Decode(&got),
		"every field the client can send must decode on the server; request was %s", data)

	assert.True(t, got.GroupedExecution,
		"the statement-grouping decision must survive the client/server boundary")
	assert.Equal(t, "x", got.Database)
}

// populatedJSONValue builds a non-zero value of typ so that omitempty cannot
// hide a field from the round-trip: scalars get literal values, pointers get a
// populated element, and slices and maps get one populated entry. Structs stay
// at their zero value — the check pins this request's own keys, not the
// message types it shares with other requests.
func populatedJSONValue(t *testing.T, typ reflect.Type) reflect.Value {
	t.Helper()
	switch typ.Kind() {
	case reflect.String:
		return reflect.ValueOf("x").Convert(typ)
	case reflect.Bool:
		return reflect.ValueOf(true).Convert(typ)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v := reflect.New(typ).Elem()
		v.SetInt(1)
		return v
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v := reflect.New(typ).Elem()
		v.SetUint(1)
		return v
	case reflect.Float32, reflect.Float64:
		v := reflect.New(typ).Elem()
		v.SetFloat(1)
		return v
	case reflect.Pointer:
		p := reflect.New(typ.Elem())
		if typ.Elem().Kind() != reflect.Struct {
			p.Elem().Set(populatedJSONValue(t, typ.Elem()))
		}
		return p
	case reflect.Slice:
		s := reflect.MakeSlice(typ, 1, 1)
		s.Index(0).Set(populatedJSONValue(t, typ.Elem()))
		return s
	case reflect.Map:
		m := reflect.MakeMap(typ)
		m.SetMapIndex(populatedJSONValue(t, typ.Key()), populatedJSONValue(t, typ.Elem()))
		return m
	case reflect.Struct:
		return reflect.Zero(typ)
	default:
		t.Fatalf("no population rule for field kind %s; add one so the parity check keeps covering every field", typ.Kind())
		return reflect.Value{}
	}
}
