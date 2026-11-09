package storage

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
)

// StoredMessage holds a message to what its column takes: valid UTF-8 no
// longer than the column's byte width, cut on a character boundary. A message
// that already fits is stored exactly as given.
func TestStoredMessage(t *testing.T) {
	cases := []struct {
		name string
		msg  string
		want string
	}{
		{
			name: "a short message is stored as given",
			msg:  "copy of users failed: lock wait timeout exceeded",
			want: "copy of users failed: lock wait timeout exceeded",
		},
		{
			name: "an empty message stays empty",
			msg:  "",
			want: "",
		},
		{
			name: "a message exactly the column width is stored whole",
			msg:  strings.Repeat("a", 65_535),
			want: strings.Repeat("a", 65_535),
		},
		{
			name: "a message one byte past the column width is cut to exactly the width",
			msg:  strings.Repeat("a", 65_536),
			want: strings.Repeat("a", 65_532) + "…",
		},
		{
			// The leading byte puts every two-byte character at an odd offset,
			// so the column's byte width falls inside a character.
			name: "a cut that falls inside a character backs off to the character's start",
			msg:  "x" + strings.Repeat("é", 40_000),
			want: "x" + strings.Repeat("é", 32_765) + "…",
		},
		{
			name: "bytes that are not valid UTF-8 are replaced",
			msg:  "duplicate key \xff\xfe in row",
			want: "duplicate key \uFFFD in row",
		},
		{
			name: "a valid replacement character is kept",
			msg:  "duplicate key \uFFFD in row",
			want: "duplicate key \uFFFD in row",
		},
		{
			// Replacing the invalid byte takes the message from the column
			// width to two bytes past it, so the cut applies to the replaced
			// message.
			name: "a replacement that grows the message past the column width is cut",
			msg:  strings.Repeat("a", 65_534) + "\xff",
			want: strings.Repeat("a", 65_532) + "…",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := StoredMessage(tc.msg)
			if assert.Equal(t, len(tc.want), len(got), "stored length in bytes") {
				assert.True(t, tc.want == got, "stored message")
			}
			assert.LessOrEqual(t, len(got), 65_535, "the stored message fits the column")
			assert.True(t, utf8.ValidString(got), "the stored message is valid UTF-8")
			assert.True(t, StoredMessage(got) == got, "a stored message is already in its stored form")
		})
	}
}
