package example

// _test.go files are deliberately not flagged — assertions on rendered output
// legitimately pin the literal glyphs so they break when the vocabulary
// drifts.

func unusedTestFixture() string {
	return "⛔ Apply blocked: 3 unsafe change(s) detected"
}

var _ = unusedTestFixture
