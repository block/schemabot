package example

import "os"

// Good: error is handled.
func good() {
	f, _ := os.Open("test")
	if err := f.Close(); err != nil {
		panic(err)
	}
}

// Bad: blank assignment discards error.
func blankAssign() {
	f, _ := os.Open("test")
	_ = f.Close() // want `use utils.CloseAndLog`
}

// Bad: defer with blank assignment in anonymous func.
func deferBlankAssign() {
	f, _ := os.Open("test")
	defer func() {
		_ = f.Close() // want `use utils.CloseAndLog`
	}()
}

// Bad: bare defer Close().
func bareDefer() {
	f, _ := os.Open("test")
	defer f.Close() // want `use utils.CloseAndLog`
}

// Bad: standalone Close() as expression statement (return value ignored).
func exprStmt() {
	f, _ := os.Open("test")
	f.Close() // want `use utils.CloseAndLog`
}

// Good: Close() on a type where Close() does not return error — should not flag.
type noErrorCloser struct{}

func (n *noErrorCloser) Close() {}

func noErrorClose() {
	n := &noErrorCloser{}
	n.Close() // OK — Close() does not return error
	defer n.Close()
}
