package httpexample

import "net/http"

// Good: resp.Body.Close() is exempt — bodyclose handles HTTP bodies.
func deferBodyClose() {
	resp, _ := http.Get("http://example.com")
	defer func() { _ = resp.Body.Close() }() // OK — resp.Body is exempt
}

func bareBodyClose() {
	resp, _ := http.Get("http://example.com")
	defer resp.Body.Close() // OK — resp.Body is exempt
}

func bodyCloseExpr() {
	resp, _ := http.Get("http://example.com")
	resp.Body.Close() // OK — resp.Body is exempt
}
