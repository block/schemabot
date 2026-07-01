package auth

import (
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strings"
)

// ForwardAuthConfig configures the forward-auth authorizer, which trusts
// identity headers set by an authenticating reverse proxy in front of the API.
type ForwardAuthConfig struct {
	// UserHeader carries the authenticated user identity. Defaults to
	// "X-Forwarded-User" (the oauth2-proxy convention).
	UserHeader string

	// GroupsHeader carries the caller's group memberships. Defaults to
	// "X-Forwarded-Groups". Values are split on GroupsDelimiter and the header
	// may also be repeated; all values are collected.
	GroupsHeader string

	// GroupsDelimiter splits a single GroupsHeader value into groups. Defaults
	// to ",".
	GroupsDelimiter string

	// TrustedProxySPIFFE lists the SPIFFE IDs allowed to act as the proxy. The
	// caller's SPIFFE ID is read from the Envoy X-Forwarded-Client-Cert (XFCC)
	// header. XFCC is a spoofable HTTP header, so SPIFFE-only trust (no
	// TrustedProxyCIDRs) is safe only when the proxy sanitizes inbound XFCC and
	// the server is not directly reachable — a service mesh. Pair it with
	// TrustedProxyCIDRs for defense in depth outside that setting.
	TrustedProxySPIFFE []string

	// TrustedProxyCIDRs lists source networks allowed to act as the proxy. A
	// request is trusted if its source IP falls within one of these ranges.
	TrustedProxyCIDRs []string

	// ReadGroups are the groups granted the read tier. When empty, any
	// authenticated caller from the trusted proxy may use read-tier endpoints.
	ReadGroups []string

	// WriteGroups are the groups granted the write tier. When empty, no caller
	// can perform write-tier operations (read still works).
	WriteGroups []string
}

// ForwardAuthAuthorizer authenticates requests from an authenticating reverse
// proxy that has already verified the user, then enforces a per-endpoint access
// tier. It first proves the request came from the trusted proxy — its source is
// in a trusted CIDR, and/or its Envoy XFCC header carries a trusted SPIFFE ID —
// and only then trusts the forwarded identity headers. This mirrors the
// Kubernetes API server's authenticating-proxy model, Grafana's auth proxy, and
// oauth2-proxy.
//
// The trust anchor is mandatory: without a configured SPIFFE ID or CIDR the
// authorizer refuses to construct, so it can never trust spoofed headers by
// default. XFCC is itself a spoofable header, so SPIFFE-only trust (no CIDR) is
// safe only behind a proxy that sanitizes inbound XFCC on a server that isn't
// directly reachable — a service mesh; the constructor warns when it's used
// alone. Read (visibility) endpoints require only an authenticated caller
// (optionally narrowed to ReadGroups); write endpoints — which include planning,
// since a plan stages a change against a database — require membership in a
// configured write group. The direct-API/CLI write path is for a small
// privileged set; general users go through the PR-comment workflow instead.
type ForwardAuthAuthorizer struct {
	userHeader    string
	groupsHeader  string
	groupsDelim   string
	trustedSPIFFE []string
	trustedNets   []*net.IPNet
	readGroups    []string
	writeGroups   []string
	logger        *slog.Logger
}

// NewForwardAuthAuthorizer builds a forward-auth authorizer. It requires at
// least one trust anchor (a SPIFFE ID or a CIDR); without one it returns an
// error rather than trusting forwarded headers from any source.
func NewForwardAuthAuthorizer(cfg ForwardAuthConfig, logger *slog.Logger) (*ForwardAuthAuthorizer, error) {
	if logger == nil {
		logger = slog.Default()
	}

	userHeader := cfg.UserHeader
	if userHeader == "" {
		userHeader = "X-Forwarded-User"
	}
	groupsHeader := cfg.GroupsHeader
	if groupsHeader == "" {
		groupsHeader = "X-Forwarded-Groups"
	}
	delim := cfg.GroupsDelimiter
	if delim == "" {
		delim = ","
	}

	var nets []*net.IPNet
	for _, c := range cfg.TrustedProxyCIDRs {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		_, network, err := net.ParseCIDR(c)
		if err != nil {
			return nil, fmt.Errorf("parse trusted_proxy_cidr %q: %w", c, err)
		}
		nets = append(nets, network)
	}

	var spiffe []string
	for _, s := range cfg.TrustedProxySPIFFE {
		if s = strings.TrimSpace(s); s != "" {
			spiffe = append(spiffe, s)
		}
	}

	if len(nets) == 0 && len(spiffe) == 0 {
		return nil, fmt.Errorf("forward_auth requires at least one trust anchor (trusted_proxy_spiffe or trusted_proxy_cidrs)")
	}
	// SPIFFE-only mode reads the caller's SPIFFE ID from XFCC, an HTTP header. It
	// is safe only when the proxy sanitizes inbound XFCC and the server is not
	// directly reachable (e.g. a service mesh where the sidecar sets XFCC and the
	// app accepts traffic only from it). Without a CIDR anchor the authorizer
	// can't verify that at runtime, so warn rather than silently trust.
	if len(spiffe) > 0 && len(nets) == 0 {
		logger.Warn("forward-auth trusting XFCC with no source-CIDR anchor: ensure the proxy sanitizes inbound X-Forwarded-Client-Cert and the server is not directly reachable")
	}

	return &ForwardAuthAuthorizer{
		userHeader:    userHeader,
		groupsHeader:  groupsHeader,
		groupsDelim:   delim,
		trustedSPIFFE: spiffe,
		trustedNets:   nets,
		readGroups:    cfg.ReadGroups,
		writeGroups:   cfg.WriteGroups,
		logger:        logger,
	}, nil
}

// Middleware verifies the request came from the trusted proxy, reads the
// forwarded identity, enforces the endpoint's access tier, and records the
// authenticated user in the request context. A request that did not arrive
// through the trusted proxy is rejected before any forwarded header is trusted.
func (a *ForwardAuthAuthorizer) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipAuth(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}

		tier := tierForRequest(r.Method, r.URL.Path)

		trusted, proxyID := a.isTrustedProxy(r)
		if !trusted {
			a.logger.Warn("forward-auth request did not arrive through the trusted proxy; refusing to honor identity headers",
				"path", r.URL.Path, "remote_addr", r.RemoteAddr)
			authDecision(r, tier, "deny", "untrusted_proxy")
			writeAuthError(w, http.StatusUnauthorized, "request did not arrive through the trusted authenticating proxy")
			return
		}

		// Read the canonical header only. Go's net/http does not fold an
		// underscore variant (X_Forwarded_User) into the dashed form, so a
		// smuggled underscore header cannot be read here.
		user := strings.TrimSpace(r.Header.Get(a.userHeader))
		if user == "" {
			a.logger.Warn("forward-auth trusted proxy supplied no user identity",
				"path", r.URL.Path, "proxy", proxyID, "user_header", a.userHeader)
			authDecision(r, tier, "deny", "no_identity")
			writeAuthError(w, http.StatusUnauthorized, "no authenticated user in forwarded headers")
			return
		}
		groups := a.extractGroups(r)

		switch tier {
		case TierWrite:
			if !matchesAnyGroup(groups, a.writeGroups) {
				a.logger.Warn("forward-auth authorization denied for write operation",
					"path", r.URL.Path, "subject", user)
				authDecision(r, tier, "deny", "not_admin")
				writeAuthError(w, http.StatusForbidden, "this operation requires membership in a write-access group")
				return
			}
		default: // TierRead
			if !a.canRead(groups) {
				a.logger.Warn("forward-auth authorization denied for read operation",
					"path", r.URL.Path, "subject", user)
				authDecision(r, tier, "deny", "not_authorized")
				writeAuthError(w, http.StatusForbidden, "this operation requires membership in a read-access group")
				return
			}
		}

		authDecision(r, tier, "allow", "")
		ctx := WithUser(r.Context(), &User{Subject: user, Groups: groups})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// canRead reports whether the caller may use read-tier endpoints. With no
// configured read groups, reads are open to any authenticated caller; otherwise
// the caller must be in a read group (write-group members can always read).
func (a *ForwardAuthAuthorizer) canRead(groups []string) bool {
	if len(a.readGroups) == 0 {
		return true
	}
	return matchesAnyGroup(groups, a.readGroups) || matchesAnyGroup(groups, a.writeGroups)
}

// extractGroups collects the caller's groups from the groups header, supporting
// both a delimited single value and a repeated header.
func (a *ForwardAuthAuthorizer) extractGroups(r *http.Request) []string {
	var groups []string
	for _, value := range r.Header.Values(a.groupsHeader) {
		for g := range strings.SplitSeq(value, a.groupsDelim) {
			if g = strings.TrimSpace(g); g != "" {
				groups = append(groups, g)
			}
		}
	}
	return groups
}

// isTrustedProxy reports whether the request provably came from the configured
// proxy, and a short identifier of the matched anchor for logging. The
// source-CIDR check is a transport property the caller cannot forge; the SPIFFE
// check reads the proxy's identity from the Envoy XFCC header. The three modes:
//   - CIDR + SPIFFE: trusted iff the source is in a trusted CIDR AND the XFCC
//     carries a trusted SPIFFE ID (defense in depth).
//   - CIDR only: trusted iff the source is in a trusted CIDR.
//   - SPIFFE only: trusted iff the XFCC carries a trusted SPIFFE ID. Safe only
//     when the proxy sanitizes inbound XFCC and the server isn't directly
//     reachable (a service mesh); the constructor warns when this mode is used.
func (a *ForwardAuthAuthorizer) isTrustedProxy(r *http.Request) (bool, string) {
	// When CIDRs are configured they gate everything: a request from outside
	// them is never trusted, regardless of XFCC.
	if len(a.trustedNets) > 0 {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			host = r.RemoteAddr
		}
		ip := net.ParseIP(strings.TrimSpace(host))
		inTrustedNet := false
		for _, network := range a.trustedNets {
			if ip != nil && network.Contains(ip) {
				inTrustedNet = true
				break
			}
		}
		if !inTrustedNet {
			return false, ""
		}
		if len(a.trustedSPIFFE) == 0 {
			return true, "cidr:" + ip.String()
		}
	}

	// SPIFFE check: either SPIFFE-only mode, or the CIDR gate above has passed
	// and a matching SPIFFE ID is additionally required.
	for _, uri := range parseXFCCURIs(r.Header.Get("X-Forwarded-Client-Cert")) {
		if slices.Contains(a.trustedSPIFFE, uri) {
			return true, "spiffe:" + uri
		}
	}
	return false, ""
}

// parseXFCCURIs extracts the URI (SPIFFE SVID) values from an Envoy
// X-Forwarded-Client-Cert header. The header is a list of elements separated by
// commas (one per cert in the chain); each element is a list of key=value pairs
// separated by semicolons; values may be double-quoted, and a quoted value may
// contain commas and semicolons. Only URI keys are returned.
func parseXFCCURIs(header string) []string {
	if header == "" {
		return nil
	}
	var uris []string
	for _, pair := range splitXFCC(header) {
		before, after, ok := strings.Cut(pair, "=")
		if !ok {
			continue
		}
		key := strings.TrimSpace(before)
		val := strings.Trim(strings.TrimSpace(after), `"`)
		if val != "" && strings.EqualFold(key, "URI") {
			uris = append(uris, val)
		}
	}
	return uris
}

// splitXFCC splits an XFCC header into key=value tokens, treating both the
// element separator (comma) and the pair separator (semicolon) as boundaries
// while respecting double-quoted values.
func splitXFCC(header string) []string {
	var (
		tokens  []string
		current strings.Builder
		inQuote bool
	)
	for i := 0; i < len(header); i++ {
		c := header[i]
		switch {
		case c == '"':
			inQuote = !inQuote
			current.WriteByte(c)
		case (c == ',' || c == ';') && !inQuote:
			tokens = append(tokens, current.String())
			current.Reset()
		default:
			current.WriteByte(c)
		}
	}
	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}
	return tokens
}
