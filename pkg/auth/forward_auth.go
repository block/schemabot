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

	// OperatorGroups are the per-database operator groups (the union across
	// every configured database's operator_groups). Members are admitted to
	// write-tier endpoints by this middleware, but that admission is only the
	// first half of the decision: the middleware runs before the request body
	// is parsed, so it cannot know the target database. Each mutating handler
	// enforces the per-database and per-environment scope once the target
	// resolves. Operator members also get the read tier — an operator who can
	// mutate their database must be able to watch the result.
	OperatorGroups []string

	// TrustedGatewaySPIFFE lists the SPIFFE IDs of caller-forwarding gateways.
	// Such a gateway terminates the caller's mTLS, verifies the client
	// certificate, and forwards the caller's SPIFFE ID in CallerSPIFFEHeader.
	// List a gateway only if it strips inbound copies of that header before
	// setting it from the verified certificate — otherwise any caller could
	// forge an identity through it. Must not overlap TrustedProxySPIFFE: a peer
	// either forwards user identity headers or a caller SPIFFE ID, never both.
	// Requires a non-empty TrustedProxySPIFFE: the lane authenticates on the
	// XFCC-carried gateway SVID, and under CIDR-only proxy trust it would be
	// unreachable (see the constructor). TrustedProxyCIDRs, when set, gate
	// this lane like everything else.
	TrustedGatewaySPIFFE []string

	// CallerSPIFFEHeader carries the gateway-verified caller SPIFFE ID.
	// Defaults to "X-Forwarded-Caller-Spiffe-Id". Honored only when the request
	// provably arrived through a listed gateway.
	CallerSPIFFEHeader string

	// ReadServiceSPIFFE lists caller SPIFFE IDs granted read-tier access
	// through a trusted gateway. Service callers never get the write tier.
	ReadServiceSPIFFE []string
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
// configured write group or in a per-database operator group (OperatorGroups).
// Write groups grant every database; operator membership is only admission —
// each mutating handler enforces the caller's per-database and per-environment
// scope once the target resolves, because the middleware runs before the
// request body carrying the target is parsed. General users go through the
// PR-comment workflow instead.
//
// A second, optional lane serves services calling as themselves through a
// caller-forwarding gateway (TrustedGatewaySPIFFE): the gateway verifies the
// caller's client certificate and forwards the caller's SPIFFE ID in a
// dedicated header. That lane is read-only, honors the caller header only when
// the XFCC-verified peer is a listed gateway, and never reads the user or
// groups headers. Configured TrustedProxyCIDRs gate this lane too — a gateway
// SVID claimed from outside the trusted networks is never honored — and the
// lane requires SPIFFE-anchored proxy trust, since under CIDR-only trust an
// in-CIDR gateway request would be mistaken for the identity-header proxy. The
// identity-header proxy always wins: the gateway lane is consulted only when
// the request did not arrive through the trusted proxy.
type ForwardAuthAuthorizer struct {
	userHeader        string
	groupsHeader      string
	groupsDelim       string
	trustedSPIFFE     []string
	trustedNets       []*net.IPNet
	readGroups        []string
	writeGroups       []string
	operatorGroups    []string
	gatewaySPIFFE     []string
	callerHeader      string
	readServiceSPIFFE []string
	logger            *slog.Logger
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

	callerHeader := cfg.CallerSPIFFEHeader
	if callerHeader != strings.TrimSpace(callerHeader) {
		return nil, fmt.Errorf("forward_auth caller_spiffe_header must not have leading or trailing whitespace")
	}
	if callerHeader == "" {
		callerHeader = "X-Forwarded-Caller-Spiffe-Id"
	}
	var gateways []string
	for _, g := range cfg.TrustedGatewaySPIFFE {
		if g = strings.TrimSpace(g); g != "" {
			gateways = append(gateways, g)
		}
	}
	var readServices []string
	for _, s := range cfg.ReadServiceSPIFFE {
		if s = strings.TrimSpace(s); s != "" {
			readServices = append(readServices, s)
		}
	}
	// The service-caller lane fails closed: a gateway list without callers (or
	// callers without a gateway to vouch for them) is a configuration mistake,
	// not an empty allowlist.
	if len(gateways) > 0 && len(readServices) == 0 {
		return nil, fmt.Errorf("forward_auth trusted_gateway_spiffe requires at least one read_service_spiffe caller")
	}
	if len(readServices) > 0 && len(gateways) == 0 {
		return nil, fmt.Errorf("forward_auth read_service_spiffe requires at least one trusted_gateway_spiffe gateway")
	}
	for _, g := range gateways {
		if slices.Contains(spiffe, g) {
			return nil, fmt.Errorf("forward_auth SPIFFE ID %q is listed in both trusted_proxy_spiffe and trusted_gateway_spiffe: a peer either forwards user identity headers or a caller SPIFFE ID, never both", g)
		}
	}
	// The gateway lane authenticates on the XFCC-carried gateway SVID, so it
	// needs SPIFFE-anchored proxy trust to coexist with. Under CIDR-only trust
	// the lane can never authenticate anyone: an in-CIDR gateway request is
	// treated as the identity-header proxy (and 401s with no user header),
	// while an out-of-CIDR one is refused by the CIDR gate. Refuse to start
	// rather than ship a dead lane.
	if len(gateways) > 0 && len(spiffe) == 0 {
		return nil, fmt.Errorf("forward_auth trusted_gateway_spiffe requires SPIFFE-anchored proxy trust (trusted_proxy_spiffe): with CIDR-only trust the service-caller lane is unreachable")
	}
	if len(gateways) > 0 {
		logger.Warn("forward-auth honoring gateway-forwarded caller identity: every listed gateway must strip inbound copies of the caller header before setting it from the verified client certificate",
			"caller_header", callerHeader, "gateways", len(gateways), "read_service_callers", len(readServices))
	}

	return &ForwardAuthAuthorizer{
		userHeader:        userHeader,
		groupsHeader:      groupsHeader,
		groupsDelim:       delim,
		trustedSPIFFE:     spiffe,
		trustedNets:       nets,
		readGroups:        cfg.ReadGroups,
		writeGroups:       cfg.WriteGroups,
		operatorGroups:    cfg.OperatorGroups,
		gatewaySPIFFE:     gateways,
		callerHeader:      callerHeader,
		readServiceSPIFFE: readServices,
		logger:            logger,
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

		tier := TierForRequest(r.Method, r.URL.Path)

		trusted, proxyID := a.isTrustedProxy(r)
		if !trusted {
			// Parse the XFCC header once for the whole deny path: the gateway
			// lane and the denial log both consume the URI values.
			xfccURIs := parseXFCCURIs(r.Header.Get("X-Forwarded-Client-Cert"))
			// The identity-header proxy did not vouch for this request; try the
			// service-caller lane before rejecting.
			if user, handled := a.authorizeGatewayCaller(w, r, tier, xfccURIs); handled {
				if user != nil {
					next.ServeHTTP(w, r.WithContext(WithUser(r.Context(), user)))
				}
				return
			}
			// Log the URI identities present in the XFCC header so an operator
			// can tell a missing trust-anchor entry apart from a request whose
			// header carried no identity at all. On this path the values are
			// unverified caller input — triage signal, never a trust decision.
			uris, uriCount := clampLoggedXFCCURIs(xfccURIs)
			a.logger.Warn("forward-auth request did not arrive through the trusted proxy; refusing to honor identity headers",
				"path", r.URL.Path, "remote_addr", r.RemoteAddr,
				"xfcc_uris", uris, "xfcc_uri_count", uriCount)
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
			// Write admission covers deployment write groups and per-database
			// operator groups. For operators this is only the first half of the
			// decision: the target database is in the not-yet-parsed request
			// body, so the mutating handler enforces the per-database and
			// per-environment scope once the target resolves.
			if !matchesAnyGroup(groups, a.writeGroups) && !matchesAnyGroup(groups, a.operatorGroups) {
				a.logger.Warn("forward-auth authorization denied for write operation",
					"path", r.URL.Path, "subject", user)
				authDecision(r, tier, "deny", "not_admin")
				// Only mention operator groups when the deployment has any;
				// on a deployment without them the suffix would send a denied
				// caller hunting for a lane that does not exist.
				msg := "this operation requires membership in a write-access group"
				if len(a.operatorGroups) > 0 {
					msg += " or a database operator group"
				}
				writeAuthError(w, http.StatusForbidden, msg)
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

// authorizeGatewayCaller handles the service-caller lane: a caller-forwarding
// gateway terminated the caller's mTLS, verified the client certificate, and
// forwarded the caller's SPIFFE ID in the caller header. The lane grants the
// read tier only, to callers explicitly listed in ReadServiceSPIFFE, and never
// reads the user or groups headers. handled=false means the request did not
// arrive through a listed gateway — or, when CIDRs are configured, did not
// arrive from a trusted network — so the caller falls through to the standard
// untrusted-proxy denial. xfccURIs are the URI values already parsed from the
// request's XFCC header.
func (a *ForwardAuthAuthorizer) authorizeGatewayCaller(w http.ResponseWriter, r *http.Request, tier Tier, xfccURIs []string) (*User, bool) {
	if len(a.gatewaySPIFFE) == 0 {
		return nil, false
	}
	// Configured CIDRs gate everything, this lane included: XFCC is a
	// spoofable header, so a gateway SVID claimed from outside the trusted
	// networks is never honored — the request falls through to the standard
	// untrusted-proxy denial, whose log records the source address and the
	// claimed identities.
	if len(a.trustedNets) > 0 {
		if _, ok := a.trustedSourceIP(r); !ok {
			return nil, false
		}
	}
	var gatewayID string
	for _, uri := range xfccURIs {
		if slices.Contains(a.gatewaySPIFFE, uri) {
			gatewayID = uri
			break
		}
	}
	if gatewayID == "" {
		return nil, false
	}

	caller := strings.TrimSpace(r.Header.Get(a.callerHeader))
	if caller == "" {
		a.logger.Warn("forward-auth gateway forwarded no caller identity; denying",
			"path", r.URL.Path, "remote_addr", r.RemoteAddr, "gateway", gatewayID, "caller_header", a.callerHeader)
		authDecision(r, tier, "deny", "no_service_identity")
		writeAuthError(w, http.StatusUnauthorized, "no forwarded service caller identity from the gateway")
		return nil, true
	}
	if !slices.Contains(a.readServiceSPIFFE, caller) {
		// The caller value is unverified header input on this path — clamp it
		// like the denial-logged XFCC URIs so a synthetic value cannot bloat
		// the log line.
		a.logger.Warn("forward-auth service caller is not in the read allowlist; denying",
			"path", r.URL.Path, "remote_addr", r.RemoteAddr, "gateway", gatewayID, "caller", clampLoggedHeaderValue(caller))
		authDecision(r, tier, "deny", "service_not_authorized")
		writeAuthError(w, http.StatusForbidden, "service caller is not authorized for read access")
		return nil, true
	}
	if tier == TierWrite {
		a.logger.Warn("forward-auth service caller denied for write operation; service callers are read-only",
			"path", r.URL.Path, "remote_addr", r.RemoteAddr, "gateway", gatewayID, "caller", caller)
		authDecision(r, tier, "deny", "service_caller_write")
		writeAuthError(w, http.StatusForbidden, "service callers are limited to read-only operations")
		return nil, true
	}

	authDecision(r, tier, "allow", "")
	return &User{Subject: caller}, true
}

// canRead reports whether the caller may use read-tier endpoints. With no
// configured read groups, reads are open to any authenticated caller; otherwise
// the caller must be in a read group (write-group and database-operator members
// can always read — anyone who can mutate must be able to watch the result).
func (a *ForwardAuthAuthorizer) canRead(groups []string) bool {
	if len(a.readGroups) == 0 {
		return true
	}
	return matchesAnyGroup(groups, a.readGroups) ||
		matchesAnyGroup(groups, a.writeGroups) ||
		matchesAnyGroup(groups, a.operatorGroups)
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
		ip, ok := a.trustedSourceIP(r)
		if !ok {
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

// trustedSourceIP parses the request's source IP and reports whether it falls
// inside a configured trusted network. The source address is a transport
// property the caller cannot forge, which is why both the proxy lane and the
// gateway lane anchor on it whenever CIDRs are configured.
func (a *ForwardAuthAuthorizer) trustedSourceIP(r *http.Request) (net.IP, bool) {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	ip := net.ParseIP(strings.TrimSpace(host))
	if ip == nil {
		return nil, false
	}
	for _, network := range a.trustedNets {
		if network.Contains(ip) {
			return ip, true
		}
	}
	return nil, false
}

// clampLoggedHeaderValue bounds a single untrusted header value recorded in a
// denial log, mirroring the per-value bound on logged XFCC URIs.
func clampLoggedHeaderValue(v string) string {
	if len(v) > maxLoggedXFCCURIBytes {
		return v[:maxLoggedXFCCURIBytes] + "…"
	}
	return v
}

// maxLoggedXFCCURIs and maxLoggedXFCCURIBytes bound a denial log in both
// dimensions — how many XFCC URI values it records and how long each recorded
// value may be — so a hostile caller cannot bloat log lines with a synthetic
// header (Go's default request-header limit otherwise allows a single quoted
// URI value near 1MB). Legitimate chains carry one URI per hop and real SPIFFE
// IDs stay far below both bounds.
const (
	maxLoggedXFCCURIs     = 8
	maxLoggedXFCCURIBytes = 512
)

// clampLoggedXFCCURIs bounds the URI (SPIFFE SVID) values a denial log records
// to maxLoggedXFCCURIs values of at most maxLoggedXFCCURIBytes each (oversized
// values end in "…") and returns the unclamped total. Only parsed URI values
// are logged — never the raw header — and on a denied request they are
// unverified caller input: a triage signal, not verified certificate
// identities. Like the trust check itself, only the first XFCC header line is
// parsed, so a zero count means the header the trust decision evaluated
// carried no URI identity.
func clampLoggedXFCCURIs(uris []string) ([]string, int) {
	total := len(uris)
	if total > maxLoggedXFCCURIs {
		uris = uris[:maxLoggedXFCCURIs]
	}
	for i, uri := range uris {
		if len(uri) > maxLoggedXFCCURIBytes {
			uris[i] = uri[:maxLoggedXFCCURIBytes] + "…"
		}
	}
	return uris, total
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
// while respecting double-quoted values. Envoy escapes embedded quotes inside
// a quoted value as \" — the escape is consumed as part of the value so it
// cannot end the quoted section and desync the tokenizer.
func splitXFCC(header string) []string {
	var (
		tokens  []string
		current strings.Builder
		inQuote bool
	)
	for i := 0; i < len(header); i++ {
		c := header[i]
		switch {
		case c == '\\' && inQuote && i+1 < len(header):
			current.WriteByte(c)
			i++
			current.WriteByte(header[i])
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
