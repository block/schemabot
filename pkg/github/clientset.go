package github

import (
	"fmt"
	"maps"
	"sort"
)

// ClientSet holds one or more GitHub App client factories keyed by the App's
// logical name (the same key used in api.ServerConfig.Apps). Webhook dispatch
// resolves a repository to its owning App, looks up the factory via For, and
// then mints an installation-scoped client via the returned factory.
//
// Today the production path constructs a single-entry ClientSet keyed
// "default" from the legacy ServerConfig.GitHub field; the multi-App
// dispatcher that fans out across N entries lands in a follow-up PR.
type ClientSet struct {
	clients map[string]GitHubClientFactory
}

// NewClientSet returns a ClientSet that owns the given factories. The
// underlying map is copied so subsequent mutations by the caller do not
// affect the set.
func NewClientSet(clients map[string]GitHubClientFactory) ClientSet {
	cs := ClientSet{clients: make(map[string]GitHubClientFactory, len(clients))}
	maps.Copy(cs.clients, clients)
	return cs
}

// NewSingleClientSet returns a ClientSet containing one factory under the
// given name. Convenience for legacy single-App configurations and tests.
func NewSingleClientSet(name string, c GitHubClientFactory) ClientSet {
	return ClientSet{clients: map[string]GitHubClientFactory{name: c}}
}

// For returns the factory registered under appName, or an error if no such
// factory exists. The error includes the configured names so misconfiguration
// at startup or in tests fails closed with a useful message. A registered
// but nil factory is also treated as a fail-closed misconfiguration so
// callers never get a nil factory back and panic on a later ForInstallation.
func (cs ClientSet) For(appName string) (GitHubClientFactory, error) {
	if cs.clients == nil {
		return nil, fmt.Errorf("no GitHub App clients configured")
	}
	c, ok := cs.clients[appName]
	if !ok {
		return nil, fmt.Errorf("no GitHub App client for %q (configured: %v)", appName, cs.Names())
	}
	if c == nil {
		return nil, fmt.Errorf("GitHub App client for %q is nil (misconfigured)", appName)
	}
	return c, nil
}

// Names returns the configured App names in sorted order. Used in error
// messages and structured logs.
func (cs ClientSet) Names() []string {
	names := make([]string, 0, len(cs.clients))
	for name := range cs.clients {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Len returns the number of configured App clients.
func (cs ClientSet) Len() int { return len(cs.clients) }
