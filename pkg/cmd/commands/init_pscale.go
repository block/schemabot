package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/block/mysql"
	"github.com/google/uuid"
)

var initPlanetScalePermissions = []string{
	"read_branch", "create_branch", "connect_branch", "delete_branch",
	"read_deploy_request", "create_deploy_request", "write_branch_vschema",
}

type initPScale struct {
	run  func(context.Context, ...string) ([]byte, error)
	save func(string, string) (string, error)
}

type initPScaleCredentials struct{ dsn, token string }

// Use the user's pscale login, never a service-token override inherited from the
// shell. Secrets are returned over stdout to the caller, never put in argv.
func initPScaleCommand(ctx context.Context, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "pscale", args...)
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if key != "PLANETSCALE_SERVICE_TOKEN" && key != "PLANETSCALE_SERVICE_TOKEN_ID" && key != "PLANETSCALE_API_TOKEN" {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	return cmd
}

func runInitPScale(ctx context.Context, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cmd := initPScaleCommand(ctx, append(args, "--format", "json", "--api-url", "https://api.planetscale.com")...)
	cmd.Stderr = io.Discard // Provider errors can contain credentials or request bodies.
	data, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("PlanetScale CLI could not complete %s %s; check your login, network, and permissions", args[0], args[1])
	}
	return data, nil
}

func (p initPScale) organizations(ctx context.Context) ([]string, error) {
	data, err := p.run(ctx, "org", "list")
	if err != nil {
		return nil, err
	}
	var orgs []struct {
		Name string `json:"name"`
	}
	if json.Unmarshal(data, &orgs) != nil {
		return nil, fmt.Errorf("could not read organizations from PlanetScale CLI")
	}
	var names []string
	for _, org := range orgs {
		if initName.MatchString(org.Name) {
			names = append(names, org.Name)
		}
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("no organizations found; check your PlanetScale account")
	}
	return names, nil
}

func (p initPScale) databases(ctx context.Context, org string) ([]string, error) {
	var names []string
	for page := 1; page <= 1000; page++ {
		data, err := p.run(ctx, "database", "list", "--org", org, "--page", strconv.Itoa(page), "--per-page", "100")
		if err != nil {
			return nil, err
		}
		var databases []struct {
			Name string `json:"name"`
			Kind string `json:"kind"`
		}
		if json.Unmarshal(data, &databases) != nil {
			return nil, fmt.Errorf("could not read databases from PlanetScale CLI")
		}
		for _, db := range databases {
			if db.Kind == "mysql" && initName.MatchString(db.Name) {
				names = append(names, db.Name)
			}
		}
		if len(databases) < 100 {
			if len(names) == 0 {
				return nil, fmt.Errorf("no Vitess databases found in this organization")
			}
			return names, nil
		}
	}
	return nil, fmt.Errorf("database listing is too large; use manual setup")
}

// Credential creation is explicitly approved in the wizard. On known partial
// failures, revoke only resources created by this attempt, using a fresh context
// so cancellation cannot prevent cleanup. Never repeat a failed create blindly.
func (p initPScale) provision(ctx context.Context, org, database string) (result initPScaleCredentials, err error) {
	name := "schemabot-" + uuid.NewString()
	var token struct {
		ID    string `json:"id"`
		Token string `json:"token"`
	}
	var password struct {
		ID       string `json:"id"`
		Host     string `json:"access_host_url"`
		User     string `json:"username"`
		Password string `json:"plain_text"`
	}
	defer func() {
		if err == nil {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		cleanupFailed := false
		if password.ID != "" {
			_, e := p.run(cleanupCtx, "password", "delete", database, "main", password.ID, "--org", org, "--force")
			cleanupFailed = cleanupFailed || e != nil
		}
		if token.ID != "" {
			_, e := p.run(cleanupCtx, "service-token", "delete", token.ID, "--org", org)
			cleanupFailed = cleanupFailed || e != nil
		}
		if cleanupFailed {
			err = fmt.Errorf("%w; remove credentials named %s in PlanetScale before retrying", err, name)
		}
	}()
	data, err := p.run(ctx, "service-token", "create", "--name", name, "--org", org)
	if err != nil {
		return result, fmt.Errorf("%w; check PlanetScale for a token named %s before retrying", err, name)
	}
	if json.Unmarshal(data, &token) != nil || token.ID == "" || token.Token == "" {
		return result, fmt.Errorf("could not read the created service token; check credentials named %s in PlanetScale", name)
	}
	args := append([]string{"service-token", "add-access", token.ID}, initPlanetScalePermissions...)
	args = append(args, "--database", database, "--org", org)
	if _, err = p.run(ctx, args...); err != nil {
		return result, err
	}
	data, err = p.run(ctx, "password", "create", database, "main", name, "--org", org, "--role", "reader")
	if err != nil {
		return result, fmt.Errorf("%w; check PlanetScale for a password named %s before retrying", err, name)
	}
	if json.Unmarshal(data, &password) != nil || password.ID == "" || password.Host == "" || password.User == "" || password.Password == "" {
		return result, fmt.Errorf("could not read the created database password; check credentials named %s in PlanetScale", name)
	}
	cfg := mysql.NewConfig()
	cfg.Net = "tcp"
	cfg.Addr = net.JoinHostPort(password.Host, "3306")
	cfg.User = password.User
	cfg.Passwd = password.Password
	cfg.DBName = database
	cfg.TLSConfig = "true"
	result.token, err = p.save(name+"-token", token.ID+":"+token.Token)
	if err != nil {
		return result, fmt.Errorf("could not save the service token privately")
	}
	result.dsn, err = p.save(name+"-database", cfg.FormatDSN())
	if err != nil {
		return result, fmt.Errorf("could not save the database connection privately")
	}
	return result, nil
}
