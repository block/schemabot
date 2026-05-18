package api

import (
	"net/http"
	"sort"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/storage"
)

// HandleListMysqlDatabases is the HTTP handler for GET /api/mysql/databases.
func (s *Service) HandleListMysqlDatabases(w http.ResponseWriter, r *http.Request) {
	s.handleListMysqlDatabases(w, r)
}

func (s *Service) handleListMysqlDatabases(w http.ResponseWriter, r *http.Request) {
	environment := r.URL.Query().Get("environment")
	resp := s.ListMysqlDatabases(environment)
	s.writeJSON(w, http.StatusOK, resp)
}

// ListMysqlDatabases returns MySQL databases from the local SchemaBot catalog.
func (s *Service) ListMysqlDatabases(environmentFilter string) *apitypes.ListMysqlDatabasesResponse {
	databases := make([]*apitypes.MysqlDatabaseResponse, 0, len(s.config.Databases))
	for name, dbConfig := range s.config.Databases {
		if dbConfig.Type != storage.DatabaseTypeMySQL {
			s.logger.Debug("skipping non-MySQL database in inventory list",
				"database", name,
				"database_type", dbConfig.Type)
			continue
		}

		environments := matchingEnvironments(dbConfig.Environments, environmentFilter)
		if len(environments) == 0 {
			s.logger.Debug("skipping MySQL database with no matching environments",
				"database", name,
				"environment_filter", environmentFilter)
			continue
		}

		databases = append(databases, &apitypes.MysqlDatabaseResponse{
			Database:     name,
			DatabaseType: dbConfig.Type,
			Deployment:   name,
			Environments: environments,
		})
	}

	sort.Slice(databases, func(i, j int) bool {
		return databases[i].Database < databases[j].Database
	})

	return &apitypes.ListMysqlDatabasesResponse{
		Databases: databases,
		Count:     len(databases),
	}
}

func matchingEnvironments(configs map[string]EnvironmentConfig, environmentFilter string) []string {
	environments := make([]string, 0, len(configs))
	for environment := range configs {
		if environmentFilter == "" || environment == environmentFilter {
			environments = append(environments, environment)
		}
	}
	sort.Strings(environments)
	return environments
}
