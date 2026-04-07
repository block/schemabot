-- Branch metadata for LocalScale's fake PlanetScale API.
-- Each row represents a database branch with its schema snapshot and VSchema data.

CREATE TABLE localscale_branches (
    org VARCHAR(255) NOT NULL DEFAULT '',
    database_name VARCHAR(255) NOT NULL DEFAULT '',
    name VARCHAR(255) NOT NULL,
    parent_branch VARCHAR(255) NOT NULL DEFAULT '',
    region VARCHAR(255) NOT NULL DEFAULT 'us-east-1',
    ready BOOLEAN NOT NULL DEFAULT FALSE,
    error_message TEXT,
    vschema_data TEXT,
    instant_ddl_eligible BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (org, database_name, name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
