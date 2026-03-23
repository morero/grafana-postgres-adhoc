package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/jackc/pgx/v5/pgxpool"
)

var logger = log.DefaultLogger

type Datasource struct {
	pool     *pgxpool.Pool
	settings DatasourceSettings
}

type DatasourceSettings struct {
	Host        string `json:"host"`
	Port        int    `json:"port"`
	Database    string `json:"database"`
	User        string `json:"user"`
	SSLMode     string `json:"sslmode"`
	Timescaledb bool   `json:"timescaledb"`
	AdhocTable  string `json:"adhocTable"`
	AdhocSchema string `json:"adhocSchema"`
}

func NewDatasource(ctx context.Context, settings backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	var ds DatasourceSettings
	if err := json.Unmarshal(settings.JSONData, &ds); err != nil {
		return nil, fmt.Errorf("unmarshal settings: %w", err)
	}

	if ds.Database == "" {
		ds.Database = settings.Database
	}
	if ds.SSLMode == "" {
		ds.SSLMode = "disable"
	}

	// Fallback: parse host/port from the URL field if not in jsonData
	if ds.Host == "" && settings.URL != "" {
		parts := strings.SplitN(settings.URL, ":", 2)
		ds.Host = parts[0]
		if len(parts) > 1 {
			if p, err := strconv.Atoi(parts[1]); err == nil {
				ds.Port = p
			}
		}
	}
	if ds.Port == 0 {
		ds.Port = 5432
	}
	if ds.AdhocSchema == "" {
		ds.AdhocSchema = "public"
	}
	if ds.AdhocTable == "" {
		ds.AdhocTable = "wide_events"
	}

	password := settings.DecryptedSecureJSONData["password"]

	connStr := fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		ds.Host, ds.Port, ds.Database, ds.User, password, ds.SSLMode,
	)

	logger.Info("Connecting to PostgreSQL", "host", ds.Host, "port", ds.Port, "database", ds.Database, "user", ds.User, "connStr_redacted", fmt.Sprintf("host=%s port=%d dbname=%s user=%s sslmode=%s", ds.Host, ds.Port, ds.Database, ds.User, ds.SSLMode))

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	logger.Info("Connected to PostgreSQL", "host", ds.Host, "database", ds.Database)

	return &Datasource{pool: pool, settings: ds}, nil
}

func (d *Datasource) Dispose() {
	if d.pool != nil {
		d.pool.Close()
	}
}

func (d *Datasource) CheckHealth(ctx context.Context, req *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	err := d.pool.Ping(ctx)
	if err != nil {
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: fmt.Sprintf("Database connection failed: %v", err),
		}, nil
	}
	return &backend.CheckHealthResult{
		Status:  backend.HealthStatusOk,
		Message: "Database connection OK",
	}, nil
}

func (d *Datasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	response := backend.NewQueryDataResponse()

	for _, q := range req.Queries {
		res := d.executeQuery(ctx, q, req.Queries[0].TimeRange)
		response.Responses[q.RefID] = res
	}

	return response, nil
}

func (d *Datasource) CallResource(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	switch req.Path {
	case "tag-keys":
		return d.handleTagKeys(ctx, sender)
	case "tag-values":
		key := ""
		if req.URL != "" {
			// Parse key from query string
			if parsed, err := http.NewRequest("GET", "http://localhost?"+req.URL, nil); err == nil {
				key = parsed.URL.Query().Get("key")
			}
		}
		if key == "" {
			// Try parsing from body
			var body map[string]string
			if err := json.Unmarshal(req.Body, &body); err == nil {
				key = body["key"]
			}
		}
		return d.handleTagValues(ctx, key, sender)
	default:
		return sender.Send(&backend.CallResourceResponse{Status: 404, Body: []byte("not found")})
	}
}
