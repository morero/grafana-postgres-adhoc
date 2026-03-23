package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/jackc/pgx/v5"
)

type QueryModel struct {
	RawSQL       string        `json:"rawSql"`
	Format       string        `json:"format"`
	AdhocFilters []AdhocFilter `json:"adhocFilters"`
	RefID        string        `json:"refId"`
}

func (d *Datasource) executeQuery(ctx context.Context, q backend.DataQuery, tr backend.TimeRange) backend.DataResponse {
	var qm QueryModel
	if err := json.Unmarshal(q.JSON, &qm); err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, fmt.Sprintf("unmarshal query: %v", err))
	}

	sql := qm.RawSQL
	if sql == "" {
		return backend.ErrDataResponse(backend.StatusBadRequest, "empty query")
	}

	// 1. Expand macros
	sql = expandMacros(sql, tr, q.Interval)

	// 2. Inject ad-hoc filters
	sql = injectAdhocFilters(sql, qm.AdhocFilters)

	// 3. Execute
	rows, err := d.pool.Query(ctx, sql)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusInternal, fmt.Sprintf("query error: %v", err))
	}
	defer rows.Close()

	// 4. Convert to DataFrame
	frame, err := rowsToFrame(rows, q.RefID)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusInternal, fmt.Sprintf("frame error: %v", err))
	}

	return backend.DataResponse{Frames: data.Frames{frame}}
}

func rowsToFrame(rows pgx.Rows, refID string) (*data.Frame, error) {
	descs := rows.FieldDescriptions()
	if len(descs) == 0 {
		return data.NewFrame(refID), nil
	}

	// Collect all rows first
	var allRows [][]interface{}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, err
		}
		allRows = append(allRows, values)
	}

	// Build fields
	fields := make([]*data.Field, len(descs))
	for i, desc := range descs {
		name := string(desc.Name)

		if len(allRows) == 0 {
			fields[i] = data.NewField(name, nil, []*string{})
			continue
		}

		// Detect type from first non-nil value
		var sampleVal interface{}
		for _, row := range allRows {
			if row[i] != nil {
				sampleVal = row[i]
				break
			}
		}

		switch sampleVal.(type) {
		case float64, float32:
			vals := make([]*float64, len(allRows))
			for j, row := range allRows {
				if v, ok := row[i].(float64); ok {
					vals[j] = &v
				} else if v, ok := row[i].(float32); ok {
					f := float64(v)
					vals[j] = &f
				}
			}
			fields[i] = data.NewField(name, nil, vals)
		case int64, int32, int16, int:
			vals := make([]*int64, len(allRows))
			for j, row := range allRows {
				switch v := row[i].(type) {
				case int64:
					vals[j] = &v
				case int32:
					v64 := int64(v)
					vals[j] = &v64
				case int16:
					v64 := int64(v)
					vals[j] = &v64
				case int:
					v64 := int64(v)
					vals[j] = &v64
				}
			}
			fields[i] = data.NewField(name, nil, vals)
		case bool:
			vals := make([]*bool, len(allRows))
			for j, row := range allRows {
				if v, ok := row[i].(bool); ok {
					vals[j] = &v
				}
			}
			fields[i] = data.NewField(name, nil, vals)
		case time.Time:
			vals := make([]*time.Time, len(allRows))
			for j, row := range allRows {
				if v, ok := row[i].(time.Time); ok {
					vals[j] = &v
				}
			}
			fields[i] = data.NewField(name, nil, vals)
		default:
			// Fallback: convert to string
			vals := make([]*string, len(allRows))
			for j, row := range allRows {
				if row[i] != nil {
					s := fmt.Sprintf("%v", row[i])
					vals[j] = &s
				}
			}
			fields[i] = data.NewField(name, nil, vals)
		}
	}

	frame := data.NewFrame(refID, fields...)
	return frame, nil
}

func expandMacros(sql string, tr backend.TimeRange, interval time.Duration) string {
	from := tr.From.UTC().Format(time.RFC3339Nano)
	to := tr.To.UTC().Format(time.RFC3339Nano)

	// $__timeFilter(column) -> column BETWEEN 'from' AND 'to'
	re := regexp.MustCompile(`\$__timeFilter\(([^)]+)\)`)
	sql = re.ReplaceAllStringFunc(sql, func(match string) string {
		col := re.FindStringSubmatch(match)[1]
		return fmt.Sprintf("%s BETWEEN '%s' AND '%s'", strings.TrimSpace(col), from, to)
	})

	// $__timeFrom() -> 'from'
	sql = strings.ReplaceAll(sql, "$__timeFrom()", fmt.Sprintf("'%s'", from))
	sql = strings.ReplaceAll(sql, "$__timeTo()", fmt.Sprintf("'%s'", to))

	// $__timeGroup(column, interval) -> date_trunc('interval', column)
	reGroup := regexp.MustCompile(`\$__timeGroup\(([^,]+),\s*([^)]+)\)`)
	sql = reGroup.ReplaceAllStringFunc(sql, func(match string) string {
		parts := reGroup.FindStringSubmatch(match)
		col := strings.TrimSpace(parts[1])
		ivl := strings.TrimSpace(parts[2])
		return fmt.Sprintf("date_trunc('%s', %s)", ivl, col)
	})

	return sql
}
