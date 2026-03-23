package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
)

type AdhocFilter struct {
	Key      string `json:"key"`
	Operator string `json:"operator"`
	Value    string `json:"value"`
}

type TagKey struct {
	Text string `json:"text"`
	Type string `json:"type,omitempty"`
}

type TagValue struct {
	Text string `json:"text"`
}

func (d *Datasource) handleTagKeys(ctx context.Context, sender backend.CallResourceResponseSender) error {
	schema := d.settings.AdhocSchema
	table := d.settings.AdhocTable

	// Get regular columns
	query := `SELECT column_name, data_type FROM information_schema.columns 
	          WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position`
	rows, err := d.pool.Query(ctx, query, schema, table)
	if err != nil {
		return sender.Send(&backend.CallResourceResponse{
			Status: 500, Body: []byte(fmt.Sprintf(`{"error":"%s"}`, err.Error())),
		})
	}
	defer rows.Close()

	var keys []TagKey
	for rows.Next() {
		var name, dtype string
		if err := rows.Scan(&name, &dtype); err != nil {
			continue
		}
		keys = append(keys, TagKey{Text: name, Type: dtype})
	}

	// Also get JSONB keys from the 'attributes' column if it exists
	jsonbQuery := fmt.Sprintf(
		`SELECT DISTINCT jsonb_object_keys(attributes) AS key FROM %s.%s WHERE timestamp > now() - interval '1 hour' LIMIT 100`,
		quoteIdentifier(schema), quoteIdentifier(table),
	)
	jsonbRows, err := d.pool.Query(ctx, jsonbQuery)
	if err == nil {
		defer jsonbRows.Close()
		for jsonbRows.Next() {
			var key string
			if err := jsonbRows.Scan(&key); err != nil {
				continue
			}
			keys = append(keys, TagKey{Text: "attributes." + key, Type: "jsonb"})
		}
	}

	body, _ := json.Marshal(keys)
	return sender.Send(&backend.CallResourceResponse{Status: 200, Body: body})
}

func (d *Datasource) handleTagValues(ctx context.Context, key string, sender backend.CallResourceResponseSender) error {
	schema := d.settings.AdhocSchema
	table := d.settings.AdhocTable

	var query string
	if strings.HasPrefix(key, "attributes.") {
		// JSONB key
		jsonKey := strings.TrimPrefix(key, "attributes.")
		query = fmt.Sprintf(
			`SELECT DISTINCT attributes->>'%s' AS value FROM %s.%s WHERE attributes->>'%s' IS NOT NULL ORDER BY 1 LIMIT 500`,
			sanitizeIdentifier(jsonKey), quoteIdentifier(schema), quoteIdentifier(table), sanitizeIdentifier(jsonKey),
		)
	} else {
		// Regular column
		query = fmt.Sprintf(
			`SELECT DISTINCT "%s"::text AS value FROM %s.%s WHERE "%s" IS NOT NULL ORDER BY 1 LIMIT 500`,
			sanitizeIdentifier(key), quoteIdentifier(schema), quoteIdentifier(table), sanitizeIdentifier(key),
		)
	}

	rows, err := d.pool.Query(ctx, query)
	if err != nil {
		return sender.Send(&backend.CallResourceResponse{
			Status: 500, Body: []byte(fmt.Sprintf(`{"error":"%s"}`, err.Error())),
		})
	}
	defer rows.Close()

	var values []TagValue
	for rows.Next() {
		var val *string
		if err := rows.Scan(&val); err != nil || val == nil {
			continue
		}
		values = append(values, TagValue{Text: *val})
	}

	body, _ := json.Marshal(values)
	return sender.Send(&backend.CallResourceResponse{Status: 200, Body: body})
}

func injectAdhocFilters(rawSQL string, filters []AdhocFilter) string {
	if len(filters) == 0 {
		return rawSQL
	}

	var conditions []string
	for _, f := range filters {
		cond := buildCondition(f)
		if cond != "" {
			conditions = append(conditions, cond)
		}
	}

	if len(conditions) == 0 {
		return rawSQL
	}

	adhocClause := strings.Join(conditions, " AND ")
	return injectWhereClause(rawSQL, adhocClause)
}

func buildCondition(f AdhocFilter) string {
	val := escapeSQLString(f.Value)

	if strings.HasPrefix(f.Key, "attributes.") {
		jsonKey := strings.TrimPrefix(f.Key, "attributes.")
		col := fmt.Sprintf("attributes->>'%s'", sanitizeIdentifier(jsonKey))
		return buildComparisonSQL(col, f.Operator, val, false)
	}

	col := quoteIdentifier(f.Key)
	return buildComparisonSQL(col, f.Operator, val, true)
}

func buildComparisonSQL(col, operator, val string, supportBool bool) string {
	nullLiteral := isNullLiteral(val)
	boolLiteral := supportBool && isBoolLiteral(val)

	formatVal := func(v string) string {
		if boolLiteral {
			return strings.TrimSpace(strings.ToLower(v))
		}
		return "'" + v + "'"
	}

	switch operator {
	case "=":
		if nullLiteral {
			return col + " IS NULL"
		}
		return fmt.Sprintf("%s = %s", col, formatVal(val))
	case "!=":
		if nullLiteral {
			return col + " IS NOT NULL"
		}
		return fmt.Sprintf("%s != %s", col, formatVal(val))
	case "=~":
		return fmt.Sprintf("%s LIKE '%s'", col, val)
	case "!~":
		return fmt.Sprintf("%s NOT LIKE '%s'", col, val)
	case ">":
		return fmt.Sprintf("%s > %s", col, formatVal(val))
	case "<":
		return fmt.Sprintf("%s < %s", col, formatVal(val))
	default:
		return ""
	}
}

func injectWhereClause(rawSQL string, conditions string) string {
	upper := strings.ToUpper(rawSQL)

	terminators := []string{"GROUP BY", "ORDER BY", "LIMIT", "HAVING", "UNION"}
	findFirstTerminator := func(sql string, start int) int {
		earliest := len(sql)
		for _, kw := range terminators {
			idx := strings.Index(sql[start:], kw)
			if idx != -1 && start+idx < earliest {
				earliest = start + idx
			}
		}
		return earliest
	}

	whereIdx := strings.LastIndex(upper, "WHERE ")
	if whereIdx == -1 {
		insertAt := findFirstTerminator(upper, 0)
		return rawSQL[:insertAt] + " WHERE " + conditions + " " + rawSQL[insertAt:]
	}

	insertAt := findFirstTerminator(upper, whereIdx+6)
	return rawSQL[:insertAt] + " AND " + conditions + " " + rawSQL[insertAt:]
}

func sanitizeIdentifier(s string) string {
	s = strings.ReplaceAll(s, `"`, "")
	s = strings.ReplaceAll(s, ";", "")
	s = strings.ReplaceAll(s, "--", "")
	s = strings.ReplaceAll(s, "/*", "")
	return s
}

func quoteIdentifier(s string) string {
	return `"` + sanitizeIdentifier(s) + `"`
}

func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func isNullLiteral(s string) bool {
	return strings.TrimSpace(strings.ToLower(s)) == "null"
}

func isBoolLiteral(s string) bool {
	v := strings.TrimSpace(strings.ToLower(s))
	return v == "true" || v == "false"
}
