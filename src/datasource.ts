import {
  DataSourceInstanceSettings,
  DataQueryRequest,
  DataQueryResponse,
  MetricFindValue,
} from '@grafana/data';
import { DataSourceWithBackend, getTemplateSrv } from '@grafana/runtime';
import { Observable } from 'rxjs';

export interface PostgresQuery {
  refId: string;
  rawSql: string;
  format: string;
  adhocFilters?: AdhocFilter[];
}

interface AdhocFilter {
  key: string;
  operator: string;
  value: string;
}

export class PostgresAdhocDatasource extends DataSourceWithBackend<PostgresQuery> {
  constructor(instanceSettings: DataSourceInstanceSettings) {
    super(instanceSettings);
  }

  // Tell Grafana we support ad-hoc filters
  async getTagKeys(): Promise<MetricFindValue[]> {
    return this.getResource('tag-keys');
  }

  async getTagValues(options: { key: string }): Promise<MetricFindValue[]> {
    return this.getResource('tag-values', { key: options.key });
  }

  // Inject ad-hoc filters and expand template variables before sending to backend
  query(request: DataQueryRequest<PostgresQuery>): Observable<DataQueryResponse> {
    const templateSrv = getTemplateSrv() as any;
    const adhocFilters: AdhocFilter[] = templateSrv.getAdhocFilters
      ? templateSrv.getAdhocFilters(this.name) || []
      : [];

    const targets = request.targets.map((t) => ({
      ...t,
      // Expand template variables on the frontend
      rawSql: getTemplateSrv().replace(t.rawSql || '', request.scopedVars),
      adhocFilters,
    }));

    return super.query({ ...request, targets });
  }

  // Support metricFindQuery for template variable queries
  async metricFindQuery(query: string): Promise<MetricFindValue[]> {
    // Not implemented for v1 — use the built-in PostgreSQL datasource for variable queries
    return [];
  }
}
