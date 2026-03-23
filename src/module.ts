import { DataSourcePlugin } from '@grafana/data';
import { PostgresAdhocDatasource } from './datasource';

export const plugin = new DataSourcePlugin(PostgresAdhocDatasource as any);
