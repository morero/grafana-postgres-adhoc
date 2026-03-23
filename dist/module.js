define(["react","@grafana/data","@grafana/runtime","rxjs"],function(e,t,n,r){
  "use strict";
  class PostgresAdhocDatasource extends n.DataSourceWithBackend {
    constructor(e){super(e)}
    async getTagKeys(){return this.getResource("tag-keys")}
    async getTagValues(e){return this.getResource("tag-values",{key:e.key})}
    query(e){
      const s=n.getTemplateSrv();
      const f=s.getAdhocFilters?s.getAdhocFilters(this.name)||[]:[];
      const targets=e.targets.map(t=>({...t,rawSql:s.replace(t.rawSql||"",e.scopedVars),adhocFilters:f}));
      return super.query({...e,targets})
    }
  }
  return{plugin:new t.DataSourcePlugin(PostgresAdhocDatasource)}
});
