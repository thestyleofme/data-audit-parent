package com.github.thestyleofme.comparison.common.app.service.transform;

import java.util.List;
import java.util.Map;

import com.github.thestyleofme.comparison.common.domain.JobEnv;
import com.github.thestyleofme.comparison.common.domain.SelectTableInfo;
import com.github.thestyleofme.comparison.common.domain.SourceDataMapping;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.driver.core.app.service.DriverSessionService;
import com.github.thestyleofme.driver.core.app.service.session.DriverSession;
import com.github.thestyleofme.plugin.core.infra.utils.JsonUtil;
import org.springframework.stereotype.Component;

/**
 * <p>
 * 基于数据源获取源表目标表数据
 * </p>
 *
 * @author isaac 2020/10/22 16:31
 * @since 1.0.0
 */
@Component
public class TableDataHandler {

    private final DriverSessionService driverSessionService;

    public TableDataHandler(DriverSessionService driverSessionService) {
        this.driverSessionService = driverSessionService;
    }

    public SourceDataMapping handle(ComparisonJob comparisonJob,
                                    Map<String, Object> env) {
        Long tenantId = comparisonJob.getTenantId();
        JobEnv jobEnv = JsonUtil.toObj(JsonUtil.toJson(env), JobEnv.class);
        SelectTableInfo source = jobEnv.getSource();
        SelectTableInfo target = jobEnv.getTarget();
        String sourceDatasourceCode = source.getDataSourceCode();
        String sourceSchema = source.getSchema();
        String sourceTable = source.getTable();
        String targetDatasourceCode = target.getDataSourceCode();
        String targetSchema = target.getSchema();
        String targetTable = target.getTable();
        // 封装ComparisonMapping
        SourceDataMapping sourceDataMapping = new SourceDataMapping();
        handleSource(sourceDataMapping, tenantId, sourceDatasourceCode, sourceSchema, sourceTable);
        handleTarget(sourceDataMapping, tenantId, targetDatasourceCode, targetSchema, targetTable);
        return sourceDataMapping;
    }

    private void handleSource(SourceDataMapping sourceDataMapping,
                              Long tenantId,
                              String sourceDatasourceCode,
                              String sourceSchema,
                              String sourceTable) {
        DriverSession sourceDriverSession = driverSessionService.getDriverSession(tenantId, sourceDatasourceCode);
        List<Map<String, Object>> sourceList = sourceDriverSession.tableQuery(sourceSchema, sourceTable);
        sourceDataMapping.setSourceDataList(sourceList);
    }

    private void handleTarget(SourceDataMapping sourceDataMapping,
                              Long tenantId,
                              String targetDatasourceCode,
                              String targetSchema,
                              String targetTable) {
        DriverSession targetDriverSession = driverSessionService.getDriverSession(tenantId, targetDatasourceCode);
        List<Map<String, Object>> targetList = targetDriverSession.tableQuery(targetSchema, targetTable);
        sourceDataMapping.setTargetDataList(targetList);
    }
}
