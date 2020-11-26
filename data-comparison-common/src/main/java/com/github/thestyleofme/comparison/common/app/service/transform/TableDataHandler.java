package com.github.thestyleofme.comparison.common.app.service.transform;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.github.thestyleofme.comparison.common.domain.ColMapping;
import com.github.thestyleofme.comparison.common.domain.JobEnv;
import com.github.thestyleofme.comparison.common.domain.SourceDataMapping;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.infra.utils.TransformUtils;
import com.github.thestyleofme.driver.core.app.service.DriverSessionService;
import com.github.thestyleofme.driver.core.app.service.session.DriverSession;
import com.github.thestyleofme.plugin.core.infra.utils.BeanUtils;
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
        JobEnv jobEnv = BeanUtils.map2Bean(env, JobEnv.class);
        String sourceDatasourceCode = jobEnv.getSourceDatasourceCode();
        String sourceSchema = jobEnv.getSourceSchema();
        String sourceTable = jobEnv.getSourceTable();
        String targetDatasourceCode = jobEnv.getTargetDatasourceCode();
        String targetSchema = jobEnv.getTargetSchema();
        String targetTable = jobEnv.getTargetTable();
        // 封装ComparisonMapping
        SourceDataMapping sourceDataMapping = new SourceDataMapping();
        handleSource(jobEnv, sourceDataMapping, tenantId, sourceDatasourceCode, sourceSchema, sourceTable);
        handleTarget(jobEnv, sourceDataMapping, tenantId, targetDatasourceCode, targetSchema, targetTable);
        return sourceDataMapping;
    }

    private void handleSource(JobEnv jobEnv,
                              SourceDataMapping sourceDataMapping,
                              Long tenantId,
                              String sourceDatasourceCode,
                              String sourceSchema,
                              String sourceTable) {
        DriverSession sourceDriverSession = driverSessionService.getDriverSession(tenantId, sourceDatasourceCode);
        List<Map<String, Object>> sourceList = sourceDriverSession.tableQuery(sourceSchema, sourceTable);
        // 排序
        List<LinkedHashMap<String, Object>> result = TransformUtils.sortListMap(jobEnv, sourceList, ColMapping.SOURCE);
        sourceDataMapping.setSourceDataList(result);
    }

    private void handleTarget(JobEnv jobEnv,
                              SourceDataMapping sourceDataMapping,
                              Long tenantId,
                              String targetDatasourceCode,
                              String targetSchema,
                              String targetTable) {
        DriverSession targetDriverSession = driverSessionService.getDriverSession(tenantId, targetDatasourceCode);
        List<Map<String, Object>> targetList = targetDriverSession.tableQuery(targetSchema, targetTable);
        // 排序
        List<LinkedHashMap<String, Object>> result = TransformUtils.sortListMap(jobEnv, targetList, ColMapping.TARGET);
        sourceDataMapping.setTargetDataList(result);
    }
}
