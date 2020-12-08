package com.github.thestyleofme.comparison.common.app.service.transform;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.github.thestyleofme.comparison.common.domain.ColMapping;
import com.github.thestyleofme.comparison.common.domain.ComparisonInfo;
import com.github.thestyleofme.comparison.common.domain.SelectTableInfo;
import com.github.thestyleofme.comparison.common.domain.SourceDataMapping;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.infra.utils.CommonUtil;
import com.github.thestyleofme.comparison.common.infra.utils.SqlUtils;
import com.github.thestyleofme.driver.core.app.service.DriverSessionService;
import com.github.thestyleofme.driver.core.app.service.session.DriverSession;
import lombok.extern.slf4j.Slf4j;
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
@Slf4j
public class TableDataHandler {

    private final DriverSessionService driverSessionService;

    public TableDataHandler(DriverSessionService driverSessionService) {
        this.driverSessionService = driverSessionService;
    }

    public SourceDataMapping handle(ComparisonJob comparisonJob,
                                    Map<String, Object> env) {
        Long tenantId = comparisonJob.getTenantId();
        ComparisonInfo comparisonInfo = CommonUtil.getComparisonInfo(env);
        List<ColMapping> colMapping = comparisonInfo.getColMapping();
        StringBuilder builder = new StringBuilder();
        // source
        SelectTableInfo source = comparisonInfo.getSource();
        String sourceTableName = comparisonInfo.getSourceTableName();
        String sourceDatasourceCode = source.getDataSourceCode();
        String sourceSchema = source.getSchema();
        String sourceWhere = SqlUtils.getOneWhereCondition(builder, source.getGlobalWhere(), source.getWhere());
        String sourceCol = colMapping.stream()
                .map(mapping -> String.format("_a.%s", mapping.getSourceCol()))
                .collect(Collectors.joining(","));
        // target
        SelectTableInfo target = comparisonInfo.getTarget();
        String targetTableName = comparisonInfo.getTargetTableName();
        String targetDatasourceCode = target.getDataSourceCode();
        String targetSchema = target.getSchema();
        String targetWhere = SqlUtils.getOneWhereCondition(builder, target.getGlobalWhere(), target.getWhere());
        String targetCol = colMapping.stream()
                .map(mapping -> String.format("_b.%s", mapping.getTargetCol()))
                .collect(Collectors.joining(","));

        // 封装SourceDataMapping
        SourceDataMapping sourceDataMapping = new SourceDataMapping();
        handleSource(sourceDataMapping, tenantId, sourceDatasourceCode, sourceSchema, sourceTableName, sourceWhere, sourceCol);
        handleTarget(sourceDataMapping, tenantId, targetDatasourceCode, targetSchema, targetTableName, targetWhere, targetCol);
        return sourceDataMapping;
    }

    private void handleSource(SourceDataMapping sourceDataMapping,
                              Long tenantId,
                              String datasourceCode,
                              String schema,
                              String tableName,
                              String whereCondition,
                              String col) {
        String sql = String.format("select %s from %s as _a %s", col, tableName, whereCondition);
        log.info("java transform handlerSource sql:{}", sql);
        DriverSession sourceDriverSession = driverSessionService.getDriverSession(tenantId, datasourceCode);
        List<Map<String, Object>> sourceList = sourceDriverSession.executeOneQuery(schema, sql);
        sourceDataMapping.setSourceDataList(sourceList);
    }

    private void handleTarget(SourceDataMapping sourceDataMapping,
                              Long tenantId,
                              String datasourceCode,
                              String schema,
                              String tableName,
                              String whereCondition,
                              String col) {
        String sql = String.format("select %s from %s as _b %s", col, tableName, whereCondition);
        log.info("java transform handlerTarget sql:{}", sql);
        DriverSession targetDriverSession = driverSessionService.getDriverSession(tenantId, datasourceCode);
        List<Map<String, Object>> targetList = targetDriverSession.executeOneQuery(schema, sql);
        sourceDataMapping.setTargetDataList(targetList);
    }
}
