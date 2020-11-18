package com.github.thestyleofme.comparison.source.handler;

import static com.github.thestyleofme.comparison.common.infra.utils.CommonUtil.requireNonNullElse;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.github.thestyleofme.comparison.common.app.service.source.BaseSourceHandler;
import com.github.thestyleofme.comparison.common.app.service.source.SourceDataMapping;
import com.github.thestyleofme.comparison.common.domain.ColMapping;
import com.github.thestyleofme.comparison.common.domain.JobEnv;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.infra.annotation.SourceType;
import com.github.thestyleofme.comparison.common.infra.utils.TransformUtils;
import com.github.thestyleofme.comparison.source.pojo.TableInfo;
import com.github.thestyleofme.driver.core.app.service.DriverSessionService;
import com.github.thestyleofme.driver.core.app.service.session.DriverSession;
import com.github.thestyleofme.plugin.core.infra.utils.BeanUtils;
import org.springframework.stereotype.Component;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/22 16:31
 * @since 1.0.0
 */
@Component
@SourceType("TABLE")
public class TableSourceTypeHandler implements BaseSourceHandler {

    private final DriverSessionService driverSessionService;

    public TableSourceTypeHandler(DriverSessionService driverSessionService) {
        this.driverSessionService = driverSessionService;
    }

    @Override
    public SourceDataMapping handle(ComparisonJob comparisonJob,
                                    Map<String, Object> env,
                                    Map<String, Object> sourceMap) {
        Long tenantId = comparisonJob.getTenantId();
        TableInfo tableInfo = BeanUtils.map2Bean(sourceMap, TableInfo.class);
        JobEnv jobEnv = BeanUtils.map2Bean(env, JobEnv.class);
        // 优先从tableInfo取 取不到从jobEnv取
        String sourceDatasourceCode = requireNonNullElse(tableInfo.getSourceDatasourceCode(), jobEnv.getSourceDatasourceCode());
        String sourceSchema = requireNonNullElse(tableInfo.getSourceSchema(), jobEnv.getSourceSchema());
        String sourceTable = requireNonNullElse(tableInfo.getSourceTable(), jobEnv.getSourceTable());
        String targetDatasourceCode = requireNonNullElse(tableInfo.getTargetDatasourceCode(), jobEnv.getTargetDatasourceCode());
        String targetSchema = requireNonNullElse(tableInfo.getTargetSchema(), jobEnv.getTargetSchema());
        String targetTable = requireNonNullElse(tableInfo.getTargetTable(), jobEnv.getTargetTable());
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
