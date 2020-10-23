package com.github.thestyleofme.data.comparison.infra.handler.comparison;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.github.thestyleofme.data.comparison.domain.entity.ColMapping;
import com.github.thestyleofme.data.comparison.domain.entity.ComparisonJob;
import com.github.thestyleofme.data.comparison.infra.annotation.ComparisonType;
import com.github.thestyleofme.data.comparison.infra.exceptions.HandlerException;
import com.github.thestyleofme.data.comparison.infra.utils.JsonUtil;
import com.github.thestyleofme.driver.core.app.service.DriverSessionService;
import com.github.thestyleofme.driver.core.app.service.session.DriverSession;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/22 16:31
 * @since 1.0.0
 */
@Component
@ComparisonType("TABLE")
public class TableComparisonTypeHandler implements BaseComparisonHandler {

    private final DriverSessionService driverSessionService;

    public TableComparisonTypeHandler(DriverSessionService driverSessionService) {
        this.driverSessionService = driverSessionService;
    }

    @Override
    public ComparisonMapping handle(ComparisonJob comparisonJob) {
        Long tenantId = comparisonJob.getTenantId();
        String sourceDatasourceCode = comparisonJob.getSourceDatasourceCode();
        String sourceSchema = comparisonJob.getSourceSchema();
        String sourceTable = comparisonJob.getSourceTable();
        String targetDatasourceCode = comparisonJob.getTargetDatasourceCode();
        String targetSchema = comparisonJob.getTargetSchema();
        String targetTable = comparisonJob.getTargetTable();
        boolean anyMatch = Stream.of(tenantId, sourceDatasourceCode, sourceSchema, sourceTable,
                targetDatasourceCode, targetSchema, targetTable)
                .anyMatch(Objects::isNull);
        if (anyMatch) {
            throw new HandlerException("when comparisonType=TABLE, " +
                    "[tenantId, sourceDatasourceCode, sourceSchema, sourceTable, " +
                    "targetDatasourceCode, targetSchema, targetTable] all cannot be null");
        }
        // 封装ComparisonMapping
        ComparisonMapping comparisonMapping = new ComparisonMapping();
        handleSource(comparisonJob, comparisonMapping, tenantId, sourceDatasourceCode, sourceSchema, sourceTable);
        handleTarget(comparisonJob, comparisonMapping, tenantId, targetDatasourceCode, targetSchema, targetTable);
        return comparisonMapping;
    }

    private void handleSource(ComparisonJob comparisonJob,
                              ComparisonMapping comparisonMapping,
                              Long tenantId,
                              String sourceDatasourceCode,
                              String sourceSchema,
                              String sourceTable) {
        DriverSession sourceDriverSession = driverSessionService.getDriverSession(tenantId, sourceDatasourceCode);
        List<Map<String, Object>> sourceList = sourceDriverSession.tableQuery(sourceSchema, sourceTable);
        // 排序
        List<LinkedHashMap<String, Object>> result = sortListMap(comparisonJob, sourceList, ColMapping.SOURCE);
        comparisonMapping.setSourceDataList(result);
    }

    private void handleTarget(ComparisonJob comparisonJob, ComparisonMapping comparisonMapping,
                              Long tenantId,
                              String targetDatasourceCode,
                              String targetSchema,
                              String targetTable) {
        DriverSession targetDriverSession = driverSessionService.getDriverSession(tenantId, targetDatasourceCode);
        List<Map<String, Object>> targetList = targetDriverSession.tableQuery(targetSchema, targetTable);
        // 排序
        List<LinkedHashMap<String, Object>> result = sortListMap(comparisonJob, targetList, ColMapping.TARGET);
        comparisonMapping.setTargetDataList(result);
    }

    private List<LinkedHashMap<String, Object>> sortListMap(ComparisonJob comparisonJob,
                                                        List<Map<String, Object>> list,
                                                        String position) {
        String colMapping = comparisonJob.getColMapping();
        List<LinkedHashMap<String, Object>> result = new ArrayList<>(list.size());
        if (StringUtils.isEmpty(colMapping)) {
            SortedMap<String, Object> sortedMap;
            LinkedHashMap<String,Object> linkedHashMap;
            for (Map<String, Object> map : list) {
                sortedMap = new TreeMap<>(Comparator.reverseOrder());
                sortedMap.putAll(map);
                linkedHashMap = new LinkedHashMap<>(sortedMap);
                result.add(linkedHashMap);
            }
            return result;
        }
        // 根据ColMapping的index进行排序
        List<ColMapping> colMappingList = JsonUtil.toObj(colMapping, new TypeReference<List<ColMapping>>() {
        });
        LinkedHashMap<String, Object> linkedHashMap = colMappingList.stream()
                .sorted(Comparator.comparingInt(ColMapping::getIndex))
                .collect(Collectors.toMap(
                        o -> {
                            if (ColMapping.SOURCE.equals(position)) {
                                return o.getSourceCol();
                            }
                            return o.getTargetCol();
                        },
                        ColMapping::getIndex,
                        (oldValue, newValue) -> oldValue,
                        LinkedHashMap::new));
        LinkedHashMap<String, Object> temp;
        for (Map<String, Object> map : list) {
            linkedHashMap.putAll(map);
            temp = new LinkedHashMap<>(linkedHashMap);
            result.add(temp);
        }
        return result;
    }
}
