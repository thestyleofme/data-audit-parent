package com.github.thestyleofme.comparison.common.app.service.pretransform;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.github.thestyleofme.comparison.common.app.service.transform.HandlerResult;
import com.github.thestyleofme.comparison.common.domain.AppConf;
import com.github.thestyleofme.comparison.common.domain.ComparisonInfo;
import com.github.thestyleofme.comparison.common.domain.ResultStatistics;
import com.github.thestyleofme.comparison.common.domain.SelectTableInfo;
import com.github.thestyleofme.comparison.common.domain.entity.DataInfo;
import com.github.thestyleofme.comparison.common.domain.entity.SkipCondition;
import com.github.thestyleofme.comparison.common.infra.constants.ErrorCode;
import com.github.thestyleofme.comparison.common.infra.exceptions.HandlerException;
import com.github.thestyleofme.comparison.common.infra.utils.CommonUtil;
import com.github.thestyleofme.driver.core.app.service.DriverSessionService;
import com.github.thestyleofme.driver.core.app.service.session.DriverSession;
import com.github.thestyleofme.plugin.core.infra.utils.JsonUtil;
import org.springframework.stereotype.Component;

/**
 * <p>通过数据源进行预处理</p>
 *
 * @author hsq 2020/12/04 15:44
 * @since 1.0.0
 */
@Component
public class DefaultPreTransformHook extends BasePreTransformHook {

    private static final String TABLE_NAME_FT = "%s.%s";
    private final DriverSessionService driverSessionService;

    public DefaultPreTransformHook(DriverSessionService driverSessionService) {
        this.driverSessionService = driverSessionService;
    }

    @Override
    public String getName() {
        return "java";
    }

    @Override
    protected DataInfo prepareDataInfo(Long tenantId, AppConf appConf) {
        return CommonUtil.getComparisonInfo(appConf.getEnv());
    }

    @Override
    protected List<String> generateSqlByCondition(DataInfo dataInfo, List<SkipCondition> skipConditionList) {
        ComparisonInfo comparisonInfo = (ComparisonInfo) dataInfo;
        String sourceTableName = String.format(TABLE_NAME_FT, comparisonInfo.getSource().getSchema(),
                comparisonInfo.getSource().getTable());
        String targetTableName = String.format(TABLE_NAME_FT, comparisonInfo.getTarget().getSchema(),
                comparisonInfo.getTarget().getTable());
        return skipConditionList.stream().map(condition ->
                String.format("select %s as _cdt1 from %s as _a;&select %s as _cdt2 from %s as _b;&%s",
                        condition.getSource(), sourceTableName,
                        condition.getTarget(), targetTableName,
                        condition.getOperation().trim()))
                .collect(Collectors.toList());
    }


    @Override
    protected boolean execSqlAndComputeSkip(Long tenantId, DataInfo dataInfo,
                                            List<String> sqlList,
                                            HandlerResult handlerResult) {
        ComparisonInfo comparisonInfo = (ComparisonInfo) dataInfo;
        ResultStatistics resultStatistics = handlerResult.getResultStatistics();
        List<Boolean> result = new ArrayList<>();
        SelectTableInfo source = comparisonInfo.getSource();
        SelectTableInfo target = comparisonInfo.getTarget();

        DriverSession sourceDriverSession = driverSessionService.getDriverSession(tenantId,
                source.getDataSourceCode());
        DriverSession targetDriverSession = driverSessionService.getDriverSession(tenantId,
                target.getDataSourceCode());
        for (String sql : sqlList) {
            String[] split = sql.split("&");
            String sourceSql = split[0];
            String targetSql = split[1];
            String operation = split[2];
            List<Map<String, Object>> sourceResult = sourceDriverSession.executeOneQuery(source.getSchema(), sourceSql);
            List<Map<String, Object>> targetResult = targetDriverSession.executeOneQuery(target.getSchema(), targetSql);
            Object result1 = sourceResult.stream().findFirst().map(map -> map.get("_cdt1")).orElse(null);
            Object result2 = targetResult.stream().findFirst().map(map -> map.get("_cdt2")).orElse(null);
            if (Objects.isNull(result1) || Objects.isNull(result2) || !meetCondition(result1, result2, operation)) {
                result.add(Boolean.FALSE);
            } else {
                result.add(Boolean.TRUE);
            }
        }
        resultStatistics.setPreAuditResult(JsonUtil.toJson(result));
        return result.stream().allMatch(oneCondition -> oneCondition);
    }

    private boolean meetCondition(Object result1, Object result2, String operation) {
        int opt;
        if (!Objects.isNull(result1) && !Objects.isNull(result2)) {
            opt = String.valueOf(result1).compareTo(String.valueOf(result2));
        } else {
            throw new HandlerException(ErrorCode.CONDITION_NOT_SUPPORT);
        }
        return compare(operation, opt);
    }

    private boolean compare(String operation, int opt) {
        String compareChar;
        if (opt == -1) {
            compareChar = "<";
        } else if (opt == 0) {
            compareChar = "=";
        } else {
            compareChar = ">";
        }
        return operation.contains(compareChar);
    }

}
