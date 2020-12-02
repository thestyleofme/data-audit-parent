package com.github.thestyleofme.comparison.presto.handler.hook;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.github.thestyleofme.comparison.common.app.service.transform.HandlerResult;
import com.github.thestyleofme.comparison.common.domain.ResultStatistics;
import com.github.thestyleofme.comparison.common.infra.constants.ErrorCode;
import com.github.thestyleofme.comparison.common.infra.exceptions.HandlerException;
import com.github.thestyleofme.comparison.presto.handler.pojo.PrestoInfo;
import com.github.thestyleofme.comparison.presto.handler.pojo.SkipCondition;
import com.github.thestyleofme.comparison.presto.handler.service.PrestoExecutor;
import com.github.thestyleofme.presto.infra.utils.JsonUtil;
import org.springframework.stereotype.Component;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/30 17:50
 * @since 1.0.0
 */
@Component
public class DefaultPreTransformHook extends BasePreTransformHook {
    private final PrestoExecutor prestoExecutor;

    public DefaultPreTransformHook(PrestoExecutor prestoExecutor) {
        this.prestoExecutor = prestoExecutor;
    }

    @Override
    public String getName() {
        return "DEFAULT";
    }

    @Override
    protected boolean execSqlAndComputeSkip(Long tenantId, PrestoInfo prestoInfo, List<String> sqlList,
                                            HandlerResult handlerResult) {
        String sql = String.join("\n", sqlList);
        List<List<Map<String, Object>>> result = prestoExecutor.executeSql(tenantId, prestoInfo, sql);
        List<Boolean> values = result.stream().map(list ->
                Optional.ofNullable(list.get(0))
                        .map(map -> (long) map.get("_result") == 1L)
                        .orElseThrow(() -> new HandlerException(ErrorCode.PRE_TRANSFORM_RESULT_NOT_FOUND)))
                .collect(Collectors.toList());
        // 封装预处理结果
        ResultStatistics statistics = handlerResult.getResultStatistics();
        statistics.setPreAuditResult(JsonUtil.toJson(values));
        return values.stream().allMatch(Boolean.TRUE::equals);
    }


    @Override
    protected List<String> generateSqlByCondition(PrestoInfo prestoInfo, List<SkipCondition> skipConditionList) {
        String sourceTableName = prestoInfo.getSourceTableName();
        String targetTableName = prestoInfo.getTargetTableName();
        //`select count(*) as _result
        // from (select count(*)as _cdt1 from target_t1) as _a
        // ,(select count(*) as _cdt2 from source_t1) as _b
        //where _a._cdt1 = _b._cdt2;`
        return skipConditionList.stream().map(skipCondition ->
                String.format("select count(*) as _result from (select %s as _cdt1 from %s) as _a," +
                                "(select %s as _cdt2 from %s) as _b where _a._cdt1 %s _b._cdt2;",
                        skipCondition.getSource(), sourceTableName, skipCondition.getTarget(), targetTableName,
                        skipCondition.getOperation()))
                .collect(Collectors.toList());
    }

}
