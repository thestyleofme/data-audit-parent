package com.github.thestyleofme.comparison.presto.handler.hook;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.github.thestyleofme.comparison.common.app.service.pretransform.BasePreTransformHook;
import com.github.thestyleofme.comparison.common.app.service.transform.HandlerResult;
import com.github.thestyleofme.comparison.common.domain.AppConf;
import com.github.thestyleofme.comparison.common.domain.ResultStatistics;
import com.github.thestyleofme.comparison.common.domain.entity.DataInfo;
import com.github.thestyleofme.comparison.common.domain.entity.SkipCondition;
import com.github.thestyleofme.comparison.common.infra.constants.ErrorCode;
import com.github.thestyleofme.comparison.common.infra.exceptions.HandlerException;
import com.github.thestyleofme.comparison.presto.handler.pojo.PrestoInfo;
import com.github.thestyleofme.comparison.presto.handler.service.PrestoExecutor;
import com.github.thestyleofme.comparison.presto.handler.utils.PrestoUtils;
import com.github.thestyleofme.presto.app.service.ClusterService;
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
public class PrestoPreTransformHook extends BasePreTransformHook {

    private final PrestoExecutor prestoExecutor;
    private final ClusterService clusterService;

    public PrestoPreTransformHook(PrestoExecutor prestoExecutor, ClusterService clusterService) {
        this.prestoExecutor = prestoExecutor;
        this.clusterService = clusterService;
    }

    @Override
    public String getName() {
        return "presto";
    }

    @Override
    protected DataInfo prepareDataInfo(Long tenantId, AppConf appConf) {
        Map<String, Object> transformMap = appConf.getTransform().get("presto");
        return PrestoUtils.getPrestoInfo(tenantId, appConf.getEnv(), transformMap, clusterService);
    }

    @Override
    protected List<String> generateSqlByCondition(DataInfo dataInfo,
                                                  List<SkipCondition> skipConditionList) {
        PrestoInfo prestoInfo = (PrestoInfo) dataInfo;
        String sourceTableName = prestoInfo.getSourceTableName();
        String targetTableName = prestoInfo.getTargetTableName();
        /*
         SELECT count(*) AS _result
         FROM ( SELECT count(*) AS _cdt1 FROM target_t1 ) AS _a,
         ( SELECT count(*) AS _cdt2 FROM source_t1 ) AS _b
         WHERE
	        _a._cdt1 = _b._cdt2
         */
        return skipConditionList.stream().map(skipCondition ->
                String.format("select count(*) as _result from (select %s as _cdt1 from %s) as _a," +
                                "(select %s as _cdt2 from %s) as _b where _a._cdt1 %s _b._cdt2;",
                        skipCondition.getSource(), sourceTableName, skipCondition.getTarget(), targetTableName,
                        skipCondition.getOperation()))
                .collect(Collectors.toList());
    }

    @Override
    protected boolean execSqlAndComputeSkip(Long tenantId, DataInfo dataInfo, List<String> sqlList,
                                            HandlerResult handlerResult) {
        PrestoInfo prestoInfo = (PrestoInfo) dataInfo;
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

}
