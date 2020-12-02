package com.github.thestyleofme.comparison.presto.handler;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.github.thestyleofme.comparison.common.app.service.transform.BaseTransformHandler;
import com.github.thestyleofme.comparison.common.app.service.transform.HandlerResult;
import com.github.thestyleofme.comparison.common.domain.JobEnv;
import com.github.thestyleofme.comparison.common.domain.ResultStatistics;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.infra.annotation.TransformType;
import com.github.thestyleofme.comparison.common.infra.constants.ErrorCode;
import com.github.thestyleofme.comparison.common.infra.exceptions.HandlerException;
import com.github.thestyleofme.comparison.presto.handler.exceptions.SkipAuditException;
import com.github.thestyleofme.comparison.presto.handler.hook.BasePreTransformHook;
import com.github.thestyleofme.comparison.presto.handler.pojo.PrestoInfo;
import com.github.thestyleofme.comparison.presto.handler.pojo.SkipCondition;
import com.github.thestyleofme.comparison.presto.handler.service.PrestoExecutor;
import com.github.thestyleofme.comparison.presto.handler.utils.PrestoUtils;
import com.github.thestyleofme.comparison.presto.handler.utils.SqlGeneratorUtil;
import com.github.thestyleofme.presto.app.service.ClusterService;
import com.github.thestyleofme.presto.domain.entity.Cluster;
import com.github.thestyleofme.presto.infra.utils.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * <p>
 * description
 * </p>
 *
 * @author hsq 2020/11/20 11:25
 * @since 1.0.0
 */
@Component
@Slf4j
@TransformType(value = "PRESTO")
public class PrestoJobHandler implements BaseTransformHandler {

    private final ClusterService clusterService;
    private final PrestoExecutor prestoExecutor;

    private final List<BasePreTransformHook> basePreTransformHookList;

    public PrestoJobHandler(ClusterService clusterService, PrestoExecutor prestoExecutor,
                            List<BasePreTransformHook> basePreTransformHookList) {
        this.clusterService = clusterService;
        this.prestoExecutor = prestoExecutor;
        this.basePreTransformHookList = basePreTransformHookList;
    }

    @Override
    public void handle(ComparisonJob comparisonJob,
                       Map<String, Object> env,
                       Map<String, Object> preTransform,
                       Map<String, Object> transformMap,
                       HandlerResult handlerResult) {
        LocalDateTime startTime = LocalDateTime.now();
        String json = JsonUtil.toJson(env);
        JobEnv jobEnv = JsonUtil.toObj(json, JobEnv.class);
        PrestoInfo prestoInfo = PrestoUtils.getPrestoInfo(jobEnv, transformMap);
        // 尝试获取 presto 的 dataSourceCode
        Long tenantId = comparisonJob.getTenantId();
        if (StringUtils.isEmpty(prestoInfo.getDataSourceCode()) && !StringUtils.isEmpty(prestoInfo.getClusterCode())) {
            Cluster one = clusterService.getOne(new QueryWrapper<>(Cluster.builder()
                    .tenantId(tenantId).clusterCode(prestoInfo.getClusterCode()).build()));
            Optional.ofNullable(one)
                    .ifPresent(cluster -> {
                        if (!StringUtils.isEmpty(cluster.getDatasourceCode())) {
                            prestoInfo.setDataSourceCode(cluster.getDatasourceCode());
                        }
                    });
        }
        // do 预处理
        doPreTransform(tenantId, prestoInfo, preTransform, handlerResult);
        // do 数据稽核流程
        doTransform(tenantId, prestoInfo, handlerResult);

        LocalDateTime endTime = LocalDateTime.now();
        log.debug("job time cost :" + Duration.between(endTime, startTime));
    }

    private void doPreTransform(Long tenantId, PrestoInfo prestoInfo, Map<String, Object> preTransform, HandlerResult handlerResult) {
        String preTransformType = (String) preTransform.get("preTransformType");
        BasePreTransformHook preTransformHook;
        // 判断preTransformType是否为空 空即是取DEFAULT 反之取preTransformType
        if (StringUtils.isEmpty(preTransformType)) {
            preTransformType = "default";
        }
        String type = preTransformType;
        preTransformHook = basePreTransformHookList.stream()
                .filter(hook -> hook.getName().equalsIgnoreCase(type))
                .findFirst().orElseThrow(() -> new HandlerException(ErrorCode.PRE_TRANSFORM_CLASS_NOT_FOUND));
        List<SkipCondition> skipConditionList = JsonUtil.toArray(JsonUtil.toJson(preTransform.get("skipCondition")), SkipCondition.class);
        //拼sql执行 skipCondition是否都满足 满足则跳过即抛一个指定异常，交由上游处理
        if (preTransformHook.skip(tenantId, prestoInfo, skipConditionList, handlerResult)) {
            throw new SkipAuditException(ErrorCode.PRE_TRANSFORM_SKIP_INFO);
        }
    }

    private void doTransform(Long tenantId, PrestoInfo prestoInfo, HandlerResult handlerResult) {
        String auditSql = SqlGeneratorUtil.generateAuditSql(prestoInfo);
        List<List<Map<String, Object>>> result = prestoExecutor.executeSql(tenantId, prestoInfo, auditSql);
        // 装载数据到handlerResult
        this.fillHandlerResult(handlerResult, result, auditSql);

    }

    private void fillHandlerResult(HandlerResult handlerResult,
                                   List<List<Map<String, Object>>> result, String sql) {
        String[] sqls = sql.split("\n");
        ResultStatistics statistics = handlerResult.getResultStatistics();
        //1. 源端、目标端都有的数据量
        Optional.ofNullable(result.get(0)).flatMap(list -> list.stream().findFirst()).ifPresent(map -> {
            long size = 0L;
            if (!CollectionUtils.isEmpty(map)) {
                size = (long) map.get("count");
            }
            statistics.setSameCount(statistics.getSameCount() + size);
            statistics.setSameCountSql(sqls[0]);
        });
        //2. 源端有但目标端无
        statistics.setInsertCountSql(sqls[1]);
        Optional.ofNullable(result.get(1)).ifPresent(list -> {
            handlerResult.getSourceUniqueDataList().addAll(list);
            statistics.setInsertCount(statistics.getInsertCount() + list.size());
            statistics.setInsertCountSql(sqls[1]);
        });
        //3. 目标端有但源端无
        statistics.setInsertCountSql(sqls[2]);

        Optional.ofNullable(result.get(2)).ifPresent(list -> {
            handlerResult.getTargetUniqueDataList().addAll(list);
            statistics.setDeleteCount(statistics.getDeleteCount() + list.size());
            statistics.setDeleteCountSql(sqls[2]);
        });
        //4. 源端和目标端数据不一样，但主键或唯一性索引一样
        statistics.setInsertCountSql(sqls[3]);
        Optional.ofNullable(result.get(3)).ifPresent(list -> {
            handlerResult.getPkOrIndexSameDataList().addAll(list);
            statistics.setUpdateCount(statistics.getUpdateCount() + list.size());
            statistics.setUpdateCountSql(sqls[3]);
        });
    }

}