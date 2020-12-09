package com.github.thestyleofme.comparison.presto.handler;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.github.thestyleofme.comparison.common.app.service.transform.BaseTransformHandler;
import com.github.thestyleofme.comparison.common.app.service.transform.HandlerResult;
import com.github.thestyleofme.comparison.common.domain.ResultStatistics;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.infra.annotation.TransformType;
import com.github.thestyleofme.comparison.common.infra.utils.HandlerUtil;
import com.github.thestyleofme.comparison.presto.handler.pojo.PrestoInfo;
import com.github.thestyleofme.comparison.presto.handler.service.PrestoExecutor;
import com.github.thestyleofme.comparison.presto.handler.utils.PrestoUtils;
import com.github.thestyleofme.comparison.presto.handler.utils.SqlGeneratorUtil;
import com.github.thestyleofme.presto.app.service.ClusterService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

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


    public PrestoJobHandler(ClusterService clusterService, PrestoExecutor prestoExecutor) {
        this.clusterService = clusterService;
        this.prestoExecutor = prestoExecutor;
    }

    @Override
    public void handle(ComparisonJob comparisonJob,
                       Map<String, Object> env,
                       Map<String, Object> transformMap,
                       HandlerResult handlerResult) {
        LocalDateTime startTime = LocalDateTime.now();
        Long tenantId = comparisonJob.getTenantId();
        PrestoInfo prestoInfo = PrestoUtils.getPrestoInfo(tenantId, env, transformMap, clusterService);
        // do 数据稽核流程
        doTransform(tenantId, prestoInfo, handlerResult);
        LocalDateTime endTime = LocalDateTime.now();
        log.debug("job time cost :" + HandlerUtil.timestamp2String(Duration.between(startTime, endTime).toMillis()));
    }

    private void doTransform(Long tenantId, PrestoInfo prestoInfo, HandlerResult handlerResult) {
        String auditSql = SqlGeneratorUtil.generateAuditSql(prestoInfo);
        LocalDateTime startTime = LocalDateTime.now();
        List<List<Map<String, Object>>> result = prestoExecutor.executeSql(tenantId, prestoInfo, auditSql);
        LocalDateTime endTime = LocalDateTime.now();
        log.info("transform time:{}", HandlerUtil.timestamp2String(Duration.between(startTime, endTime).toMillis()));

        // 装载数据到handlerResult
        this.fillHandlerResult(handlerResult, result, auditSql);

    }

    private void fillHandlerResult(HandlerResult handlerResult,
                                   List<List<Map<String, Object>>> result, String sql) {
        String[] sqlList = sql.split("\n");
        ResultStatistics statistics = handlerResult.getResultStatistics();
        //1. 源端、目标端都有的数据量
        Optional.ofNullable(result.get(0)).flatMap(list -> list.stream().findFirst()).ifPresent(map -> {
            long size = 0L;
            if (!CollectionUtils.isEmpty(map)) {
                size = (long) map.get("count");
            }
            statistics.setSameCount(statistics.getSameCount() + size);
            statistics.setSameCountSql(sqlList[0]);
        });
        //2. 源端有但目标端无
        statistics.setInsertCountSql(sqlList[1]);
        Optional.ofNullable(result.get(1)).ifPresent(list -> {
            handlerResult.getSourceUniqueDataList().addAll(list);
            statistics.setInsertCount(statistics.getInsertCount() + list.size());
            statistics.setInsertCountSql(sqlList[1]);
        });
        //3. 目标端有但源端无
        statistics.setInsertCountSql(sqlList[2]);

        Optional.ofNullable(result.get(2)).ifPresent(list -> {
            handlerResult.getTargetUniqueDataList().addAll(list);
            statistics.setDeleteCount(statistics.getDeleteCount() + list.size());
            statistics.setDeleteCountSql(sqlList[2]);
        });
        //4. 源端和目标端数据不一样，但主键或唯一性索引一样
        statistics.setInsertCountSql(sqlList[3]);
        Optional.ofNullable(result.get(3)).ifPresent(list -> {
            handlerResult.getPkOrIndexSameDataList().addAll(list);
            statistics.setUpdateCount(statistics.getUpdateCount() + list.size());
            statistics.setUpdateCountSql(sqlList[3]);
        });
    }

}