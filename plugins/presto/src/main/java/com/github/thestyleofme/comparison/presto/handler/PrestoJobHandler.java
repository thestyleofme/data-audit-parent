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
import com.github.thestyleofme.comparison.common.infra.exceptions.HandlerException;
import com.github.thestyleofme.comparison.presto.handler.context.JdbcHandler;
import com.github.thestyleofme.comparison.presto.handler.hook.BasePreTransformHook;
import com.github.thestyleofme.comparison.presto.handler.pojo.PrestoInfo;
import com.github.thestyleofme.comparison.presto.handler.utils.PrestoUtils;
import com.github.thestyleofme.comparison.presto.handler.utils.SqlGeneratorUtil;
import com.github.thestyleofme.driver.core.app.service.DriverSessionService;
import com.github.thestyleofme.driver.core.app.service.session.DriverSession;
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

    private static final int FOUR = 4;
    private final DriverSessionService driverSessionService;
    private final ClusterService clusterService;
    private final JdbcHandler jdbcHandler;

    private final List<BasePreTransformHook> basePreTransformHookList;

    public PrestoJobHandler(DriverSessionService driverSessionService,
                            ClusterService clusterService,
                            JdbcHandler jdbcHandler,
                            List<BasePreTransformHook> basePreTransformHookList) {
        this.driverSessionService = driverSessionService;
        this.clusterService = clusterService;
        this.jdbcHandler = jdbcHandler;
        this.basePreTransformHookList = basePreTransformHookList;
    }

    @Override
    public HandlerResult handle(ComparisonJob comparisonJob,
                                Map<String, Object> env,
                                Map<String, Object> preTransform,
                                Map<String, Object> transformMap) {
        LocalDateTime startTime = LocalDateTime.now();
        String json = JsonUtil.toJson(env);
        JobEnv jobEnv = JsonUtil.toObj(json, JobEnv.class);
        PrestoInfo prestoInfo = PrestoUtils.getPrestoInfo(jobEnv, transformMap);
        HandlerResult handlerResult = new HandlerResult();
        handlerResult.setResultStatistics(new ResultStatistics());

        // 尝试获取 presto 的 dataSourceCode
        if (StringUtils.isEmpty(prestoInfo.getDataSourceCode()) && !StringUtils.isEmpty(prestoInfo.getClusterCode())) {
            Cluster one = clusterService.getOne(new QueryWrapper<>(Cluster.builder()
                    .tenantId(comparisonJob.getTenantId()).clusterCode(prestoInfo.getClusterCode()).build()));
            Optional.ofNullable(one)
                    .ifPresent(cluster -> {
                        if (!StringUtils.isEmpty(cluster.getDatasourceCode())) {
                            prestoInfo.setDataSourceCode(cluster.getDatasourceCode());
                        }
                    });
        }
        // preTransform
        // todo 拼sql执行 skipCondition是否都满足 满足则跳过即抛一个指定异常，交由上游处理
        // throw new SkipAuditException("hdsp.xadt.error.pre.transform.skip.conditions.match");
        // 判断preTransformType是否为空 空即是取DEFAULT 反之
        // basePreTransformHookList.stream().filter("")
        String auditSql = SqlGeneratorUtil.generateAuditSql(prestoInfo);
        if (!StringUtils.isEmpty(prestoInfo.getDataSourceCode())) {
            // 走数据源
            handleByDataSourceCode(comparisonJob.getTenantId(), handlerResult, prestoInfo, auditSql);
        } else {
            // 走jdbc
            handleByJdbc(handlerResult, prestoInfo, auditSql);
        }
        LocalDateTime endTime = LocalDateTime.now();
        log.debug("job time cost :" + Duration.between(endTime, startTime));
        return handlerResult;
    }

    private void handleByJdbc(HandlerResult handlerResult, PrestoInfo prestoInfo, String sql) {
        // 执行sql
        List<List<Map<String, Object>>> list = jdbcHandler.executeBatchQuerySql(prestoInfo, sql);
        // 装载数据到handlerResult
        this.fillHandlerResult(handlerResult, list, sql);
    }

    private void handleByDataSourceCode(Long tenantId, HandlerResult handlerResult,
                                        PrestoInfo prestoInfo, String sql) {
        // 执行sql
        DriverSession driverSession = driverSessionService.getDriverSession(tenantId, prestoInfo.getDataSourceCode());
        List<List<Map<String, Object>>> result = driverSession.executeAll(null, sql, true);
        if (CollectionUtils.isEmpty(result) || result.size() != FOUR) {
            throw new HandlerException("hdsp.xadt.error.presto.not_support");
        }
        // 装载数据到handlerResult
        this.fillHandlerResult(handlerResult, result, sql);
    }

    private void fillHandlerResult(HandlerResult handlerResult,
                                   List<List<Map<String, Object>>> result, String sql) {
        String[] sqls = sql.split("\n");
        ResultStatistics statistics = handlerResult.getResultStatistics();
        // 源端、目标端都有的数据量
        Optional.ofNullable(result.get(0)).flatMap(list -> list.stream().findFirst()).ifPresent(map -> {
            long size = 0L;
            if (!CollectionUtils.isEmpty(map)) {
                size = (long) map.get("count");
            }
            statistics.setSameCount(statistics.getSameCount() + size);
            statistics.setSameCountSql(sqls[0]);
        });
        // 源端有但目标端无
        statistics.setInsertCountSql(sqls[1]);
        Optional.ofNullable(result.get(1)).ifPresent(list -> {
            handlerResult.getSourceUniqueDataList().addAll(list);
            statistics.setInsertCount(statistics.getInsertCount() + list.size());
        });
        // 目标端有但源端无
        statistics.setInsertCountSql(sqls[2]);

        Optional.ofNullable(result.get(2)).ifPresent(list -> {
            handlerResult.getTargetUniqueDataList().addAll(list);
            statistics.setDeleteCount(statistics.getDeleteCount() + list.size());
        });
        // 源端和目标端数据不一样，但主键或唯一性索引一样
        statistics.setInsertCountSql(sqls[3]);
        Optional.ofNullable(result.get(3)).ifPresent(list -> {
            handlerResult.getPkOrIndexSameDataList().addAll(list);
            statistics.setUpdateCount(statistics.getUpdateCount() + list.size());
        });
    }

}