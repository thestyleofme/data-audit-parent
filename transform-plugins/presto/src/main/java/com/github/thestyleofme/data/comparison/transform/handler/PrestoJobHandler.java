package com.github.thestyleofme.data.comparison.transform.handler;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.github.thestyleofme.comparison.common.app.service.source.SourceDataMapping;
import com.github.thestyleofme.comparison.common.app.service.transform.BaseTransformHandler;
import com.github.thestyleofme.comparison.common.app.service.transform.HandlerResult;
import com.github.thestyleofme.comparison.common.domain.AppConf;
import com.github.thestyleofme.comparison.common.domain.ColMapping;
import com.github.thestyleofme.comparison.common.domain.JobEnv;
import com.github.thestyleofme.comparison.common.domain.PrestoInfo;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.infra.annotation.TransformType;
import com.github.thestyleofme.comparison.common.infra.exceptions.HandlerException;
import com.github.thestyleofme.comparison.common.infra.utils.PrestoUtils;
import com.github.thestyleofme.comparison.common.infra.utils.SqlGeneratorUtil;
import com.github.thestyleofme.comparison.common.infra.utils.TransformUtils;
import com.github.thestyleofme.driver.core.app.service.DriverSessionService;
import com.github.thestyleofme.driver.core.app.service.session.DriverSession;
import com.github.thestyleofme.plugin.core.infra.utils.BeanUtils;
import com.github.thestyleofme.plugin.core.infra.utils.JsonUtil;
import com.github.thestyleofme.presto.app.service.ClusterService;
import com.github.thestyleofme.presto.domain.entity.Cluster;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * @author siqi.hou@hand-china.com
 * @date 2020-11-16 19:43
 */
@TransformType(value = "PRESTO")
@Component
@Slf4j
public class PrestoJobHandler implements BaseTransformHandler {
    private static final int FOUR = 4;
    private final DriverSessionService driverSessionService;
    private final ClusterService clusterService;
    private final JdbcHandler jdbcHandler;

    public PrestoJobHandler(DriverSessionService driverSessionService, ClusterService clusterService, JdbcHandler jdbcHandler) {
        this.driverSessionService = driverSessionService;
        this.clusterService = clusterService;
        this.jdbcHandler = jdbcHandler;
    }

    @Override
    public HandlerResult handle(ComparisonJob comparisonJob,
                                Map<String, Object> env,
                                Map<String, Object> transformMap,
                                SourceDataMapping sourceDataMapping) {
        LocalDateTime startTime = LocalDateTime.now();
        AppConf appConf = JsonUtil.toObj(comparisonJob.getAppConf(), AppConf.class);
        JobEnv jobEnv = BeanUtils.map2Bean(env, JobEnv.class);
        PrestoInfo prestoInfo = PrestoUtils.getPrestoInfo(appConf, transformMap);
        HandlerResult handlerResult = new HandlerResult();

        // 尝试获取 presto 的dataSourceCode
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
        if (!StringUtils.isEmpty(prestoInfo.getDataSourceCode())) {
            // 走数据源
            handleByDataSourceCode(handlerResult, prestoInfo, comparisonJob, jobEnv);
        } else {
            // 走jdbc
            handleByJDBC(handlerResult, prestoInfo, jobEnv);
        }
        LocalDateTime endTime = LocalDateTime.now();
        log.debug("job time cost :" + Duration.between(endTime, startTime));
        return handlerResult;
    }

    private void handleByJDBC(HandlerResult handlerResult, PrestoInfo prestoInfo, JobEnv jobEnv) {
        // 生成sql
        String sql = SqlGeneratorUtil.generateSql(prestoInfo, jobEnv);
        // 执行sql
        List<List<Map<String, Object>>> list = jdbcHandler.executeAllSql(prestoInfo, sql);
        // 装载数据到handlerResult
        this.fillHandlerResult(handlerResult, jobEnv, list);
    }

    private void handleByDataSourceCode(HandlerResult handlerResult,
                                        PrestoInfo prestoInfo,
                                        ComparisonJob comparisonJob,
                                        JobEnv jobEnv) {
        // 生成sql
        String sql = SqlGeneratorUtil.generateSql(prestoInfo, jobEnv);
        // 执行sql
        DriverSession driverSession = driverSessionService.getDriverSession(comparisonJob.getTenantId(), prestoInfo.getDataSourceCode());
        List<List<Map<String, Object>>> result = driverSession.executeAll(null, sql, true);
        if (CollectionUtils.isEmpty(result) || result.size() != FOUR) {
            throw new HandlerException("hdsp.xadt.error.presto.not_support");
        }
        // 装载数据到handlerResult
        this.fillHandlerResult(handlerResult, jobEnv, result);
    }

    private void fillHandlerResult(HandlerResult handlerResult, JobEnv jobEnv, List<List<Map<String, Object>>> result) {
        // 源端和目标端都有的数据
        Optional.ofNullable(result.get(0)).ifPresent(list -> {
            List<LinkedHashMap<String, Object>> sortedList =
                    TransformUtils.sortListMap(jobEnv, list, ColMapping.SOURCE);
            handlerResult.getSameDataList().addAll(sortedList);
        });
        // 源端有但目标端无
        Optional.ofNullable(result.get(1)).ifPresent(list -> {
            List<LinkedHashMap<String, Object>> sortedList =
                    TransformUtils.sortListMap(jobEnv, list, ColMapping.SOURCE);
            handlerResult.getSourceUniqueDataList().addAll(sortedList);
        });
        // 目标端有但源端无
        Optional.ofNullable(result.get(2)).ifPresent(list -> {
            List<LinkedHashMap<String, Object>> sortedList =
                    TransformUtils.sortListMap(jobEnv, list, ColMapping.TARGET);
            handlerResult.getTargetUniqueDataList().addAll(sortedList);
        });
        // 源端和目标端数据不一样，但主键或唯一性索引一样
        Optional.ofNullable(result.get(3)).ifPresent(list -> {
            List<LinkedHashMap<String, Object>> sortedList =
                    TransformUtils.sortListMap(jobEnv, list, ColMapping.SOURCE);
            handlerResult.getPkOrIndexSameDataList().addAll(sortedList);
        });
    }

}