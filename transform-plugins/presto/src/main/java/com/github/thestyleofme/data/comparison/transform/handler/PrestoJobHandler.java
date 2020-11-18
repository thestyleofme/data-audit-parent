package com.github.thestyleofme.data.comparison.transform.handler;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.github.thestyleofme.comparison.common.app.service.source.SourceDataMapping;
import com.github.thestyleofme.comparison.common.app.service.transform.BaseTransformHandler;
import com.github.thestyleofme.comparison.common.app.service.transform.HandlerResult;
import com.github.thestyleofme.comparison.common.domain.AppConf;
import com.github.thestyleofme.comparison.common.domain.ColMapping;
import com.github.thestyleofme.comparison.common.domain.JobEnv;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.infra.annotation.TransformType;
import com.github.thestyleofme.comparison.common.infra.exceptions.HandlerException;
import com.github.thestyleofme.comparison.common.infra.utils.CommonUtil;
import com.github.thestyleofme.comparison.common.infra.utils.TransformUtils;
import com.github.thestyleofme.data.comparison.transform.constants.PrestoConstant.SqlConstant;
import com.github.thestyleofme.data.comparison.transform.pojo.PrestoInfo;
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

    private static final Integer FOUR = 4;
    private final DriverSessionService driverSessionService;
    private final ClusterService clusterService;

    public PrestoJobHandler(DriverSessionService driverSessionService, ClusterService clusterService) {
        this.driverSessionService = driverSessionService;
        this.clusterService = clusterService;
    }

    @Override
    public HandlerResult handle(ComparisonJob comparisonJob,
                                Map<String, Object> env,
                                Map<String, Object> transformMap,
                                SourceDataMapping sourceDataMapping) {
        HandlerResult handlerResult = new HandlerResult();
        LocalDateTime startTime = LocalDateTime.now();
        AppConf appConf = JsonUtil.toObj(comparisonJob.getAppConf(), AppConf.class);
        JobEnv jobEnv = BeanUtils.map2Bean(env, JobEnv.class);
        PrestoInfo prestoInfo = getPrestoInfo(appConf, comparisonJob, transformMap);
        // 走数据源
        if (!StringUtils.isEmpty(prestoInfo.getDataSourceCode())) {
            handleByDataSourceCode(handlerResult, prestoInfo, comparisonJob, jobEnv);
        } else {
            // 走url
            throw new HandlerException("hdsp.xadt.hand.presto.not_support");
        }
        LocalDateTime endTime = LocalDateTime.now();
        log.debug("job time cost :" + Duration.between(endTime, startTime));
        return handlerResult;
    }

    private void handleByDataSourceCode(HandlerResult handlerResult,
                                        PrestoInfo prestoInfo,
                                        ComparisonJob comparisonJob,
                                        JobEnv jobEnv) {
        String sql;
        String sourcePk = jobEnv.getSourcePk();
        String targetPk = jobEnv.getTargetPk();
        // 如果有指定主键
        if (!StringUtils.isEmpty(sourcePk) && !StringUtils.isEmpty(targetPk)) {
            sql = generateSqlByPk(sourcePk, targetPk, prestoInfo, jobEnv);
        } else {
            // todo 唯一索引
            throw new HandlerException("hdsp.xadt.error.presto.not_support");
        }
        // 获取数据源
        DriverSession driverSession = driverSessionService.getDriverSession(comparisonJob.getTenantId(), prestoInfo.getDataSourceCode());
        // 执行查询语句
        List<List<Map<String, Object>>> result = driverSession.executeAll(null, sql, true);
        if (CollectionUtils.isEmpty(result) || result.size() != FOUR) {
            throw new HandlerException("hdsp.xadt.error.presto.not_support");
        }

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
                    TransformUtils.sortListMap(jobEnv, list, ColMapping.SOURCE);
            handlerResult.getTargetUniqueDataList().addAll(sortedList);
        });
        // 源端和目标端数据不一样，但主键或唯一性索引一样
        Optional.ofNullable(result.get(3)).ifPresent(list -> {
            List<LinkedHashMap<String, Object>> sortedList =
                    TransformUtils.sortListMap(jobEnv, list, ColMapping.SOURCE);
            handlerResult.getPkOrIndexSameDataList().addAll(sortedList);
        });
    }

    private String generateSqlByPk(String sourcePk,
                                   String targetPk,
                                   PrestoInfo prestoInfo,
                                   JobEnv jobEnv) {
        StringBuilder builder = new StringBuilder();
        String sourceTable = String.format(SqlConstant.TABLE_FT, prestoInfo.getSourceDatasourceCode(),
                prestoInfo.getSourceSchema(), prestoInfo.getSourceTable());
        String targetTable = String.format(SqlConstant.TABLE_FT, prestoInfo.getTargetDatasourceCode(),
                prestoInfo.getTargetSchema(), prestoInfo.getTargetTable());

        // 获取列的映射关系
        List<Map<String, Object>> colMapping = jobEnv.getColMapping();
        List<ColMapping> colMappingList = colMapping.stream()
                .map(map -> BeanUtils.map2Bean(map, ColMapping.class))
                .collect(Collectors.toList());

        // AB都有的数据
        builder.append(String.format(SqlConstant.LEFT_SQL, sourceTable, targetTable))
                .append(String.format(SqlConstant.ON_PK, sourcePk, targetPk))
                .append(String.format(SqlConstant.WHERE, String.format(SqlConstant.RIGHT_IS_NOT_NULL, targetPk)));
        StringBuilder equalsBuilder = new StringBuilder();
        for (ColMapping mapping : colMappingList) {
            equalsBuilder.append(String.format(SqlConstant.EQUAL, mapping.getSourceCol(), mapping.getTargetCol()));
        }
        equalsBuilder.append(SqlConstant.AND_END);
        builder.append(equalsBuilder.toString())
                .append(SqlConstant.LINE_END);
        // A有B无
        builder.append(String.format(SqlConstant.LEFT_SQL, sourceTable, targetTable))
                .append(String.format(SqlConstant.ON_PK, sourcePk, targetPk))
                .append(String.format(SqlConstant.WHERE, String.format(SqlConstant.RIGHT_IS_NULL, targetPk)))
                .append(SqlConstant.LINE_END);

        // A无B有
        builder.append(String.format(SqlConstant.LEFT_SQL, targetTable, sourceTable))
                .append(String.format(SqlConstant.ON_PK, targetPk, sourcePk))
                .append(String.format(SqlConstant.WHERE, String.format(SqlConstant.RIGHT_IS_NULL, sourcePk)))
                .append(SqlConstant.LINE_END);

        // AB主键或唯一索引相同，部分字段不一样
        builder.append(String.format(SqlConstant.LEFT_SQL, sourceTable, targetTable))
                .append(String.format(SqlConstant.ON_PK, sourcePk, targetPk));
        StringBuilder whereBuilder = new StringBuilder();
        for (ColMapping mapping : colMappingList) {
            whereBuilder.append(String.format(SqlConstant.NOT_EQUAL, mapping.getSourceCol(), mapping.getTargetCol()));
        }
        whereBuilder.append(SqlConstant.OR_END);
        builder.append(String.format(SqlConstant.WHERE, whereBuilder.toString()))
                .append(SqlConstant.LINE_END);

        log.debug("==> presto sql: {}", builder.toString());
        return builder.toString();
    }

    private PrestoInfo getPrestoInfo(AppConf appConf,
                                     ComparisonJob comparisonJob,
                                     Map<String, Object> transformMap) {
        if (CollectionUtils.isEmpty(transformMap)) {
            throw new HandlerException("hdsp.xadt.error.transform.is_null");
        }
        // 优先使用transform中数据，其次从env中获取
        PrestoInfo prestoInfo = BeanUtils.map2Bean(transformMap, PrestoInfo.class);
        JobEnv jobEnv = BeanUtils.map2Bean(appConf.getEnv(), JobEnv.class);
        prestoInfo.setSourceDatasourceCode(CommonUtil.requireNonNullElse(prestoInfo.getSourceDatasourceCode(), jobEnv.getSourceDatasourceCode()));
        prestoInfo.setSourceSchema(CommonUtil.requireNonNullElse(prestoInfo.getSourceSchema(), jobEnv.getSourceSchema()));
        prestoInfo.setSourceTable(CommonUtil.requireNonNullElse(prestoInfo.getSourceTable(), jobEnv.getSourceTable()));
        prestoInfo.setTargetDatasourceCode(CommonUtil.requireNonNullElse(prestoInfo.getTargetDatasourceCode(), jobEnv.getTargetDatasourceCode()));
        prestoInfo.setTargetSchema(CommonUtil.requireNonNullElse(prestoInfo.getTargetSchema(), jobEnv.getTargetSchema()));
        prestoInfo.setTargetTable(CommonUtil.requireNonNullElse(prestoInfo.getTargetTable(), jobEnv.getTargetTable()));

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
        return prestoInfo;
    }

}