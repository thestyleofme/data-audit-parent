package com.github.thestyleofme.data.comparison.transform.handler;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.StringPool;
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
        PrestoInfo prestoInfo = getPrestoInfo(appConf, comparisonJob, transformMap);
        HandlerResult handlerResult = new HandlerResult();

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
        String sql = generateSql(prestoInfo, jobEnv);
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
        String sql = generateSql(prestoInfo, jobEnv);
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

    private String generateSql(PrestoInfo prestoInfo, JobEnv jobEnv) {
        String sql;
        String sourcePk = jobEnv.getSourcePk();
        String targetPk = jobEnv.getTargetPk();
        String sourceIndex = jobEnv.getSourceIndex();
        String targetIndex = jobEnv.getTargetIndex();
        // 如果有指定主键
        if (!StringUtils.isEmpty(sourcePk) && !StringUtils.isEmpty(targetPk)) {
            sql = createSqlByPk(sourcePk, targetPk, prestoInfo, jobEnv);
        } else if (!StringUtils.isEmpty(sourceIndex) && !StringUtils.isEmpty(targetIndex)) {
            sql = createSqlByIndex(sourceIndex, targetIndex, prestoInfo, jobEnv);
        } else {
            throw new HandlerException("hdsp.xadt.error.presto.not_support");
        }
        return sql;
    }

    private String createSqlByPk(String sourcePk,
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
        /*
        AB都有的数据
        例：
        `select a.* from devmysql.hdsp_test.resume as a join devmysql.hdsp_test.resume_bak  as b
        ON a.id = b.id WHERE a.id = b.id and a.name = b.name and a.sex = b.sex and a.phone = b
        .call and a.address = b.address and a.education = b.education and a.state = b.state;`
        */
        StringBuilder equalsBuilder = new StringBuilder();
        for (ColMapping mapping : colMappingList) {
            equalsBuilder.append(String.format(SqlConstant.EQUAL, mapping.getSourceCol(), mapping.getTargetCol()));
        }
        builder.append(String.format(SqlConstant.ALL_HAVE_SQL_PK, sourceTable, targetTable, sourcePk, targetPk,
                equalsBuilder.toString()))
                .append(SqlConstant.LINE_END);
        /*
         A有B无
         `select a.* from devmysql.hdsp_test.resume  as a left join devmysql.hdsp_test.resume_bak  as b ON a.id = b.id WHERE b.id is null;`
         */
        builder.append(String.format(SqlConstant.LEFT_HAVE_SQL_PK, sourceTable, targetTable, sourcePk, targetPk, targetPk))
                .append(SqlConstant.LINE_END);
        /*
          A无B有
          `select a.* from devmysql.hdsp_test.resume_bak  as a left join devmysql.hdsp_test.resume  as b ON a.id = b.id WHERE b.id is null  ;`
         */
        //
        builder.append(String.format(SqlConstant.LEFT_HAVE_SQL_PK, targetTable, sourceTable, targetPk, sourcePk, sourcePk))
                .append(SqlConstant.LINE_END);

        /*
          AB主键或唯一索引相同，部分字段不一样
          `select a.* from devmysql.hdsp_test.resume  as a left join devmysql.hdsp_test.resume_bak  as b
          ON a.id = b.id
          WHERE a.id != b.id or a.name != b.name or a.sex != b.sex or a.phone != b.call or a.address != b.address
          or a.education != b.education or a.state != b.state or 1=2  ;`
         */
        StringBuilder whereBuilder = new StringBuilder();
        for (ColMapping mapping : colMappingList) {
            whereBuilder.append(String.format(SqlConstant.NOT_EQUAL, mapping.getSourceCol(), mapping.getTargetCol()));
        }
        whereBuilder.append(SqlConstant.OR_END);
        builder.append(String.format(SqlConstant.ANY_NOT_IN_SQL_PK, sourceTable, targetTable, sourcePk, targetPk,
                whereBuilder.toString()))
                .append(SqlConstant.LINE_END);

        log.debug("==> presto create sql by primary key: {}", builder.toString());
        return builder.toString();
    }

    private String createSqlByIndex(String sourceIndex,
                                    String targetIndex,
                                    PrestoInfo prestoInfo,
                                    JobEnv jobEnv) {
        //获取索引的列
        String[] sourceIndexArray = sourceIndex.split(StringPool.COMMA);
        String[] targetIndexArray = targetIndex.split(StringPool.COMMA);
        if (sourceIndexArray.length != targetIndexArray.length) {
            throw new HandlerException("hdsp.xadt.error.presto.index_length.not_equals");
        }
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
        // 获取除去索引外的所有列
        List<ColMapping> colList = colMappingList.stream().filter(col -> {
            for (int i = 0; i < sourceIndexArray.length; i++) {
                if (sourceIndexArray[i].equalsIgnoreCase(col.getSourceCol())
                        && targetIndexArray[i].equalsIgnoreCase(col.getTargetCol())) {
                    return false;
                }
            }
            return true;
        }).collect(Collectors.toList());

        // 创建on语句 AB型
        StringBuilder onConditionBuilder = new StringBuilder();
        for (int i = 0; i < sourceIndexArray.length; i++) {
            onConditionBuilder.append(String.format(SqlConstant.ON_EQUAL, sourceIndexArray[i], targetIndexArray[i])).append(SqlConstant.AND);
        }
        onConditionBuilder.delete(onConditionBuilder.lastIndexOf(SqlConstant.AND), onConditionBuilder.length());
        String onConditionAB = onConditionBuilder.toString();
        // 创建on语句 AB型
        onConditionBuilder.setLength(0);
        for (int i = 0; i < sourceIndexArray.length; i++) {
            onConditionBuilder.append(String.format(SqlConstant.ON_EQUAL, targetIndexArray[i], sourceIndexArray[i])).append(SqlConstant.AND);
        }
        onConditionBuilder.delete(onConditionBuilder.lastIndexOf(SqlConstant.AND), onConditionBuilder.length());
        String onConditionBA = onConditionBuilder.toString();

        /*
        AB都有的数据
        例：
        `select a.* from devmysql.hdsp_test.resume as a join devmysql.hdsp_test.resume_bak  as b
        ON a.name = b.name and a.phone = b.call WHERE a.id = b.id and a.sex = b.sex and a.address = b.address
        and a.education = b.education and a.state = b.state;`
        */
        StringBuilder equalsBuilder = new StringBuilder();
        for (ColMapping mapping : colList) {
            equalsBuilder.append(String.format(SqlConstant.EQUAL, mapping.getSourceCol(), mapping.getTargetCol()));
        }
        builder.append(String.format(SqlConstant.ALL_HAVE_SQL_INDEX, sourceTable, targetTable, onConditionAB, equalsBuilder.toString()))
                .append(SqlConstant.LINE_END);
        /*
         A有B无
         `select a.* from devmysql.hdsp_test.resume as a left join devmysql.hdsp_test.resume_bak  as b ON a.name = b.name and a.phone = b.call where b.name is null and b.call is null;`
         */
        StringBuilder whereCondition1 = new StringBuilder();
        for (String idx : targetIndexArray) {
            whereCondition1.append(String.format(SqlConstant.IS_NULL, idx)).append(SqlConstant.AND);
        }
        whereCondition1.delete(whereCondition1.lastIndexOf(SqlConstant.AND), whereCondition1.length());
        builder.append(String.format(SqlConstant.LEFT_HAVE_SQL_INDEX, sourceTable, targetTable, onConditionAB,
                whereCondition1.toString()))
                .append(SqlConstant.LINE_END);
        /*
         B有A无
         `select a.* from devmysql.hdsp_test.resume_bak as a left join devmysql.hdsp_test.resume  as b ON a.name = b.name and a.call = b.phone where b.name is null and b.phone is null;`
         */
        StringBuilder whereCondition2 = new StringBuilder();
        for (String idx : sourceIndexArray) {
            whereCondition2.append(String.format(SqlConstant.IS_NULL, idx)).append(SqlConstant.AND);
        }
        whereCondition2.delete(whereCondition2.lastIndexOf(SqlConstant.AND), whereCondition2.length());
        builder.append(String.format(SqlConstant.LEFT_HAVE_SQL_INDEX, targetTable, sourceTable, onConditionBA,
                whereCondition2.toString()))
                .append(SqlConstant.LINE_END);

        /*
          AB唯一索引相同，部分字段不一样
          `select a.* from devmysql.hdsp_test.resume as a left join devmysql.hdsp_test.resume_bak  as b
          ON a.name = b.name and a.phone = b.call
          WHERE a.id != b.id or a.sex != b.sex or a.address != b.address
          or a.education != b.education or a.state != b.state or 1=2  ;`
         */
        StringBuilder whereBuilder = new StringBuilder();
        for (ColMapping mapping : colList) {
            whereBuilder.append(String.format(SqlConstant.NOT_EQUAL, mapping.getSourceCol(), mapping.getTargetCol()));
        }
        whereBuilder.append(SqlConstant.OR_END);
        builder.append(String.format(SqlConstant.ANY_NOT_IN_SQL_INDEX, sourceTable, targetTable, onConditionAB,
                whereBuilder.toString()))
                .append(SqlConstant.LINE_END);

        log.debug("==> presto create sql by index: {}", builder.toString());
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