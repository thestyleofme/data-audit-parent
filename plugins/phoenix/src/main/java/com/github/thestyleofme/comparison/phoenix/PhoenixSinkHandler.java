package com.github.thestyleofme.comparison.phoenix;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.sql.DataSource;

import com.github.thestyleofme.comparison.common.app.service.sink.BaseSinkHandler;
import com.github.thestyleofme.comparison.common.app.service.transform.HandlerResult;
import com.github.thestyleofme.comparison.common.domain.ColMapping;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.domain.entity.Reader;
import com.github.thestyleofme.comparison.common.infra.annotation.SinkType;
import com.github.thestyleofme.comparison.common.infra.constants.ErrorCode;
import com.github.thestyleofme.comparison.common.infra.constants.RowTypeEnum;
import com.github.thestyleofme.comparison.common.infra.exceptions.HandlerException;
import com.github.thestyleofme.comparison.common.infra.utils.CommonUtil;
import com.github.thestyleofme.comparison.common.infra.utils.HandlerUtil;
import com.github.thestyleofme.comparison.common.infra.utils.ThreadPoolUtil;
import com.github.thestyleofme.comparison.phoenix.constant.PhoenixConstant;
import com.github.thestyleofme.comparison.phoenix.context.PhoenixDatasourceHolder;
import com.github.thestyleofme.comparison.phoenix.pojo.DatasourceInfo;
import com.github.thestyleofme.comparison.phoenix.pojo.DataxPhoenixReader;
import com.github.thestyleofme.comparison.phoenix.utils.PhoenixHelper;
import com.github.thestyleofme.plugin.core.infra.utils.BeanUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/23 17:39
 * @since 1.0.0
 */
@Component
@SinkType("PHOENIX")
@Slf4j
public class PhoenixSinkHandler implements BaseSinkHandler {

    private final ExecutorService executorService = ThreadPoolUtil.getExecutorService();
    public static final Pattern PHOENIX_JDBC_PATTERN = Pattern.compile("jdbc:phoenix:thin:url=(.*?)");
    public static final String PHOENIX_SERIALIZATION = ";serialization=";

    @Override
    public void handle(ComparisonJob comparisonJob,
                       Map<String, Object> env,
                       Map<String, Object> sinkMap,
                       HandlerResult handlerResult) {
        // 创建数据源
        DatasourceInfo datasourceInfo = BeanUtils.map2Bean(sinkMap, DatasourceInfo.class);
        DataSource dataSource = PhoenixDatasourceHolder.getOrCreate(datasourceInfo);
        // 创库 创表 创序列
        PhoenixHelper.prepare(dataSource);
        // 往phoenix写结果数据
        doSink(dataSource, comparisonJob, handlerResult);
    }

    private void doSink(DataSource dataSource,
                        ComparisonJob comparisonJob,
                        HandlerResult handlerResult) {
        String jobName = comparisonJob.getJobCode();
        List<ColMapping> colMappingList = CommonUtil.getColMappingList(comparisonJob);
        String columns = colMappingList.stream()
                .map(colMapping -> String.format("\"0\".\"%s\" VARCHAR", colMapping.getSourceCol()))
                .collect(Collectors.joining(", "));
        String sqlPrefix = String.format("%s %s) %s ", PhoenixConstant.UPSET_SQL_PREFIX,
                columns, PhoenixConstant.UPSET_SQL_VALUES);
        doSink(dataSource, handlerResult, jobName, sqlPrefix);
    }

    private void doSink(DataSource dataSource, HandlerResult handlerResult, String jobName, String sqlPrefix) {
        // A有B无
        List<Future<?>> futures = new ArrayList<>(4);
        futures.add(executorService.submit(() -> {
            executeSql(dataSource, RowTypeEnum.INSERT.getRawType(), jobName,
                    sqlPrefix, handlerResult.getSourceUniqueDataList());
            return true;
        }));
        // B有A无
        futures.add(executorService.submit(() -> {
            executeSql(dataSource, RowTypeEnum.DELETED.getRawType(), jobName,
                    sqlPrefix, handlerResult.getTargetUniqueDataList());
            return true;
        }));
        // AB主键或唯一索引相同 其他字段值不同
        futures.add(executorService.submit(() -> {
            executeSql(dataSource, RowTypeEnum.UPDATED.getRawType(), jobName,
                    sqlPrefix, handlerResult.getPkOrIndexSameDataList());
            return true;
        }));
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new HandlerException(e);
            } catch (ExecutionException e) {
                throw new HandlerException(e);
            }
        }
    }

    private void executeSql(DataSource dataSource,
                            int rawType,
                            String jobName,
                            String sqlPrefix,
                            List<Map<String, Object>> mapList) {
        LocalDateTime start = LocalDateTime.now();
        StringBuilder valueStringBuilder = new StringBuilder();
        String prefix = String.format("%d, '%s', ", rawType, jobName);
        List<String> list = new LinkedList<>();
        List<CompletableFuture<?>> futureList = new ArrayList<>();
        for (Map<String, Object> map : mapList) {
            String collect = map.values().stream()
                    .map(o -> String.format("'%s'", CommonUtil.requireNonNullElse(o, "")))
                    .collect(Collectors.joining(", "));
            valueStringBuilder.append(collect).append(")");
            String sql = sqlPrefix + prefix + valueStringBuilder.toString();
            list.add(sql);
            // 别忘了清空
            valueStringBuilder.setLength(0);
            if (list.size() == 1024) {
                // 分批执行 1024 一批
                futureList.add(PhoenixHelper.executeAsync(dataSource, new ArrayList<>(list)));
                list.clear();
            }
        }
        // 剩余的sql集合
        if (!CollectionUtils.isEmpty(list)) {
            futureList.add(PhoenixHelper.executeAsync(dataSource, new ArrayList<>(list)));
        }
        CommonUtil.completableFutureAllOf(futureList);
        log.debug("execute sql cost: {}", HandlerUtil.timestamp2String(Duration.between(start, LocalDateTime.now()).toMillis()));
    }

    @Override
    public Reader dataxReader(ComparisonJob comparisonJob, Map<String, Object> sinkMap, Integer syncType) {
        // 生成phoenix查询sql
        String jobName = comparisonJob.getJobCode();
        List<ColMapping> colMappingList = CommonUtil.getColMappingList(comparisonJob);
        String sql = genPhoenixQuerySql(colMappingList, jobName, syncType);
        // 封装datax phoenix reader
        return genDataxPhoenixReader(sinkMap, sql);
    }

    private DataxPhoenixReader genDataxPhoenixReader(Map<String, Object> sinkMap, String sql) {
        String jdbcUrl = (String) sinkMap.get(DatasourceInfo.FIELD_JDBC_URL);
        Matcher matcher = PHOENIX_JDBC_PATTERN.matcher(jdbcUrl);
        String queryServerAddress;
        if (matcher.matches()) {
            queryServerAddress = matcher.group(1);
        } else {
            throw new HandlerException(ErrorCode.JOB_PHOENIX_JDBC_URL_NOT_FOUND);
        }
        DataxPhoenixReader dataxPhoenixReader = new DataxPhoenixReader();
        dataxPhoenixReader.setParameter(DataxPhoenixReader.Parameter.builder()
                .queryServerAddress(queryServerAddress)
                .querySql(Collections.singletonList(sql))
                .build());
        if (queryServerAddress.contains(PHOENIX_SERIALIZATION)) {
            String serialization = queryServerAddress.substring(queryServerAddress.indexOf(PHOENIX_SERIALIZATION) + PHOENIX_SERIALIZATION.length() + 1);
            dataxPhoenixReader.getParameter().setSerialization(serialization);
        }
        return dataxPhoenixReader;
    }

    private String genPhoenixQuerySql(List<ColMapping> colMappingList, String jobName, Integer rowType) {
        String selectColumns = colMappingList.stream()
                .map(colMapping -> String.format("\"0\".\"%s\"", colMapping.getSourceCol()))
                .collect(Collectors.joining(", "));
        StringBuilder stringBuilder = new StringBuilder("SELECT ");
        stringBuilder.append(selectColumns).append(" FROM data_audit.audit_result(");
        String fromColumns = colMappingList.stream()
                .map(colMapping -> String.format("\"0\".\"%s\" VARCHAR", colMapping.getSourceCol()))
                .collect(Collectors.joining(", "));
        stringBuilder.append(fromColumns).append(")");
        stringBuilder.append("WHERE \"-1\".job_name='").append(jobName).append("' ");
        stringBuilder.append("AND \"-1\".row_type=").append(rowType);
        return stringBuilder.toString();
    }

}
