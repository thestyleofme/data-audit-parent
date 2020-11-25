package com.github.thestyleofme.comparison.phoenix;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import javax.sql.DataSource;

import com.github.thestyleofme.comparison.common.app.service.sink.BaseSinkHandler;
import com.github.thestyleofme.comparison.common.app.service.transform.HandlerResult;
import com.github.thestyleofme.comparison.common.domain.ColMapping;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.infra.annotation.SinkType;
import com.github.thestyleofme.comparison.common.infra.exceptions.HandlerException;
import com.github.thestyleofme.comparison.common.infra.utils.CommonUtil;
import com.github.thestyleofme.comparison.common.infra.utils.HandlerUtil;
import com.github.thestyleofme.comparison.common.infra.utils.ThreadPoolUtil;
import com.github.thestyleofme.comparison.phoenix.constant.PhoenixConstant;
import com.github.thestyleofme.comparison.phoenix.constant.RowTypeEnum;
import com.github.thestyleofme.comparison.phoenix.context.PhoenixDatasourceHolder;
import com.github.thestyleofme.comparison.phoenix.pojo.DatasourceInfo;
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
        // AB主键或唯一索引相同
        futures.add(executorService.submit(() -> {
            executeSql(dataSource, RowTypeEnum.UPDATED.getRawType(), jobName,
                    sqlPrefix, handlerResult.getPkOrIndexSameDataList());
            return true;
        }));
        // AB相同数据
        futures.add(executorService.submit(() -> {
            executeSql(dataSource, RowTypeEnum.SAME.getRawType(), jobName,
                    sqlPrefix, handlerResult.getSameDataList());
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
                            List<LinkedHashMap<String, Object>> mapList) {
        LocalDateTime start = LocalDateTime.now();
        StringBuilder valueStringBuilder = new StringBuilder();
        String prefix = String.format("%d, '%s', ", rawType, jobName);
        List<String> list = new LinkedList<>();
        List<CompletableFuture<?>> futureList = new ArrayList<>();
        for (LinkedHashMap<String, Object> map : mapList) {
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
}
