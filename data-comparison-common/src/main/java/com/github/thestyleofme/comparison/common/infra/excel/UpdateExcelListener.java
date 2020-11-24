package com.github.thestyleofme.comparison.common.infra.excel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.alibaba.excel.context.AnalysisContext;
import com.github.thestyleofme.comparison.common.domain.ColMapping;
import com.github.thestyleofme.comparison.common.domain.JobEnv;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.infra.exceptions.HandlerException;
import com.github.thestyleofme.comparison.common.infra.handler.sink.excel.ExcelListenerUtil;
import com.github.thestyleofme.comparison.common.infra.utils.CommonUtil;
import com.github.thestyleofme.driver.core.app.service.DriverSessionService;
import com.github.thestyleofme.driver.core.app.service.session.DriverSession;
import com.github.thestyleofme.driver.core.infra.meta.Tuple;
import com.github.thestyleofme.plugin.core.infra.constants.BaseConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;

/**
 * <p>
 * description
 * </p>
 *
 * @author hsq 2020/11/23 17:50
 * @since 1.0.0
 */
@Slf4j
public class UpdateExcelListener<T> extends BaseExcelListener<T> {
    private final List<String> sqlList = new LinkedList<>();
    private final List<CompletableFuture<?>> completableFutureList = new LinkedList<>();
    private final List<ColMapping> colMappingList;
    private final JobEnv jobEnv;
    private final DriverSession driverSession;

    private static final int BATCH_COUNT = 2000;

    public UpdateExcelListener(ComparisonJob comparisonJob,
                               String targetDataSourceCode,
                               List<ColMapping> colMappingList,
                               DriverSessionService driverSessionService) {
        this.jobEnv = CommonUtil.getJobEnv(comparisonJob);
        this.colMappingList = colMappingList;
        this.driverSession = driverSessionService.getDriverSession(comparisonJob.getTenantId(), targetDataSourceCode);
    }

    @Override
    boolean addListBefore(T object) {
        return true;
    }


    @Override
    void doListAfter(T object) {
        List<Tuple<String, String>> tupleList = ExcelListenerUtil.rowToTupleListByColMapping(object, colMappingList);
        String condition = generateCondition(jobEnv, tupleList);
        sqlList.add(driverSession.tableUpdateSql(jobEnv.getTargetTable(), tupleList, condition));
        // 分批写到数据库 防止dataList太大 OOM
        if (sqlList.size() == BATCH_COUNT) {
            updateToTable(new ArrayList<>(sqlList));
            sqlList.clear();
        }
    }

    @Override
    void doAfterAll(AnalysisContext analysisContext) {
        updateToTable(new ArrayList<>(sqlList));
        sqlList.clear();
        CompletableFuture<Void> future = CompletableFuture.allOf(completableFutureList.toArray(new CompletableFuture[0]));
        try {
            future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new HandlerException(e);
        } catch (ExecutionException e) {
            throw new HandlerException(e);
        }
    }

    private void updateToTable(List<String> list) {
        CompletableFuture<Boolean> completableFuture = CompletableFuture.supplyAsync(() -> {
            if (driverSession.supportedBatch()) {
                driverSession.executeBatch(jobEnv.getTargetSchema(), list);
            } else {
                for (String sql : list) {
                    driverSession.executeOneUpdate(null, sql);
                }
            }
            log.debug("update {} data to the table", list.size());
            return true;
        });
        completableFutureList.add(completableFuture);
    }


    public String generateCondition(JobEnv jobEnv, List<Tuple<String, String>> row) {
        String condition;
        String sourcePk = jobEnv.getSourcePk();
        String targetPk = jobEnv.getTargetPk();
        String sourceIndex = jobEnv.getSourceIndex();
        String targetIndex = jobEnv.getTargetIndex();
        // 如果有指定主键
        if (!StringUtils.isEmpty(sourcePk) && !StringUtils.isEmpty(targetPk)) {
            Tuple<String, String> kv = row.stream()
                    .filter(tuple -> targetPk.equalsIgnoreCase(tuple.getFirst()))
                    .findFirst()
                    .orElseThrow(() -> new HandlerException("hdsp.xadt.error.sql.pk.not_find"));
            condition = String.format("%s=%s", targetPk, kv.getSecond());
        } else if (!StringUtils.isEmpty(sourceIndex) && !StringUtils.isEmpty(targetIndex)) {
            List<String> idx = Arrays.asList(targetIndex.split(BaseConstant.Symbol.COMMA));
            condition = row.stream().filter(tuple -> idx.contains(tuple.getFirst()))
                    .map(kv -> String.format("%s=%s", kv.getFirst(), kv.getSecond()))
                    .collect(Collectors.joining(BaseConstant.Symbol.COMMA));
        } else {
            throw new HandlerException("hdsp.xadt.error.deploy.condition.not_support");
        }
        return condition;
    }
}
