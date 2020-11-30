package com.github.thestyleofme.comparison.common.infra.excel;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.alibaba.excel.context.AnalysisContext;
import com.github.thestyleofme.comparison.common.domain.JobEnv;
import com.github.thestyleofme.comparison.common.domain.SelectTableInfo;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.infra.exceptions.HandlerException;
import com.github.thestyleofme.comparison.common.infra.handler.sink.excel.ExcelListenerUtil;
import com.github.thestyleofme.comparison.common.infra.utils.CommonUtil;
import com.github.thestyleofme.driver.core.app.service.DriverSessionService;
import com.github.thestyleofme.driver.core.app.service.session.DriverSession;
import com.github.thestyleofme.driver.core.infra.meta.Tuple;
import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/18 16:13
 * @since 1.0.0
 */
@Slf4j
public class InsertExcelListener<T> extends BaseExcelListener<T> {

    private final List<String> sqlList = new LinkedList<>();
    private final List<CompletableFuture<?>> completableFutureList = new LinkedList<>();
    private final List<List<String>> excelHeader;
    private final JobEnv jobEnv;
    private final DriverSession driverSession;
    private static final int BATCH_COUNT = 2048;

    public InsertExcelListener(ComparisonJob comparisonJob,
                               String targetDataSourceCode,
                               List<List<String>> excelHeader,
                               DriverSessionService driverSessionService) {
        this.jobEnv = CommonUtil.getJobEnv(comparisonJob);
        this.excelHeader = excelHeader;
        this.driverSession = driverSessionService.getDriverSession(comparisonJob.getTenantId(), targetDataSourceCode);
    }

    @Override
    boolean addListBefore(T object) {
        return true;
    }

    @Override
    void doListAfter(T object) {
        List<Tuple<String, String>> tupleList = ExcelListenerUtil.rowToTupleList(object, excelHeader);
        SelectTableInfo target = jobEnv.getTarget();
        sqlList.add(driverSession.tableInsertSql(target.getTable(), tupleList));
        // 分批写到数据库 防止dataList太大 OOM
        if (sqlList.size() == BATCH_COUNT) {
            saveToTable(new ArrayList<>(sqlList));
            sqlList.clear();
        }
    }

    private void saveToTable(List<String> list) {
        CompletableFuture<Boolean> completableFuture = CompletableFuture.supplyAsync(() -> {
            if (driverSession.supportedBatch()) {
                SelectTableInfo target = jobEnv.getTarget();
                driverSession.executeBatch(target.getSchema(), list);
            } else {
                for (String sql : list) {
                    driverSession.executeOneUpdate(null, sql);
                }
            }
            log.debug("add {} data to the table", list.size());
            return true;
        });
        completableFutureList.add(completableFuture);
    }

    @Override
    void doAfterAll(AnalysisContext analysisContext) {
        saveToTable(new ArrayList<>(sqlList));
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
}
