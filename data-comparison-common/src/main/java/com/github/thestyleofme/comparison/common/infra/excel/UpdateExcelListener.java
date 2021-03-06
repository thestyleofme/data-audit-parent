package com.github.thestyleofme.comparison.common.infra.excel;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.alibaba.excel.context.AnalysisContext;
import com.github.thestyleofme.comparison.common.domain.ColMapping;
import com.github.thestyleofme.comparison.common.domain.JobEnv;
import com.github.thestyleofme.comparison.common.domain.SelectTableInfo;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.infra.constants.ErrorCode;
import com.github.thestyleofme.comparison.common.infra.exceptions.HandlerException;
import com.github.thestyleofme.comparison.common.infra.handler.sink.excel.ExcelListenerUtil;
import com.github.thestyleofme.comparison.common.infra.utils.CommonUtil;
import com.github.thestyleofme.driver.core.app.service.DriverSessionService;
import com.github.thestyleofme.driver.core.app.service.session.DriverSession;
import com.github.thestyleofme.driver.core.infra.meta.Tuple;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;

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
    private final SelectTableInfo target;
    private final DriverSession driverSession;
    private final List<ColMapping> joinMappingList;

    private static final int BATCH_COUNT = 2000;

    public UpdateExcelListener(ComparisonJob comparisonJob,
                               String targetDataSourceCode,
                               List<ColMapping> colMappingList,
                               DriverSessionService driverSessionService) {
        JobEnv jobEnv = CommonUtil.getJobEnv(comparisonJob);
        this.target = jobEnv.getTarget();
        this.joinMappingList = CommonUtil.getJoinMappingList(jobEnv);
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
        String condition = generateCondition(tupleList);
        sqlList.add(driverSession.tableUpdateSql(target.getTable(), tupleList, condition));
        // 分批写到数据库 防止dataList太大 OOM
        if (sqlList.size() == BATCH_COUNT) {
            updateToTable(new ArrayList<>(sqlList));
            sqlList.clear();
        }
    }

    @Override
    void doAfterAll(AnalysisContext analysisContext) {
        updateToTable(sqlList);
        CommonUtil.completableFutureAllOf(completableFutureList);
    }

    private void updateToTable(List<String> list) {
        CompletableFuture<Boolean> completableFuture = CompletableFuture.supplyAsync(() -> {
            if (driverSession.supportedBatch()) {
                driverSession.executeBatch(target.getSchema(), list);
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


    public String generateCondition(List<Tuple<String, String>> row) {
        String condition;
        // 如果有指定主键
        if (!CollectionUtils.isEmpty(joinMappingList)) {
            List<String> idx = joinMappingList.stream()
                    .map(ColMapping::getTargetCol)
                    .collect(Collectors.toList());
            condition = row.stream().filter(tuple -> idx.contains(tuple.getFirst()))
                    .map(kv -> String.format("%s='%s'", kv.getFirst(), kv.getSecond()))
                    .collect(Collectors.joining(" and "));
        } else {
            throw new HandlerException(ErrorCode.DEPLOY_JOIN_CONDITION_IS_NULL);
        }
        return condition;
    }
}
