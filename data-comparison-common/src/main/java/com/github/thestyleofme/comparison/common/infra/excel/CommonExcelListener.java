package com.github.thestyleofme.comparison.common.infra.excel;

import java.util.LinkedList;
import java.util.List;

import com.alibaba.excel.context.AnalysisContext;
import com.github.thestyleofme.comparison.common.domain.JobEnv;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.infra.handler.sink.excel.ExcelListenerUtil;
import com.github.thestyleofme.comparison.common.infra.utils.CommonUtil;
import com.github.thestyleofme.driver.core.app.service.DriverSessionService;
import com.github.thestyleofme.driver.core.app.service.session.DriverSession;
import com.github.thestyleofme.driver.core.infra.meta.Tuple;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/18 16:13
 * @since 1.0.0
 */
@Slf4j
public class CommonExcelListener<T> extends BaseExcelListener<T> {

    private final List<String> sqlList = new LinkedList<>();
    private final List<List<String>> excelHeader;
    private final JobEnv jobEnv;
    private final DriverSession driverSession;
    private static final int BATCH_COUNT = 1024;

    public CommonExcelListener(ComparisonJob comparisonJob,
                               List<List<String>> excelHeader,
                               DriverSessionService driverSessionService) {
        this.jobEnv = CommonUtil.getJobEnv(comparisonJob);
        this.excelHeader = excelHeader;
        this.driverSession = driverSessionService.getDriverSession(comparisonJob.getTenantId(), jobEnv.getTargetDatasourceCode());
    }

    @Override
    boolean addListBefore(T object) {
        return true;
    }

    @Override
    void doListAfter(T object) {
        List<Tuple<String, String>> tupleList = ExcelListenerUtil.rowToTupleList(object, excelHeader);
        sqlList.add(driverSession.tableInsertSql(jobEnv.getTargetTable(), tupleList));
        // 分批写到数据库 防止dataList太大 OOM
        if (sqlList.size() == BATCH_COUNT) {
            saveToTable();
            log.debug("add {} data to the table", sqlList.size());
            sqlList.clear();
        }
    }

    private void saveToTable() {
        if (StringUtils.isEmpty(jobEnv.getTargetDatasourceCode())) {
            // todo 使用presto导入到表
            throw new UnsupportedOperationException();
        } else {
            driverSession.executeBatch(jobEnv.getTargetSchema(), sqlList);
        }
    }

    @Override
    void doAfterAll(AnalysisContext analysisContext) {
        saveToTable();
        log.debug("add {} data to the table", sqlList.size());
        sqlList.clear();
    }
}
