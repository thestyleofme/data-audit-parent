package com.github.thestyleofme.comparison.common.infra.excel;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.alibaba.excel.context.AnalysisContext;
import com.github.thestyleofme.comparison.common.infra.handler.sink.excel.ExcelListenerUtil;
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
public class CommonExcelListener<T> extends BaseExcelListener<T> {

    private final List<Map<String, Object>> dataList = new LinkedList<>();
    private final List<List<String>> excelHeader;
    private static final int BATCH_COUNT = 1024;

    public CommonExcelListener(List<List<String>> excelHeader) {
        this.excelHeader = excelHeader;
    }

    @Override
    boolean addListBefore(T object) {
        return true;
    }

    @Override
    void doListAfter(T object) {
        Map<String, Object> dataMap = ExcelListenerUtil.rowToMap(object, excelHeader);
        dataList.add(dataMap);
        // 分批写到数据库
        if (dataList.size() >= BATCH_COUNT) {
            // todo 写到数据库
            log.debug("add {} data to the table", dataList.size());
            dataList.clear();
        }
    }

    @Override
    void doAfterAll(AnalysisContext analysisContext) {
        // 别遗漏 todo 写到数据库
        log.debug("add {} data to the table", dataList.size());
        dataList.clear();
    }
}
