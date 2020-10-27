package com.github.thestyleofme.data.comparison.infra.utils;

import java.util.List;

import com.alibaba.excel.EasyExcelFactory;
import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/27 10:44
 * @since 1.0.0
 */
@Slf4j
public class ExcelUtil {

    private ExcelUtil() {
    }

    public static void writeExcel(String excelName,
                                  String sheetName,
                                  List<List<String>> excelSourceHeaderList,
                                  List<List<Object>> sourceDataList) {
        EasyExcelFactory.write(excelName)
                .head(excelSourceHeaderList)
                .sheet(sheetName)
                .doWrite(sourceDataList);
    }
}
