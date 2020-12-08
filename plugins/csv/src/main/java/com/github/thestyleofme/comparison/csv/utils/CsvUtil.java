package com.github.thestyleofme.comparison.csv.utils;

import java.util.Optional;

import com.github.thestyleofme.comparison.common.infra.utils.CommonUtil;
import com.github.thestyleofme.comparison.csv.pojo.CsvInfo;

/**
 * <p></p>
 *
 * @author hsq 2020/12/03 9:32
 * @since 1.0.0
 */
public class CsvUtil {

    private CsvUtil() {
    }

    public static String getCsvPath(CsvInfo csvInfo, Long tenantId, String jobCode, int option) {
        String dir = Optional.ofNullable(csvInfo.getPath())
                .orElseGet(() -> CommonUtil.createDirPath(String.format("csv/%s_%s", tenantId, jobCode)));
        return String.format("%s/%d_%s_%d.csv", dir, tenantId, jobCode, option);
    }
}
