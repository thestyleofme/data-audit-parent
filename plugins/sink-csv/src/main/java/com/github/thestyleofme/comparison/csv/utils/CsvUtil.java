package com.github.thestyleofme.comparison.csv.utils;

/**
 * <p></p>
 *
 * @author hsq 2020/12/03 9:32
 * @since 1.0.0
 */
public class CsvUtil {
    private CsvUtil() {
    }

    public static String getCsvPath(String dirPath, Long tenantId, String jobCode, int option) {
        return String.format("%s/%d_%s_%d.csv", dirPath, tenantId, jobCode, option);
    }
}
