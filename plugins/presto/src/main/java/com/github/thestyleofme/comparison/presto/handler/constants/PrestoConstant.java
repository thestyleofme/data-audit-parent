package com.github.thestyleofme.comparison.presto.handler.constants;

/**
 * <p>
 * description
 * </p>
 *
 * @author hsq 2020/11/26 13:41
 * @since 1.0.0
 */
public class PrestoConstant {

    private PrestoConstant() {
    }

    public static class SqlConstant {

        //===============================================================================
        //  数据稽核SQL
        //===============================================================================

        public static final String BASE_SQL = "SELECT %s FROM %s as _a %s %s as _b on %s ";
        public static final String JOIN = "JOIN";
        public static final String LEFT_JOIN = "LEFT JOIN";
        public static final String RIGHT_JOIN = "RIGHT JOIN";

        public static final String TABLE_FT = "%s.%s.%s ";
        public static final String LINE_END = "\n";

        private SqlConstant() {
        }

    }
}
