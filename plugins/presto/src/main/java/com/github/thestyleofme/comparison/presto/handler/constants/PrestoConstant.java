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

        public static final String BASE_SQL = "select _a.* from %s as _a ";
        public static final String JOIN_SQL_PK = BASE_SQL + "join %s as _b on _a.%s = _b.%s where %s;";
        public static final String LEFT_HAVE_SQL_PK = BASE_SQL + "left join %s as _b on _a.%s = _b.%s where _b.%s is null;";

        public static final String ALL_HAVE_SQL_INDEX = BASE_SQL + "join %s as _b on %s where %s;";
        public static final String LEFT_HAVE_SQL_INDEX = BASE_SQL + "left join %s as _b on %s where %s;";
        public static final String ANY_NOT_IN_SQL_INDEX = BASE_SQL + "join %s as _b on %s where %s;";

        public static final String TABLE_FT = "%s.%s.%s ";
        public static final String LINE_END = "\n";

        private SqlConstant() {
        }

    }
}
