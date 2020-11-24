package com.github.thestyleofme.comparison.common.infra.constants;

/**
 * @author hsq
 * @date 2020-11-17 13:58
 */
public class PrestoConstant {

    private PrestoConstant() {
    }

    public static class SqlConstant {

        // 数据稽核SQL
        public static final String BASE_SQL = "select _a.* from %s as _a ";
        public static final String ALL_HAVE_SQL_PK = BASE_SQL + "join %s as _b on _a.%s = _b.%s where 1=1 %s;";
        public static final String LEFT_HAVE_SQL_PK = BASE_SQL + "left join %s as _b on _a.%s = _b.%s where _b.%s is null;";
        public static final String ANY_NOT_IN_SQL_PK = BASE_SQL + "join %s as _b on _a.%s = _b.%s where %s;";
        public static final String ALL_HAVE_SQL_INDEX = BASE_SQL + "join %s as _b on %s where 1=1 %s;";
        public static final String LEFT_HAVE_SQL_INDEX = BASE_SQL + "left join %s as _b on %s where %s;";
        public static final String ANY_NOT_IN_SQL_INDEX = BASE_SQL + "join %s as _b on %s where %s;";


        // 通用SQL常量
        public static final String ON_EQUAL = "_a.%s = _b.%s ";
        public static final String TABLE_FT = "%s.%s.%s ";
        public static final String EQUAL = "and _a.%s = _b.%s ";
        public static final String A_COLS = "_a.*";
        public static final String A_COL = "_a.%s";

        public static final String AND = "and ";
        public static final String IS_NULL = "_b.%s is null ";
        public static final String NOT_EQUAL = "_a.%s != _b.%s or ";
        public static final String LINE_END = "\n";
        public static final String OR_END = "1=2 ";

        private SqlConstant() {
        }

    }
}
