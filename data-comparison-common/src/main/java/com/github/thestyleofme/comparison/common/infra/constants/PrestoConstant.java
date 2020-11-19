package com.github.thestyleofme.comparison.common.infra.constants;

/**
 * @author siqi.hou@hand-china.com
 * @date 2020-11-17 13:58
 */
public class PrestoConstant {

    private PrestoConstant() {
    }

    public static final String PRESTO_TYPE = "PRESTO";

    public static class SqlConstant {

        // 数据稽核SQL
        public static final String BASE_SQL = "select a.* from %s as a ";
        public static final String ALL_HAVE_SQL_PK = BASE_SQL + "join %s as b on a.%s = b.%s where 1=1 %s;";
        public static final String LEFT_HAVE_SQL_PK = BASE_SQL + "left join %s as b on a.%s = b.%s where b.%s is null;";
        public static final String ANY_NOT_IN_SQL_PK = BASE_SQL + "join %s as b on a.%s = b.%s where %s;";
        public static final String ALL_HAVE_SQL_INDEX = BASE_SQL + "join %s as b on %s where 1=1 %s;";
        public static final String LEFT_HAVE_SQL_INDEX = BASE_SQL + "left join %s as b on %s where %s;";
        public static final String ANY_NOT_IN_SQL_INDEX = BASE_SQL + "join %s as b on %s where %s;";
        // 数据补偿SQL
        public static final String BASE_INSERT_INTO_SQL = "insert into %s (%s) (%s);";

        // 通用SQL常量
        public static final String ON_EQUAL = "a.%s = b.%s ";
        public static final String TABLE_FT = "%s.%s.%s ";
        public static final String EQUAL = "and a.%s = b.%s ";
        public static final String AND = "and ";
        public static final String IS_NULL = "b.%s is null ";
        public static final String NOT_EQUAL = "a.%s != b.%s or ";
        public static final String LINE_END = "\n";
        public static final String OR_END = "1=2 ";

        private SqlConstant() {
        }

    }
}
