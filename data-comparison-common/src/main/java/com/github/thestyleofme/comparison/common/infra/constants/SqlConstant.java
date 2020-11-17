package com.github.thestyleofme.comparison.common.infra.constants;

/**
 * SQL语句常量
 * @author siqi.hou@hand-china.com
 * @date 2020-11-17 9:57
 */
public class SqlConstant {
    public static final String SELECT_SQL = "SELECT * FROM ";
    public static final String LEFT_SQL = "select a.* from %s as a left join %s as b ";
    public static final String JOIN = "JOIN ";
    public static final String TABLE_FT = "%s.%s.%s ";
    public static final String WHERE = "WHERE %s ";
    public static final String ON = "ON %s ";
    public static final String ON_PK = "ON a.%s = b.%s ";
    public static final String OR = "or ";
    public static final String EQUALS = "and a.%s = b.%s ";
    public static final String NOT_EQUALS = "a.%s != b.%s or ";
    public static final String LEFT_IS_NULL = "a.%s is null ";
    public static final String RIGHT_IS_NULL = "b.%s is null ";
    public static final String LEFT_IS_NOT_NULL = "a.%s is not null ";
    public static final String RIGHT_IS_NOT_NULL = "b.%s is not null ";
    public static final String OR_END = "1=2 ";
    public static final String AND_END = "and 1=1 ";
    public static final String LINE_END = ";\n";
    private SqlConstant(){}

}
