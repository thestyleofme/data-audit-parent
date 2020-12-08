package com.github.thestyleofme.comparison.common.infra.utils;

import org.springframework.util.StringUtils;

/**
 * <p></p>
 *
 * @author hsq 2020/12/04 9:34
 * @since 1.0.0
 */
public class SqlUtils {

    private SqlUtils() {
    }

    private static final String SOURCE = "$1";
    private static final String TARGET = "$2";
    private static final String SOURCE_NAME = "_a";
    private static final String TARGET_NAME = "_b";
    private static final String AND_LEFT = " and (";



    public static String getBothWhereCondition(String sourceGlobalWhere, String sourceWhere,
                                               String targetGlobalWhere, String targetWhere) {
        StringBuilder builder = new StringBuilder();
        builder.append("where 1=1 ");
        append(sourceGlobalWhere, builder);
        append(targetGlobalWhere,builder);
        append(sourceWhere,builder);
        append(targetWhere,builder);
        // 将 $1 $2 替换为_a _b
        return builder.toString().replace(SOURCE, SOURCE_NAME).replace(TARGET, TARGET_NAME);
    }

    private static void append(String content, StringBuilder builder) {
        if (!StringUtils.isEmpty(content)) {
            builder.append(AND_LEFT).append(content).append(")");
        }
    }

    public static String getOneWhereCondition(String globalWhere, String where) {
        StringBuilder builder = new StringBuilder();
        builder.append("where 1=1 ");
        append(globalWhere, builder);
        append(where,builder);
        // 将 $1 $2 替换为_a _b
        return builder.toString().replace(SOURCE, SOURCE_NAME).replace(TARGET, TARGET_NAME);
    }

}
