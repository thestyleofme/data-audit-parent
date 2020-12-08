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

    public static String getBothWhereCondition(StringBuilder builder, String sourceGlobalWhere, String sourceWhere,
                                               String targetGlobalWhere, String targetWhere) {
        builder.setLength(0);
        builder.append("where 1=1 ");
        if (!StringUtils.isEmpty(sourceGlobalWhere)) {
            builder.append(AND_LEFT).append(sourceGlobalWhere).append(")");
        }
        if (!StringUtils.isEmpty(targetGlobalWhere)) {
            builder.append(AND_LEFT).append(targetGlobalWhere).append(")");
        }
        if (!StringUtils.isEmpty(sourceWhere)) {
            builder.append(AND_LEFT).append(sourceWhere).append(")");
        }
        if (!StringUtils.isEmpty(targetWhere)) {
            builder.append(AND_LEFT).append(targetWhere).append(")");
        }
        // 将 $1 $2 替换为_a _b
        String result = builder.toString().replace(SOURCE, SOURCE_NAME).replace(TARGET, TARGET_NAME);
        builder.setLength(0);
        return result;
    }

    public static String getOneWhereCondition(StringBuilder builder, String globalWhere, String where) {
        builder.setLength(0);
        builder.append("where 1=1 ");
        if (!StringUtils.isEmpty(globalWhere)) {
            builder.append(AND_LEFT).append(globalWhere).append(")");
        }
        if (!StringUtils.isEmpty(where)) {
            builder.append(AND_LEFT).append(where).append(")");
        }
        // 将 $1 $2 替换为_a _b
        String result = builder.toString().replace(SOURCE, SOURCE_NAME).replace(TARGET, TARGET_NAME);
        builder.setLength(0);
        return result;
    }

}
