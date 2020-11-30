package com.github.thestyleofme.comparison.presto.handler.utils;

import static com.github.thestyleofme.comparison.presto.handler.constants.PrestoConstant.SqlConstant.*;

import java.util.List;
import java.util.stream.Collectors;

import com.github.thestyleofme.comparison.common.domain.ColMapping;
import com.github.thestyleofme.comparison.common.domain.SelectTableInfo;
import com.github.thestyleofme.comparison.common.infra.exceptions.HandlerException;
import com.github.thestyleofme.comparison.presto.handler.pojo.PrestoInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * <p>
 * description
 * </p>
 *
 * @author hsq 2020/11/26 13:48
 * @since 1.0.0
 */
@Slf4j
public class SqlGeneratorUtil {

    private SqlGeneratorUtil() {

    }

    private static final String SOURCE = "$1";
    private static final String TARGET = "$2";
    private static final String SOURCE_NAME = "_a";
    private static final String TARGET_NAME = "_b";
    private static final String AND_LEFT = " and (";

    public static String generateAuditSql(PrestoInfo prestoInfo) {
        String sql;
        List<ColMapping> joinMappingList = prestoInfo.getJoinMapping();
        // 如果有指定主键或唯一索引
        if (!CollectionUtils.isEmpty(joinMappingList)) {
            sql = createSqlByPkOrIndex(prestoInfo);
        } else {
            throw new HandlerException("hdsp.xadt.error.presto.not_support");
        }
        return sql;
    }

    public static String getTableName(SelectTableInfo tableInfo) {
        return String.format(TABLE_FT, tableInfo.getCatalog(),
                tableInfo.getSchema(), tableInfo.getTable());
    }

    private static String getBothWhereCondition(StringBuilder builder, String sourceGlobalWhere, String sourceWhere,
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

    private static String getOneWhereCondition(StringBuilder builder, String globalWhere, String where) {
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

    private static String createSqlByPkOrIndex(PrestoInfo prestoInfo) {
        SelectTableInfo target = prestoInfo.getTarget();
        SelectTableInfo source = prestoInfo.getSource();
        String sourceTable = prestoInfo.getSourceTableName();
        String targetTable = prestoInfo.getTargetTableName();

        // 索引的列
        List<ColMapping> joinMappingList = prestoInfo.getJoinMapping();
        // 所有的列映射信息
        List<ColMapping> colMappingList = prestoInfo.getColMapping();
        // 需要对比的列
        List<ColMapping> colList = colMappingList.stream()
                .filter(ColMapping::isSelected)
                .collect(Collectors.toList());

        StringBuilder builder = new StringBuilder();

        // _a.id = _b.id and _a.x = _b.x
        String onCondition = joinMappingList.stream().map(colMapping ->
                String.format(" _a.%s = _b.%s ", colMapping.getSourceCol(), colMapping.getTargetCol()))
                .collect(Collectors.joining(" and "));
        String bothWhere = getBothWhereCondition(builder, source.getGlobalWhere(), source.getWhere(),
                target.getGlobalWhere(), target.getWhere());
        String sourceWhere = getOneWhereCondition(builder, source.getGlobalWhere(), source.getWhere());
        String targetWhere = getOneWhereCondition(builder, target.getGlobalWhere(), target.getWhere());
        /*
        1. AB都有的数据
        例：
        ``
        */

        String where1 = colList.stream()
                .map(col -> String.format(" _a.%s = _b.%s ", col.getSourceCol(), col.getTargetCol()))
                .collect(Collectors.joining("and", "and (", ")"));
        String sql1 = String.format(BASE_SQL, "count(*) as count", sourceTable, JOIN, targetTable, onCondition);
        builder.append(sql1).append(bothWhere).append(where1).append(";").append(LINE_END);
        /*
         2. A有B无
         ``
         */
        //"SELECT %s FROM %s as _a %s %s as _b on %s "
        String sourceCol = colMappingList.stream()
                .map(colMapping -> String.format("_a.%s", colMapping.getSourceCol()))
                .collect(Collectors.joining(","));
        String where2 = joinMappingList.stream()
                .map(col -> String.format(" _b.%s is null ", col.getTargetCol()))
                .collect(Collectors.joining("and", "and (", ")"));
        String sql2 = String.format(BASE_SQL, sourceCol, sourceTable, LEFT_JOIN, targetTable, onCondition);
        builder.append(sql2).append(sourceWhere).append(where2).append(";").append(LINE_END);
        /*
         3. B有A无
         ``
         */
        String targetCol = colMappingList.stream()
                .map(colMapping -> String.format("_b.%s", colMapping.getTargetCol()))
                .collect(Collectors.joining(","));
        String where3 = joinMappingList.stream()
                .map(col -> String.format(" _a.%s is null ", col.getSourceCol()))
                .collect(Collectors.joining("or", " and (", ")"));
        String sql3 = String.format(BASE_SQL, targetCol, sourceTable, RIGHT_JOIN, targetTable, onCondition);
        builder.append(sql3).append(targetWhere).append(where3).append(";").append(LINE_END);

        /*
          4. AB唯一索引相同，部分字段不一样
          ``
         */
        String where4 = colList.stream()
                .map(col -> String.format(" _a.%s != _b.%s ", col.getSourceCol(), col.getTargetCol()))
                .collect(Collectors.joining("or", " and (", ")"));
        String sql4 = String.format(BASE_SQL, sourceCol, sourceTable, JOIN, targetTable, onCondition);
        builder.append(sql4).append(bothWhere).append(where4).append(";").append(LINE_END);

        log.debug("==> presto create sql by index: {}", builder.toString());
        return builder.toString();
    }

}
