package com.github.thestyleofme.comparison.presto.handler.utils;

import static com.github.thestyleofme.comparison.common.infra.utils.SqlUtils.getBothWhereCondition;
import static com.github.thestyleofme.comparison.common.infra.utils.SqlUtils.getOneWhereCondition;
import static com.github.thestyleofme.comparison.presto.handler.constants.PrestoConstant.SqlConstant.*;

import java.util.List;
import java.util.stream.Collectors;

import com.github.thestyleofme.comparison.common.domain.ColMapping;
import com.github.thestyleofme.comparison.common.domain.SelectTableInfo;
import com.github.thestyleofme.comparison.presto.handler.pojo.PrestoInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;

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


    public static String generateAuditSql(PrestoInfo prestoInfo) {
        List<ColMapping> joinMappingList = prestoInfo.getIndexMapping();
        // 如果有指定主键或唯一索引
        if (CollectionUtils.isEmpty(joinMappingList)) {
            prestoInfo.setIndexMapping(prestoInfo.getColMapping());
        }
        return createSqlByPkOrIndex(prestoInfo);
    }

    public static String generatePreAuditSql(String table, String condition) {
        return String.format("select %s as _cdt from %s as _a;", condition, table);
    }

    private static String createSqlByPkOrIndex(PrestoInfo prestoInfo) {
        SelectTableInfo target = prestoInfo.getTarget();
        SelectTableInfo source = prestoInfo.getSource();
        String sourceTable = prestoInfo.getSourceTableName();
        String targetTable = prestoInfo.getTargetTableName();

        // 索引的列
        List<ColMapping> joinMappingList = prestoInfo.getIndexMapping();
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
        String bothWhere = getBothWhereCondition(source.getGlobalWhere(), source.getWhere(),
                target.getGlobalWhere(), target.getWhere());
        String sourceWhere = getOneWhereCondition(source.getGlobalWhere(), source.getWhere());
        String targetWhere = getOneWhereCondition(target.getGlobalWhere(), target.getWhere());
        /*
        1. AB都有的数据
        例：
        `SELECT count(*) as count FROM devmysql.test_28729.source_t1  as _a
        JOIN devmysql.test_28729.target_t1  as _b
        on  _a.id = _b.id1  and  _a.name = _b.name1
        where 1=1  and (0 = 0) and (1 = 1) and (_a.id<700)
        and (_b.id1<550)and ( _a.sex = _b.sex1 and _a.name = _b.name1 and _a.phone = _b.phone1 and _a.address = _b.address1
        and _a.education = _b.education1 and _a.state = _b.state1 );`
        */

        String where1 = colList.stream()
                .map(col -> String.format(" _a.%s = _b.%s ", col.getSourceCol(), col.getTargetCol()))
                .collect(Collectors.joining("and", "and (", ")"));
        String sql1 = String.format(BASE_SQL, "count(*) as count", sourceTable, JOIN, targetTable, onCondition);
        builder.append(sql1).append(bothWhere).append(where1).append(";").append(LINE_END);
        /*
         2. A有B无
         `SELECT _a.id,_a.sex,_a.name,_a.phone,_a.address,_a.education,_a.state
         FROM devmysql.test_28729.source_t1  as _a LEFT JOIN devmysql.test_28729.target_t1  as _b
         on  _a.id = _b.id1  and  _a.name = _b.name1  where 1=1  and (0 = 0) and (_a.id<700)
         and ( _b.id1 is null and _b.name1 is null );`
         */
        //"SELECT %s FROM %s as _a %s %s as _b on %s "
        String sourceCol = colMappingList.stream()
                .map(colMapping -> String.format("_a.%s", colMapping.getSourceCol()))
                .collect(Collectors.joining(","));
        String where2 = joinMappingList.stream()
                .map(col -> String.format(" _b.%s is null ", col.getTargetCol()))
                .collect(Collectors.joining(" and ", " and (", ")"));
        String sql2 = String.format(BASE_SQL, sourceCol, sourceTable, LEFT_JOIN, targetTable, onCondition);
        builder.append(sql2).append(sourceWhere).append(where2).append(";").append(LINE_END);
        /*
         3. B有A无
         `SELECT _b.id1,_b.sex1,_b.name1,_b.phone1,_b.address1,_b.education1,_b.state1
         FROM devmysql.test_28729.source_t1  as _a RIGHT JOIN devmysql.test_28729.target_t1  as _b
         on  _a.id = _b.id1  and  _a.name = _b.name1  where 1=1  and (1 = 1) and (_b.id1<550)
         and ( _a.id is null or _a.name is null );`
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
         `SELECT _a.id,_a.sex,_a.name,_a.phone,_a.address,_a.education,_a.state
         FROM devmysql.test_28729.source_t1  as _a JOIN devmysql.test_28729.target_t1  as _b
         on  _a.id = _b.id1  and  _a.name = _b.name1  where 1=1  and (0 = 0) and (1 = 1)
         and (_a.id<700) and (_b.id1<550) and ( _a.sex != _b.sex1 or _a.name != _b.name1 or _a.phone != _b.phone1
         or _a.address != _b.address1 or _a.education != _b.education1 or _a.state != _b.state1 );`
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
