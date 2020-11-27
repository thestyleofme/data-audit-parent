package com.github.thestyleofme.comparison.presto.handler.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.baomidou.mybatisplus.core.toolkit.StringPool;
import com.github.thestyleofme.comparison.common.domain.ColMapping;
import com.github.thestyleofme.comparison.common.domain.JobEnv;
import com.github.thestyleofme.comparison.common.infra.exceptions.HandlerException;
import com.github.thestyleofme.comparison.presto.handler.constants.PrestoConstant;
import com.github.thestyleofme.comparison.presto.handler.pojo.PrestoInfo;
import com.github.thestyleofme.plugin.core.infra.utils.BeanUtils;
import lombok.extern.slf4j.Slf4j;
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

    public static String generateSql(PrestoInfo prestoInfo, JobEnv jobEnv) {
        String sql;
        String sourcePk = jobEnv.getSourcePk();
        String targetPk = jobEnv.getTargetPk();
        String sourceIndex = jobEnv.getSourceIndex();
        String targetIndex = jobEnv.getTargetIndex();
        // 如果有指定主键
        if (!StringUtils.isEmpty(sourcePk) && !StringUtils.isEmpty(targetPk)) {
            sql = createSqlByPk(sourcePk, targetPk, prestoInfo, jobEnv);
        } else if (!StringUtils.isEmpty(sourceIndex) && !StringUtils.isEmpty(targetIndex)) {
            sql = createSqlByIndex(sourceIndex, targetIndex, prestoInfo, jobEnv);
        } else {
            throw new HandlerException("hdsp.xadt.error.presto.not_support");
        }
        return sql;
    }

    private static String createSqlByPk(String sourcePk,
                                        String targetPk,
                                        PrestoInfo prestoInfo,
                                        JobEnv jobEnv) {
        StringBuilder builder = new StringBuilder();
        String sourceTable = getSourceTable(prestoInfo);
        String targetTable = getTargetTable(prestoInfo);
        List<ColMapping> colMappingList = getColMappingList(jobEnv);
        // 获取除去主键外的所有列
        List<ColMapping> colList = colMappingList.stream()
                .filter(col -> !sourcePk.equalsIgnoreCase(col.getSourceCol()) || !targetPk.equalsIgnoreCase(col.getTargetCol()))
                .collect(Collectors.toList());
        /*
        1. AB都有的数据
        例：
        `select _a.* from mysql.hdsp_test.resume as _a join mysql.hdsp_test.resume_bak  as _b
        ON _a.id = _b.id WHERE _a.name = _b.name and _a.sex = _b.sex and _a.phone = _b
        .call and _a.address = _b.address and _a.education = _b.education and _a.state = _b.state;`
        */
        String equalWhere = colList.stream()
                .map(mapping -> String.format("_a.%s = _b.%s", mapping.getSourceCol(), mapping.getTargetCol()))
                .collect(Collectors.joining(" and "));
        builder.append(String.format(PrestoConstant.SqlConstant.JOIN_SQL_PK, sourceTable, targetTable,
                sourcePk, targetPk, equalWhere))
                .append(PrestoConstant.SqlConstant.LINE_END);
        /*
         2. A有B无
         `select _a.* from mysql.hdsp_test.resume  as _a left join mysql.hdsp_test.resume_bak  as _b ON _a.id = _b.id WHERE _b.id is null;`
         */
        createUniqueDataSqlByPk(builder, sourceTable, targetTable, sourcePk, targetPk);
        /*
         3. A无B有
          `select _a.* from mysql.hdsp_test.resume_bak  as _a left join mysql.hdsp_test.resume  as _b ON _a.id = _b.id WHERE _b.id is null  ;`
         */
        createUniqueDataSqlByPk(builder, targetTable, sourceTable, targetPk, sourcePk);

        /*
          4. AB主键或唯一索引相同，部分字段不一样
          `select _a.* from mysql.hdsp_test.resume  as _a left join mysql.hdsp_test.resume_bak  as _b
          ON _a.id = _b.id
          WHERE _a.name != _b.name or _a.sex != _b.sex or _a.phone != _b.call or _a.address != _b.address
          or _a.education != _b.education or _a.state != _b.state or 1=2  ;`
         */
        String notEqualWhere = colList.stream()
                .map(mapping -> String.format("_a.%s != _b.%s", mapping.getSourceCol(), mapping.getTargetCol()))
                .collect(Collectors.joining(" or "));
        builder.append(String.format(PrestoConstant.SqlConstant.JOIN_SQL_PK, sourceTable, targetTable, sourcePk, targetPk,
                notEqualWhere))
                .append(PrestoConstant.SqlConstant.LINE_END);

        log.debug("==> presto create sql by primary key: {}", builder.toString());
        return builder.toString();
    }

    public static void createUniqueDataSqlByPk(StringBuilder builder, String tableA, String tableB, String pkA, String pkB) {
        builder.append(String.format(PrestoConstant.SqlConstant.LEFT_HAVE_SQL_PK, tableA, tableB, pkA, pkB, pkB))
                .append(PrestoConstant.SqlConstant.LINE_END);
    }

    private static List<ColMapping> getColMappingList(JobEnv jobEnv) {
        // 获取列的映射关系
        List<Map<String, Object>> colMapping = jobEnv.getColMapping();
        return colMapping.stream()
                .map(map -> BeanUtils.map2Bean(map, ColMapping.class))
                .collect(Collectors.toList());
    }

    private static String getSourceTable(PrestoInfo prestoInfo) {
        return String.format(PrestoConstant.SqlConstant.TABLE_FT, prestoInfo.getSourcePrestoCatalog(),
                prestoInfo.getSourceSchema(), prestoInfo.getSourceTable());
    }

    private static String getTargetTable(PrestoInfo prestoInfo) {
        return String.format(PrestoConstant.SqlConstant.TABLE_FT, prestoInfo.getTargetPrestoCatalog(),
                prestoInfo.getTargetSchema(), prestoInfo.getTargetTable());
    }

    private static String createSqlByIndex(String sourceIndex,
                                           String targetIndex,
                                           PrestoInfo prestoInfo,
                                           JobEnv jobEnv) {
        // 获取索引的列
        List<String> sourceIndexList = new ArrayList<>(Arrays.asList(sourceIndex.split(StringPool.COMMA)));
        List<String> targetIndexList = new ArrayList<>(Arrays.asList(targetIndex.split(StringPool.COMMA)));
        if (sourceIndexList.size() != targetIndexList.size()) {
            throw new HandlerException("hdsp.xadt.error.presto.index_length.not_equals");
        }
        StringBuilder builder = new StringBuilder();
        String sourceTable = getSourceTable(prestoInfo);
        String targetTable = getTargetTable(prestoInfo);
        // 获取列的映射关系
        List<ColMapping> colMappingList = getColMappingList(jobEnv);
        // 获取除去索引外的所有列
        List<ColMapping> colList = colMappingList.stream()
                .filter(col -> !sourceIndexList.contains(col.getSourceCol()) || !targetIndexList.contains(col.getTargetCol()))
                .collect(Collectors.toList());

        // 创建on语句 AB型
        ArrayList<ColMapping> temp = new ArrayList<>(colMappingList);
        temp.removeAll(colList);
        String onCondition1 = temp.stream()
                .map(col -> String.format("_a.%s = _b.%s", col.getSourceCol(), col.getTargetCol()))
                .collect(Collectors.joining(" and "));
        // 创建on语句 BA型
        String onCondition2 = temp.stream()
                .map(col -> String.format("_a.%s = _b.%s", col.getTargetCol(), col.getSourceCol()))
                .collect(Collectors.joining(" and "));
        /*
        AB都有的数据
        例：
        `select _a.* from mysql.hdsp_test.resume as _a join mysql.hdsp_test.resume_bak  as _b
        ON _a.name = _b.name and _a.phone = _b.call WHERE _a.id = _b.id and _a.sex = _b.sex and _a.address = _b.address
        and _a.education = _b.education and _a.state = _b.state;`
        */
        String equalWhere = colList.stream()
                .map(col -> String.format("_a.%s = _b.%s", col.getSourceCol(), col.getTargetCol()))
                .collect(Collectors.joining(" and "));
        builder.append(String.format(PrestoConstant.SqlConstant.ALL_HAVE_SQL_INDEX, sourceTable, targetTable, onCondition1, equalWhere))
                .append(PrestoConstant.SqlConstant.LINE_END);
        /*
         A有B无
         `select _a.* from mysql.hdsp_test.resume as _a left join mysql.hdsp_test.resume_bak  as _b ON _a.name = _b.name and _a.phone = _b.call where _b.name is null and _b.call is null;`
         */
        String targetColIsNull = targetIndexList.stream()
                .map(col -> String.format("_b.%s is null", col))
                .collect(Collectors.joining(" and "));
        builder.append(String.format(PrestoConstant.SqlConstant.LEFT_HAVE_SQL_INDEX, sourceTable, targetTable, onCondition1,
                targetColIsNull))
                .append(PrestoConstant.SqlConstant.LINE_END);
        /*
         B有A无
         `select _a.* from mysql.hdsp_test.resume_bak as _a left join mysql.hdsp_test.resume  as _b ON _a.name = _b.name and _a.call = _b.phone where _b.name is null and _b.phone is null;`
         */
        String sourceColIsNull = sourceIndexList.stream()
                .map(col -> String.format("_b.%s is null", col))
                .collect(Collectors.joining(" and "));
        builder.append(String.format(PrestoConstant.SqlConstant.LEFT_HAVE_SQL_INDEX, targetTable, sourceTable, onCondition2,
                sourceColIsNull))
                .append(PrestoConstant.SqlConstant.LINE_END);
        /*
          AB唯一索引相同，部分字段不一样
          `select _a.* from mysql.hdsp_test.resume as _a left join mysql.hdsp_test.resume_bak  as _b
          ON _a.name = _b.name and _a.phone = _b.call
          WHERE _a.id != _b.id or _a.sex != _b.sex or _a.address != _b.address
          or _a.education != _b.education or _a.state != _b.state ;`
         */
        String notEqualWhere = colList.stream()
                .map(col -> String.format("_a.%s != _b.%s", col.getSourceCol(), col.getTargetCol()))
                .collect(Collectors.joining(" or "));
        builder.append(String.format(PrestoConstant.SqlConstant.ANY_NOT_IN_SQL_INDEX, sourceTable, targetTable, onCondition1,
                notEqualWhere))
                .append(PrestoConstant.SqlConstant.LINE_END);
        log.debug("==> presto create sql by index: {}", builder.toString());
        return builder.toString();
    }

}
