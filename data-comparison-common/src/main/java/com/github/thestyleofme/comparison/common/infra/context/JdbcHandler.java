package com.github.thestyleofme.comparison.common.infra.context;

import static com.github.thestyleofme.plugin.core.infra.constants.BaseConstant.Symbol.*;

import java.io.LineNumberReader;
import java.io.StringReader;
import java.sql.*;
import java.util.*;

import com.github.thestyleofme.comparison.common.domain.PrestoInfo;
import com.github.thestyleofme.comparison.common.infra.exceptions.HandlerException;
import com.github.thestyleofme.driver.core.infra.utils.CloseUtil;
import com.github.thestyleofme.plugin.core.infra.constants.BaseConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * <p>
 * 通过 JDBC 操作 PRESTO
 * </p>
 *
 * @author isaac 2020/11/20 11:23
 * @since 1.0.0
 */
@Slf4j
@Component
public class JdbcHandler {

    private static final int DEFAULT_BATCH_SIZE = 1024;

    public void executeBatchUpdateSql(PrestoInfo prestoInfo, String text) {
        try (Connection connection = DatasourceProvider.getOrCreate(prestoInfo.getUsername(), prestoInfo.getCoordinatorUrl()).getConnection();
             Statement st = connection.createStatement()) {
            List<String> sqlList = sqlExtract2List(text);
            // 执行
            int count = 0;
            for (String sql : sqlList) {
                st.addBatch(sql);
                // 当循环达到指定次数后执行executeBatch()，将缓存中的sql全部发给数据库，然后执行clearBatch()清除缓存
                // ，否则数据过大是会出现OutOfMemory(内存不足)
                count++;
                if (count >= DEFAULT_BATCH_SIZE) {
                    st.executeBatch();
                    st.clearBatch();
                    count = 0;
                }
            }
            if (count != 0) {
                st.executeBatch();
            }
        } catch (SQLException e) {
            throw new HandlerException("hdsp.xadt.error.sql.execute", e);
        }
    }

    private List<String> sqlExtract2List(String text) {
        // 多条SQL拆分转换成一条
        String[] split = text.split(NEWLINE);
        List<String> sqlList = new ArrayList<>();
        StringBuilder sqlBuilder = new StringBuilder();
        Arrays.asList(split).forEach(line -> {
            line = line.trim();
            // 注释符开头认为是注释
            if (line.startsWith(TWO_MIDDLE_LINE)) {
                sqlBuilder.append(SPACE);
                // ;认为是结尾
            } else if (line.endsWith(SEMICOLON)) {
                sqlBuilder.append(line).append(NEWLINE);
                sqlList.add(sqlBuilder.toString());
                sqlBuilder.delete(0, sqlBuilder.length());
            } else if (line.contains(TWO_MIDDLE_LINE)) {
                sqlBuilder.append(line, 0, line.indexOf(TWO_MIDDLE_LINE));
            } else {
                sqlBuilder.append(line).append(SPACE);
            }
        });
        // 如果最后一条语句没有;结尾，补充
        if (!StringUtils.isEmpty(sqlBuilder)) {
            String r = new LineNumberReader(new StringReader(sqlBuilder.toString().trim()))
                    .lines()
                    .filter(line -> !line.trim().startsWith("--"))
                    .findFirst()
                    .orElse(BaseConstant.Symbol.EMPTY);
            if (!StringUtils.isEmpty(r)) {
                // 全为注释 noting to do 否则补充一条SQL
                sqlList.add(sqlBuilder.toString());
            }
        }
        return sqlList;
    }

    public List<List<Map<String, Object>>> executeBatchQuerySql(PrestoInfo prestoInfo, String text) {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        String nowSql = null;
        List<List<Map<String, Object>>> result = new ArrayList<>();
        List<String> sqlList = sqlExtract2List(text);
        try {
            connection = DatasourceProvider.getOrCreate(prestoInfo.getUsername(), prestoInfo.getCoordinatorUrl()).getConnection();
            statement = connection.createStatement();
            for (String sql : sqlList) {
                // 去掉sql后面的分号
                sql = sql.substring(0, sql.lastIndexOf(BaseConstant.Symbol.SEMICOLON));
                List<Map<String, Object>> rows = new ArrayList<>();
                nowSql = sql;
                resultSet = statement.executeQuery(nowSql);
                while (resultSet.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    this.transformMap(resultSet, row);
                    rows.add(row);
                }
                result.add(rows);
            }
            return result;
        } catch (SQLException e) {
            log.error("error now sql:[{}]", nowSql);
            throw new HandlerException("hdsp.xadt.error.presto.jdbc", e);
        } finally {
            CloseUtil.close(resultSet, statement, connection);
        }
    }

    private void transformMap(ResultSet resultSet, Map<String, Object> row) throws SQLException {
        int columnCount;
        ResultSetMetaData metaData = resultSet.getMetaData();
        columnCount = metaData.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            // 多表关联如果字段重复，可能会发生值覆盖问题
            String column = metaData.getColumnLabel(i);
            if (row.containsKey(column)) {
                column = String.format("%s(%d)", column, i);
            }
            row.put(column, this.getResultSetValue(resultSet, i));
        }
    }

    private Object getResultSetValue(ResultSet rs, int index) throws SQLException {
        Object obj = rs.getObject(index);
        String className;
        if (obj == null) {
            return "";
        }
        className = obj.getClass().getName();
        if (obj instanceof Blob) {
            Blob blob = (Blob) obj;
            obj = blob.getBytes(1, (int) blob.length());
        } else if (obj instanceof Clob) {
            Clob clob = (Clob) obj;
            obj = clob.getSubString(1, (int) clob.length());
        } else if ("oracle.sql.TIMESTAMP".equals(className) || "oracle.sql.TIMESTAMPTZ".equals(className)) {
            obj = rs.getTimestamp(index);
        } else if (className.startsWith("oracle.sql.DATE")) {
            String metaDataClassName = rs.getMetaData().getColumnClassName(index);
            if (Timestamp.class.getName().equals(metaDataClassName) || "oracle.sql.TIMESTAMP"
                    .equals(metaDataClassName)) {
                obj = rs.getTimestamp(index);
            } else {
                obj = rs.getDate(index);
            }
        } else if (obj instanceof java.sql.Date &&
                Timestamp.class.getName().equals(rs.getMetaData().getColumnClassName(index))) {
            obj = rs.getTimestamp(index);
        }
        return obj;
    }

}
