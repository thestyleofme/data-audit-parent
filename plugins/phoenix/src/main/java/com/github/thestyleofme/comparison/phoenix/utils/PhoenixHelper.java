package com.github.thestyleofme.comparison.phoenix.utils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.sql.DataSource;

import com.github.thestyleofme.comparison.common.infra.constants.ErrorCode;
import com.github.thestyleofme.comparison.common.infra.exceptions.HandlerException;
import com.github.thestyleofme.comparison.phoenix.constant.PhoenixConstant;
import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/24 13:53
 * @since 1.0.0
 */
@Slf4j
public class PhoenixHelper {

    private PhoenixHelper() {
    }

    /**
     * 准备工作
     *
     * @param dataSource DataSource
     */
    public static void prepare(DataSource dataSource) {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            // 创库
            statement.execute(PhoenixConstant.CREATE_SCHEMA_SQL);
            // 创表
            statement.execute(PhoenixConstant.CREATE_TABLE_SQL);
            // 创序列作为表的主键
            statement.execute(PhoenixConstant.CREATE_SEQUENCE_SQL);
        } catch (SQLException e) {
            log.error("hdsp.xadt.err.phoenix.create.schema.table.sequence", e);
            throw new HandlerException(ErrorCode.PHOENIX_CREATE_ERROR);
        }
    }

    public static void execute(DataSource dataSource, List<String> list) {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            for (String s : list) {
                statement.addBatch(s);
            }
            statement.executeBatch();
            statement.clearBatch();
        } catch (SQLException e) {
            log.error("hdsp.xadt.err.phoenix.execute.sql", e);
            throw new HandlerException(ErrorCode.PHOENIX_EXECUTE_ERROR);
        }
    }

    public static CompletableFuture<Boolean> executeAsync(DataSource dataSource, List<String> list) {
        return CompletableFuture.supplyAsync(() -> {
            execute(dataSource, list);
            return true;
        });
    }

}
