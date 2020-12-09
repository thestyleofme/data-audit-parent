package com.github.thestyleofme.comparison.presto.handler.service;

import java.util.List;
import java.util.Map;

import com.github.thestyleofme.comparison.common.infra.constants.ErrorCode;
import com.github.thestyleofme.comparison.common.infra.exceptions.HandlerException;
import com.github.thestyleofme.comparison.presto.handler.context.JdbcHandler;
import com.github.thestyleofme.comparison.presto.handler.pojo.PrestoInfo;
import com.github.thestyleofme.driver.core.app.service.DriverSessionService;
import com.github.thestyleofme.driver.core.app.service.session.DriverSession;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * <p></p>
 *
 * @author hsq 2020/11/30 21:07
 * @since 1.0.0
 */
@Service
@Slf4j
public class PrestoExecutor {

    private final JdbcHandler jdbcHandler;
    private final DriverSessionService driverSessionService;

    public PrestoExecutor(JdbcHandler jdbcHandler, DriverSessionService driverSessionService) {
        this.jdbcHandler = jdbcHandler;
        this.driverSessionService = driverSessionService;
    }

    public List<List<Map<String, Object>>> executeSql(Long tenantId, PrestoInfo prestoInfo, String sql) {
        if (!StringUtils.isEmpty(prestoInfo.getDataSourceCode())) {
            // 走数据源
            return handleByDataSourceCode(tenantId, prestoInfo, sql);
        } else {
            // 走jdbc
            return handleByJdbc(prestoInfo, sql);
        }

    }

    private List<List<Map<String, Object>>> handleByJdbc(PrestoInfo prestoInfo, String sql) {
        // 执行sql
        return jdbcHandler.executeBatchQuerySql(prestoInfo, sql);
    }

    private List<List<Map<String, Object>>> handleByDataSourceCode(Long tenantId, PrestoInfo prestoInfo, String sql) {
        // 执行sql
        DriverSession driverSession = driverSessionService.getDriverSession(tenantId, prestoInfo.getDataSourceCode());
        List<List<Map<String, Object>>> result = driverSession.executeAll(null, sql, true);
        if (CollectionUtils.isEmpty(result)) {
            throw new HandlerException(ErrorCode.PRESTO_RESULT_NOT_FOUND);
        }
        return result;
    }


}
