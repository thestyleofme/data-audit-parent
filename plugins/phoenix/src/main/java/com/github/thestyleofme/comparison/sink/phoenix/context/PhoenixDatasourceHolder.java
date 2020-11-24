package com.github.thestyleofme.comparison.sink.phoenix.context;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;

import com.github.thestyleofme.comparison.sink.phoenix.pojo.DatasourceInfo;
import com.github.thestyleofme.plugin.core.infra.utils.JsonUtil;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.springframework.util.DigestUtils;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/23 16:59
 * @since 1.0.0
 */
public class PhoenixDatasourceHolder {

    private PhoenixDatasourceHolder() {
    }

    private static final Map<String, ImmutablePair<LocalDateTime, DataSource>> DATASOURCE_MAP = new ConcurrentHashMap<>();
    private static final long DEFAULT_CACHE_HOUR = 2;

    static {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("phoenixExecutor-%d").build();
        // 每隔30分钟清理一次超过2小时未使用的数据源
        new ScheduledThreadPoolExecutor(1, threadFactory)
                .scheduleAtFixedRate(() -> DATASOURCE_MAP.forEach((k, pair) -> {
                    if (Duration.between(pair.getLeft(), LocalDateTime.now()).toHours() > DEFAULT_CACHE_HOUR) {
                        DATASOURCE_MAP.remove(k);
                    }
                }), 10, 1800, TimeUnit.SECONDS);
    }

    public static DataSource getOrCreate(DatasourceInfo datasourceInfo) {
        String key = DigestUtils.md5DigestAsHex(JsonUtil.toJson(datasourceInfo).getBytes(StandardCharsets.UTF_8));
        // 从缓存中拿
        ImmutablePair<LocalDateTime, DataSource> immutablePair = DATASOURCE_MAP.get(key);
        if (Objects.nonNull(immutablePair)) {
            return immutablePair.getRight();
        }
        // 创建
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(datasourceInfo.getJdbcUrl());
        hikariConfig.setDriverClassName(datasourceInfo.getDriverClassName());
        HikariDataSource instance = new HikariDataSource(hikariConfig);
        DATASOURCE_MAP.put(key, ImmutablePair.of(LocalDateTime.now(), instance));
        return instance;
    }

}
