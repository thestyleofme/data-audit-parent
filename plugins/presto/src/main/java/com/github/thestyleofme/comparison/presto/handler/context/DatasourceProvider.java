package com.github.thestyleofme.comparison.presto.handler.context;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import javax.sql.DataSource;

import com.github.thestyleofme.comparison.common.infra.constants.CommonConstant;
import com.github.thestyleofme.comparison.common.infra.exceptions.HandlerException;
import com.github.thestyleofme.comparison.presto.handler.utils.PrestoUtils;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.springframework.lang.NonNull;
import org.springframework.util.DigestUtils;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/20 10:19
 * @since 1.0.0
 */
@Slf4j
public class DatasourceProvider {

    private DatasourceProvider() {
    }

    private static final Map<String, ImmutablePair<LocalDateTime, DataSource>> DATASOURCE_MAP = new ConcurrentHashMap<>();
    private static final String JDBC_URL_FT = "jdbc:presto://%s:%s";
    private static final long DEFAULT_CACHE_HOUR = 2;

    static {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("deployExecutor-%d").build();
        // 每隔30分钟清理一次超过2小时未使用的数据源
        new ScheduledThreadPoolExecutor(1, threadFactory)
                .scheduleAtFixedRate(() -> DATASOURCE_MAP.forEach((k, pair) -> {
                    if (Duration.between(pair.getLeft(), LocalDateTime.now()).toHours() > DEFAULT_CACHE_HOUR) {
                        DATASOURCE_MAP.remove(k);
                    }
                }), 10, 1800, TimeUnit.SECONDS);
    }

    public static DataSource getOrCreate(String username, @NonNull String coordinatorUrl) {
        username = Optional.ofNullable(username).orElse(CommonConstant.DEFAULT_PRESTO_USERNAME);
        String key = DigestUtils.md5DigestAsHex(String.format("%s_%s", username, coordinatorUrl)
                .getBytes(StandardCharsets.UTF_8));
        // 从缓存中拿
        ImmutablePair<LocalDateTime, DataSource> immutablePair = DATASOURCE_MAP.get(key);
        if (Objects.nonNull(immutablePair)) {
            return immutablePair.getRight();
        }
        // 创建
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setUsername(username);
        hikariConfig.setJdbcUrl(genPrestoJdbc(coordinatorUrl));
        HikariDataSource instance = new HikariDataSource(hikariConfig);
        DATASOURCE_MAP.put(key, ImmutablePair.of(LocalDateTime.now(), instance));
        return instance;
    }

    private static String genPrestoJdbc(String url) {
        Matcher matcher = PrestoUtils.HTTP_PATTERN.matcher(url);
        if (matcher.matches()) {
            String ip = matcher.group(1);
            String port = matcher.group(2);
            return String.format(JDBC_URL_FT, ip, port);
        }
        throw new HandlerException("hdsp.xadt.error.presto.analysis_url");
    }
}
