package com.github.thestyleofme.data.comparison.app.service.impl;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import com.github.thestyleofme.data.comparison.app.service.DemoService;
import com.github.thestyleofme.data.comparison.domain.entity.Bloom;
import com.github.thestyleofme.data.comparison.infra.utils.JsonUtil;
import com.github.thestyleofme.data.comparison.infra.utils.Md5Util;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/19 11:09
 * @since 1.0.0
 */
@Service
@Slf4j
public class DemoServiceImpl implements DemoService {

    private final JdbcTemplate jdbcTemplate;
    private final RedisTemplate<String, String> redisTemplate;

    public DemoServiceImpl(JdbcTemplate jdbcTemplate,
                           RedisTemplate<String, String> redisTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.redisTemplate = redisTemplate;
    }

    @Override
    public void demo() {
        LocalDateTime startTime = LocalDateTime.now();
        // 定义bloom filter 数据源source数据设置1
        Bloom bloom = createBloom();
        if (Objects.isNull(bloom)) {
            return;
        }
        int seed = ThreadLocalRandom.current().nextInt(bloom.getBitSize());
        // 校验redis key
        String redisKey = "demo";
        if (Boolean.TRUE.equals(redisTemplate.hasKey(redisKey))) {
            // throw new IllegalStateException("redis key already exists");
            // 仅做测试，这里直接删除即可，生成需抛异常
            redisTemplate.delete(redisKey);
        }
        handleSourceDatasource(redisKey, bloom, seed);
        // 数据源target数据 判断是否存在
        String pk = "id";
        handleTargetDatasource(redisKey, bloom, pk, seed);
        LocalDateTime endTime = LocalDateTime.now();
        log.debug("just test, time cost : {}", Duration.between(endTime, startTime));
    }

    private void handleTargetDatasource(String redisKey, Bloom bloom, String pk, int seed) {
        List<Map<String, Object>> list = jdbcTemplate.queryForList("select * from resume_bak");
        Map<String, Object> sortMap;
        for (Map<String, Object> map : list) {
            sortMap = new TreeMap<>(Comparator.reverseOrder());
            sortMap.putAll(map);
            String md5Str = Md5Util.getUppercaseMd5(JsonUtil.toJson(sortMap));
            List<Integer> hashList = bloom.doHash(md5Str, seed);
            // 只要有一个hash后在redis中找不到 即肯定不存在
            boolean exists = hashList.stream()
                    .map(hash -> redisTemplate.opsForValue().getBit(redisKey, hash))
                    .anyMatch(Boolean.FALSE::equals);
            if (Boolean.TRUE.equals(exists)) {
                // 肯定不存在 需判断主键是否一样
                handlerTargetNotExits(pk, map);
            } else {
                // 只能说可能存在 不能百分百保证 尽可能降低误判率
                log.debug("maybe exists: {}", map);
            }
        }
    }

    private void handlerTargetNotExits(String pk, Map<String, Object> map) {
        Object pkValue = map.get(pk);
        // 判断pk在源端是否存在
        if (StringUtils.isEmpty(pkValue)) {
            // 主键不存在
            log.debug("not exists, pk not exists: {}", map);
        } else {
            // 主键存在 其他字段不一样
            log.debug("not exists, but pk exists: {}", map);
        }
    }

    private void handleSourceDatasource(String redisKey, Bloom bloom, int seed) {
        List<Map<String, Object>> list = jdbcTemplate.queryForList("select * from resume");
        Map<String, Object> sortMap;
        for (Map<String, Object> map : list) {
            sortMap = new TreeMap<>(Comparator.reverseOrder());
            sortMap.putAll(map);
            String md5Str = Md5Util.getUppercaseMd5(JsonUtil.toJson(sortMap));
            List<Integer> hashList = bloom.doHash(md5Str, seed);
            hashList.forEach(hash ->
                    // 设置对应位置为1
                    redisTemplate.opsForValue().setBit(redisKey, hash, true)
            );
        }
    }

    private Bloom createBloom() {
        Long count = jdbcTemplate.queryForObject("select count(*) from resume", Long.class);
        if (count == null || count == 0) {
            return null;
        }
        int bound = Math.toIntExact(count);
        return new Bloom(bound);
    }

}
