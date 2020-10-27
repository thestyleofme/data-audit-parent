package com.github.thestyleofme.data.comparison.infra.handler;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.github.thestyleofme.data.comparison.domain.entity.Bloom;
import com.github.thestyleofme.data.comparison.domain.entity.ComparisonJob;
import com.github.thestyleofme.data.comparison.infra.annotation.EngineType;
import com.github.thestyleofme.data.comparison.infra.constants.CommonConstant;
import com.github.thestyleofme.data.comparison.infra.context.JobHandlerContext;
import com.github.thestyleofme.data.comparison.infra.exceptions.HandlerException;
import com.github.thestyleofme.data.comparison.infra.handler.comparison.BaseComparisonHandler;
import com.github.thestyleofme.data.comparison.infra.handler.comparison.ComparisonMapping;
import com.github.thestyleofme.data.comparison.infra.utils.JsonUtil;
import com.github.thestyleofme.data.comparison.infra.utils.Md5Util;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/22 15:33
 * @since 1.0.0
 */
@EngineType("REDIS_BLOOM_FILTER")
@Component
@Slf4j
public class RedisBloomFilterJobHandler implements BaseJobHandler {

    private final JobHandlerContext jobHandlerContext;
    private final RedisTemplate<String, String> redisTemplate;

    public RedisBloomFilterJobHandler(JobHandlerContext jobHandlerContext,
                                      RedisTemplate<String, String> redisTemplate) {
        this.jobHandlerContext = jobHandlerContext;
        this.redisTemplate = redisTemplate;
    }

    @Override
    public HandlerResult handle(ComparisonJob comparisonJob) {
        LocalDateTime startTime = LocalDateTime.now();
        log.debug("use redis bloom filter to handle this job");
        BaseComparisonHandler comparisonHandler = jobHandlerContext.getComparisonHandler(comparisonJob.getComparisonType());
        ComparisonMapping comparisonMapping = comparisonHandler.handle(comparisonJob);
        Bloom bloom = createBloom(comparisonMapping);
        if (Objects.isNull(bloom)) {
            return null;
        }
        int seed = ThreadLocalRandom.current().nextInt(bloom.getBitSize());
        String redisKey = String.format(CommonConstant.RedisKey.JOB_FORMAT, comparisonJob.getTenantId(), comparisonJob.getJobName());
        if (Boolean.TRUE.equals(redisTemplate.hasKey(redisKey))) {
            throw new HandlerException("redis key[%s] already exists", redisKey);
        }
        genRedisBloomFilter(comparisonJob, comparisonMapping, bloom, seed, redisKey);
        HandlerResult handlerResult = handleComparison(comparisonJob, comparisonMapping, bloom, seed, redisKey);
        LocalDateTime endTime = LocalDateTime.now();
        log.debug("just test, time cost : {}", Duration.between(endTime, startTime));
        return handlerResult;
    }

    private HandlerResult handleComparison(ComparisonJob comparisonJob,
                                           ComparisonMapping comparisonMapping,
                                           Bloom bloom,
                                           int seed,
                                           String redisKey) {
        HandlerResult handlerResult = new HandlerResult();
        List<LinkedHashMap<String, Object>> sourceDataList = comparisonMapping.getSourceDataList();
        for (LinkedHashMap<String, Object> map : sourceDataList) {
            String md5Str = Md5Util.getUppercaseMd5(JsonUtil.toJson(map.values()));
            List<Integer> hashList = bloom.doHash(md5Str, seed);
            // 只要有一个hash后在redis中找不到 即肯定不存在
            boolean exists = hashList.stream()
                    .map(hash -> redisTemplate.opsForValue().getBit(redisKey, hash))
                    .anyMatch(Boolean.FALSE::equals);
            if (Boolean.TRUE.equals(exists)) {
                // 肯定不存在 需判断主键或唯一索引是否一样
                handlerNotExits(handlerResult, comparisonJob, map, comparisonMapping);
            } else {
                // 只能说可能存在 不能百分百保证 尽可能降低误判率
                handlerResult.getSameDataList().add(map);
            }
        }
        return handlerResult;
    }

    private void handlerNotExits(HandlerResult handlerResult,
                                 ComparisonJob comparisonJob,
                                 LinkedHashMap<String, Object> map,
                                 ComparisonMapping comparisonMapping) {
        String sourcePk = comparisonJob.getSourcePk();
        String pkRedisKey = String.format(CommonConstant.RedisKey.TARGET_PK,
                comparisonJob.getTenantId(), comparisonJob.getJobName());
        Object pkValue = map.get(sourcePk);
        Boolean isMember = redisTemplate.opsForSet().isMember(pkRedisKey, String.valueOf(pkValue));
        if (Boolean.FALSE.equals(isMember)) {
            // 主键不存在 还需判断唯一索引
            String sourceIndex = comparisonJob.getSourceIndex();
            if (StringUtils.isEmpty(sourceIndex)) {
                // source的数据在target中不存在
                handlerResult.getSourceUniqueDataList().add(map);
                return;
            }
            String[] split = sourceIndex.split(",");
            long count = Stream.of(split)
                    .filter(v -> Objects.nonNull(map.get(v)))
                    .count();
            if (count == split.length) {
                String indexRedisKey = String.format(CommonConstant.RedisKey.TARGET_INDEX,
                        comparisonJob.getTenantId(), comparisonJob.getJobName());
                String value = Stream.of(split).map(s -> (String) map.get(s)).collect(Collectors.joining(","));
                Boolean exists = redisTemplate.opsForSet().isMember(indexRedisKey, value);
                if (Boolean.TRUE.equals(exists)) {
                    // 唯一index存在，其他字段不一样 存target的数据
                    comparisonMapping.getTargetDataList().stream()
                            .filter(targetMap -> Stream.of(split).allMatch(v -> map.get(v).equals(targetMap.get(v))))
                            .findFirst()
                            .ifPresent(result -> handlerResult.getPkOrIndexSameDataList().add(result));
                } else {
                    // source的数据在target中不存在
                    handlerResult.getSourceUniqueDataList().add(map);
                }
            } else {
                // source的数据在target中不存在
                handlerResult.getSourceUniqueDataList().add(map);
            }
        } else {
            // 主键存在 其他字段不一样 存target的数据
            comparisonMapping.getTargetDataList().stream()
                    .filter(targetMap -> targetMap.get(comparisonJob.getTargetPk()).equals(pkValue))
                    .findFirst()
                    .ifPresent(result -> handlerResult.getPkOrIndexSameDataList().add(result));
        }
    }

    private void genRedisBloomFilter(ComparisonJob comparisonJob,
                                     ComparisonMapping comparisonMapping,
                                     Bloom bloom,
                                     int seed,
                                     String redisKey) {
        // 将target的数据set bit 就能准确找到source到target 未成功同步的数据
        List<LinkedHashMap<String, Object>> list = comparisonMapping.getTargetDataList();
        if (CollectionUtils.isEmpty(list)) {
            return;
        }
        for (LinkedHashMap<String, Object> map : list) {
            String md5Str = Md5Util.getUppercaseMd5(JsonUtil.toJson(map.values()));
            List<Integer> hashList = bloom.doHash(md5Str, seed);
            // 设置对应位置为1
            hashList.forEach(hash -> redisTemplate.opsForValue().setBit(redisKey, hash, true));
            // redis 存储目标表的pk或索引
            handlerPkOrIndex(comparisonJob, redisKey, map);
        }
    }

    private void handlerPkOrIndex(ComparisonJob comparisonJob,
                                  String redisKey,
                                  LinkedHashMap<String, Object> map) {
        String targetPk = comparisonJob.getTargetPk();
        if (StringUtils.isEmpty(targetPk)) {
            // 主键不存在 还需判断唯一索引
            String targetIndex = comparisonJob.getTargetIndex();
            if (StringUtils.isEmpty(targetIndex)) {
                return;
            }
            String[] split = targetIndex.split(",");
            long count = Stream.of(split)
                    .filter(v -> Objects.nonNull(map.get(v)))
                    .count();
            if (count == split.length) {
                log.debug("not exists, pk unique index exists: {}", map);
                // 有index
                String value = Stream.of(split).map(s -> (String) map.get(s)).collect(Collectors.joining(","));
                redisTemplate.opsForSet().add(redisKey + CommonConstant.RedisKey.TARGET_INDEX_SUFFIX, value);
            }
        } else {
            // 有pk
            Object pkValue = map.get(targetPk);
            if (Objects.isNull(pkValue)) {
                return;
            }
            redisTemplate.opsForSet().add(redisKey + CommonConstant.RedisKey.TARGET_PK_SUFFIX, String.valueOf(pkValue));
        }
    }


    private Bloom createBloom(ComparisonMapping comparisonMapping) {
        if (Objects.isNull(comparisonMapping)) {
            return null;
        }
        List<LinkedHashMap<String, Object>> list = comparisonMapping.getTargetDataList();
        if (CollectionUtils.isEmpty(list)) {
            return null;
        }
        int bound = Math.toIntExact(list.size());
        return new Bloom(bound);
    }

}
