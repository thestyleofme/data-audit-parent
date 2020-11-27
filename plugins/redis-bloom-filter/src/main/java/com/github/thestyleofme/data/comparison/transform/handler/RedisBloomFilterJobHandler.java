package com.github.thestyleofme.data.comparison.transform.handler;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.github.thestyleofme.comparison.common.app.service.transform.BaseTransformHandler;
import com.github.thestyleofme.comparison.common.app.service.transform.HandlerResult;
import com.github.thestyleofme.comparison.common.app.service.transform.TableDataHandler;
import com.github.thestyleofme.comparison.common.domain.JobEnv;
import com.github.thestyleofme.comparison.common.domain.SourceDataMapping;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.infra.annotation.TransformType;
import com.github.thestyleofme.comparison.common.infra.utils.CommonUtil;
import com.github.thestyleofme.comparison.common.infra.utils.Md5Util;
import com.github.thestyleofme.comparison.common.infra.utils.ThreadPoolUtil;
import com.github.thestyleofme.data.comparison.transform.constants.RedisBloomConstant;
import com.github.thestyleofme.data.comparison.transform.pojo.Bloom;
import com.github.thestyleofme.data.comparison.transform.pojo.BloomTransformInfo;
import com.github.thestyleofme.plugin.core.infra.utils.BeanUtils;
import com.github.thestyleofme.plugin.core.infra.utils.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * <p>
 * 有误差，无法保证正确性，不可基于此去做数据补偿
 * <p>
 * table A -> table B
 * 以B为基准，遍历A，只找出A在B中不存在的数据以及相同数据 但是有误差 可以调小errorRate,默认0.001
 * 无法找出B有A无的
 * <p>
 * 场景可能是，当表数据量特别大时，可生成布隆过滤器，做一些数据判断是否存在
 * <p>
 * 有点<strong>鸡肋</strong> 计算时间长还不准确
 * 推荐使用presto，运行快且准
 * </p>
 *
 * @author isaac 2020/10/22 15:33
 * @since 1.0.0
 */
@TransformType(value = "REDIS_BLOOM_FILTER")
@Component
@Slf4j
public class RedisBloomFilterJobHandler implements BaseTransformHandler {

    private final RedisTemplate<String, String> redisTemplate;
    private final TableDataHandler tableDataHandler;
    private final ExecutorService executorService = ThreadPoolUtil.getExecutorService();

    public RedisBloomFilterJobHandler(RedisTemplate<String, String> redisTemplate,
                                      TableDataHandler tableDataHandler) {
        this.redisTemplate = redisTemplate;
        this.tableDataHandler = tableDataHandler;
    }

    @Override
    public HandlerResult handle(ComparisonJob comparisonJob,
                                Map<String, Object> env,
                                Map<String, Object> transformMap) {
        LocalDateTime startTime = LocalDateTime.now();
        SourceDataMapping sourceDataMapping = tableDataHandler.handle(comparisonJob, env);
        log.debug("use redis bloom filter to handle this job");
        Bloom bloom = createBloom(sourceDataMapping, transformMap);
        if (Objects.isNull(bloom)) {
            return null;
        }
        int seed = ThreadLocalRandom.current().nextInt(bloom.getBitSize());
        String redisKey = String.format(RedisBloomConstant.RedisKey.JOB_FORMAT, comparisonJob.getTenantId(), comparisonJob.getJobCode());
        if (Boolean.TRUE.equals(redisTemplate.hasKey(redisKey))) {
            log.info("redis key[{}] already exists, delete it first", redisKey);
            redisTemplate.delete(redisKey);
        }
        JobEnv jobEnv = BeanUtils.map2Bean(env, JobEnv.class);
        genRedisBloomFilter(jobEnv, sourceDataMapping, bloom, seed, redisKey);
        log.debug("starting......");
        HandlerResult handlerResult = handleComparison(comparisonJob, jobEnv, sourceDataMapping, bloom, seed, redisKey);
        LocalDateTime endTime = LocalDateTime.now();
        log.debug("job time cost : {}", Duration.between(endTime, startTime));
        return handlerResult;
    }

    private HandlerResult handleComparison(ComparisonJob comparisonJob,
                                           JobEnv jobEnv,
                                           SourceDataMapping sourceDataMapping,
                                           Bloom bloom,
                                           int seed,
                                           String redisKey) {
        HandlerResult handlerResult = new HandlerResult();
        List<Map<String, Object>> sourceDataList = sourceDataMapping.getSourceDataList();
        // 分批加多线程
        int batchSize = CommonUtil.calculateBatchSize(sourceDataList.size());
        List<List<Map<String, Object>>> splitList = CommonUtil.splitList(sourceDataList, batchSize);
        CountDownLatch countDownLatch = new CountDownLatch(splitList.size());
        for (List<Map<String, Object>> oneList : splitList) {
            executorService.execute(() -> {
                try {
                    LocalDateTime start = LocalDateTime.now();
                    for (Map<String, Object> map : oneList) {
                        String md5Str = Md5Util.getUppercaseMd5(JsonUtil.toJson(map.values()));
                        List<Integer> hashList = bloom.doHash(md5Str, seed);
                        // 只要有一个hash在redis中找不到 即肯定不存在
                        boolean exists = hashList.stream()
                                .map(hash -> redisTemplate.opsForValue().getBit(redisKey, hash))
                                .anyMatch(Boolean.FALSE::equals);
                        // 可能存在的这里不做处理 因为没什么意义
                        if (Boolean.TRUE.equals(exists)) {
                            // 肯定不存在 需判断主键或唯一索引是否一样 存在误差 若能忽略可以使用
                            // 一般场景用于找pk或索引相同 判断数据是否一致
                            handlerNotExits(handlerResult, jobEnv, comparisonJob, map, sourceDataMapping);
                        }
                    }
                    LocalDateTime end = LocalDateTime.now();
                    log.debug("{} completed data comparison, time cost {}",
                            Thread.currentThread().getName(), Duration.between(start, end));
                } finally {
                    countDownLatch.countDown();
                }
            });
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("handleComparison InterruptedException", e);
        }
        log.debug("handle comparison finish");
        return handlerResult;
    }

    private void handlerNotExits(HandlerResult handlerResult,
                                 JobEnv jobEnv,
                                 ComparisonJob comparisonJob,
                                 Map<String, Object> map,
                                 SourceDataMapping sourceDataMapping) {
        String pkRedisKey = String.format(RedisBloomConstant.RedisKey.TARGET_PK,
                comparisonJob.getTenantId(), comparisonJob.getJobCode());
        Object pkValue = map.get(jobEnv.getSourcePk());
        Boolean isMember = redisTemplate.opsForSet().isMember(pkRedisKey, String.valueOf(pkValue));
        if (Boolean.FALSE.equals(isMember)) {
            // 主键不存在 还需判断唯一索引
            String sourceIndex = jobEnv.getSourceIndex();
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
                String indexRedisKey = String.format(RedisBloomConstant.RedisKey.TARGET_INDEX,
                        comparisonJob.getTenantId(), comparisonJob.getJobCode());
                String value = Stream.of(split).map(s -> (String) map.get(s)).collect(Collectors.joining(","));
                Boolean exists = redisTemplate.opsForSet().isMember(indexRedisKey, value);
                if (Boolean.TRUE.equals(exists)) {
                    // 唯一index存在，其他字段不一样 存target的数据
                    sourceDataMapping.getTargetDataList().stream()
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
            sourceDataMapping.getTargetDataList().stream()
                    .filter(targetMap -> targetMap.get(jobEnv.getTargetPk()).equals(pkValue))
                    .findFirst()
                    .ifPresent(result -> handlerResult.getPkOrIndexSameDataList().add(result));
        }
    }

    private void genRedisBloomFilter(JobEnv jobEnv,
                                     SourceDataMapping sourceDataMapping,
                                     Bloom bloom,
                                     int seed,
                                     String redisKey) {
        // 将target的数据set bit 就能准确找到source到target 未成功同步的数据
        List<Map<String, Object>> list = sourceDataMapping.getTargetDataList();
        if (CollectionUtils.isEmpty(list)) {
            return;
        }
        // 分批加多线程
        int batchSize = CommonUtil.calculateBatchSize(list.size());
        List<List<Map<String, Object>>> splitList = CommonUtil.splitList(list, batchSize);
        CountDownLatch countDownLatch = new CountDownLatch(splitList.size());
        for (List<Map<String, Object>> oneList : splitList) {
            executorService.execute(() -> {
                try {
                    for (Map<String, Object> map : oneList) {
                        String md5Str = Md5Util.getUppercaseMd5(JsonUtil.toJson(map.values()));
                        List<Integer> hashList = bloom.doHash(md5Str, seed);
                        // 设置对应位置为1
                        hashList.forEach(hash -> redisTemplate.opsForValue().setBit(redisKey, hash, true));
                        // redis 存储目标表的pk或索引
                        handlerPkOrIndex(jobEnv, redisKey, map);
                    }
                } finally {
                    countDownLatch.countDown();
                }
            });
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("genRedisBloomFilter InterruptedException", e);
        }
        log.debug("redis bloom filter generated");
    }

    private void handlerPkOrIndex(JobEnv jobEnv,
                                  String redisKey,
                                  Map<String, Object> map) {
        String targetPk = jobEnv.getTargetPk();
        if (StringUtils.isEmpty(targetPk)) {
            // 主键不存在 还需判断唯一索引
            String targetIndex = jobEnv.getTargetIndex();
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
                redisTemplate.opsForSet().add(redisKey + RedisBloomConstant.RedisKey.TARGET_INDEX_SUFFIX, value);
            }
        } else {
            // 有pk
            Object pkValue = map.get(targetPk);
            if (Objects.isNull(pkValue)) {
                return;
            }
            redisTemplate.opsForSet().add(redisKey + RedisBloomConstant.RedisKey.TARGET_PK_SUFFIX, String.valueOf(pkValue));
        }
    }

    private Bloom createBloom(SourceDataMapping sourceDataMapping, Map<String, Object> transformMap) {
        if (Objects.isNull(sourceDataMapping)) {
            return null;
        }
        List<Map<String, Object>> targetDataList = sourceDataMapping.getTargetDataList();
        if (CollectionUtils.isEmpty(targetDataList)) {
            return null;
        }
        // bloom filter位数取值 源表目标表数据量大的
        List<Map<String, Object>> sourceDataList = sourceDataMapping.getSourceDataList();
        int bound = Math.toIntExact(Math.max(targetDataList.size(),
                CollectionUtils.isEmpty(sourceDataList) ? 0 : sourceDataList.size()));
        // 获取fpp
        BloomTransformInfo bloomTransformInfo = BeanUtils.map2Bean(transformMap, BloomTransformInfo.class);
        if (!StringUtils.isEmpty(bloomTransformInfo.getErrorRate())) {
            return new Bloom(bound, bloomTransformInfo.getErrorRate());
        }
        return new Bloom(bound);
    }

}
