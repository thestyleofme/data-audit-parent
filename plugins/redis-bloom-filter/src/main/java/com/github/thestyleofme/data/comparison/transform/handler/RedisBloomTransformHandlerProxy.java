package com.github.thestyleofme.data.comparison.transform.handler;

import java.util.Set;

import com.github.thestyleofme.comparison.common.app.service.transform.BaseTransformHandler;
import com.github.thestyleofme.comparison.common.app.service.transform.TransformHandlerProxy;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.infra.annotation.TransformType;
import com.github.thestyleofme.comparison.common.infra.utils.CommonUtil;
import com.github.thestyleofme.data.comparison.transform.constants.RedisBloomConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/12 15:49
 * @since 1.0.0
 */
@Component
@TransformType(value = "REDIS_BLOOM_FILTER")
@Slf4j
public class RedisBloomTransformHandlerProxy implements TransformHandlerProxy {

    private final RedisTemplate<String, String> redisTemplate;

    public RedisBloomTransformHandlerProxy(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    public BaseTransformHandler proxy(BaseTransformHandler baseTransformHandler) {
        return CommonUtil.createProxy(
                baseTransformHandler.getClass().getClassLoader(),
                BaseTransformHandler.class,
                (proxy, method, args) -> {
                    try {
                        Object invoke = method.invoke(baseTransformHandler, args);
                        // 正常执行完删除redis相关的key
                        deleteRedisKey(args);
                        return invoke;
                    } catch (Exception e) {
                        // 抛其他异常需删除redis相关的key
                        deleteRedisKey(args);
                        throw e;
                    }
                }
        );
    }

    private void deleteRedisKey(Object[] args) {
        ComparisonJob comparisonJob = (ComparisonJob) args[0];
        String redisKey = String.format(RedisBloomConstant.RedisKey.JOB_FORMAT, comparisonJob.getTenantId(), comparisonJob.getJobCode());
        deleteRedisKey(comparisonJob, redisKey);
    }

    private void deleteRedisKey(ComparisonJob comparisonJob, String redisKey) {
        Set<String> keys = redisTemplate.keys(redisKey + "*");
        if (!CollectionUtils.isEmpty(keys)) {
            redisTemplate.delete(keys);
        }
        log.debug("delete comparison job[{}_{}] redis key", comparisonJob.getJobId(), comparisonJob.getJobCode());
    }
}
