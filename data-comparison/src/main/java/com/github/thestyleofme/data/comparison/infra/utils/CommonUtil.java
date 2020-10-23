package com.github.thestyleofme.data.comparison.infra.utils;

import java.util.Optional;

import com.github.thestyleofme.data.comparison.domain.entity.ComparisonJob;
import com.github.thestyleofme.data.comparison.infra.constants.CommonConstant;
import org.apache.ibatis.plugin.PluginException;
import org.springframework.context.ApplicationContext;
import org.springframework.data.redis.core.RedisTemplate;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/23 15:30
 * @since 1.0.0
 */
public class CommonUtil {

    private CommonUtil() {
    }

    private static final RedisTemplate<String, String> REDIS_TEMPLATE;

    static {
        ApplicationContext context = Optional.ofNullable(ApplicationContextHelper.getContext())
                .orElseThrow(() -> new PluginException("not spring env, cannot get ApplicationContext"));
        // noinspection unchecked
        REDIS_TEMPLATE = context.getBean("redisTemplate", RedisTemplate.class);
    }

    public static void deleteRedisKey(ComparisonJob comparisonJob) {
        String redisKey = String.format(CommonConstant.RedisKey.JOB_FORMAT, comparisonJob.getTenantId(), comparisonJob.getJobName());
        if (Boolean.TRUE.equals(REDIS_TEMPLATE.hasKey(redisKey))) {
            REDIS_TEMPLATE.delete(redisKey);
        }
        String pkRedisKey = String.format(CommonConstant.RedisKey.TARGET_PK,
                comparisonJob.getTenantId(), comparisonJob.getJobName());
        if (Boolean.TRUE.equals(REDIS_TEMPLATE.hasKey(pkRedisKey))) {
            REDIS_TEMPLATE.delete(pkRedisKey);
        }
        String indexRedisKey = String.format(CommonConstant.RedisKey.TARGET_INDEX,
                comparisonJob.getTenantId(), comparisonJob.getJobName());
        if (Boolean.TRUE.equals(REDIS_TEMPLATE.hasKey(indexRedisKey))) {
            REDIS_TEMPLATE.delete(indexRedisKey);
        }
    }
}
