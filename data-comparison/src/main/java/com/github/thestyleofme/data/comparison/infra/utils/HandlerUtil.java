package com.github.thestyleofme.data.comparison.infra.utils;

import java.util.Optional;
import java.util.Set;

import com.github.thestyleofme.data.comparison.domain.entity.ComparisonJob;
import com.github.thestyleofme.data.comparison.infra.constants.CommonConstant;
import org.apache.ibatis.plugin.PluginException;
import org.springframework.context.ApplicationContext;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.util.CollectionUtils;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/23 15:30
 * @since 1.0.0
 */
public class HandlerUtil {

    private HandlerUtil() {
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
        Set<String> keys = REDIS_TEMPLATE.keys(redisKey + "*");
        if (!CollectionUtils.isEmpty(keys)) {
            REDIS_TEMPLATE.delete(keys);
        }
    }

}
