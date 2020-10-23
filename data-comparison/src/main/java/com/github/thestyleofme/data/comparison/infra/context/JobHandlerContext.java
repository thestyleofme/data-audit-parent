package com.github.thestyleofme.data.comparison.infra.context;

import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

import com.github.thestyleofme.data.comparison.domain.entity.ComparisonJob;
import com.github.thestyleofme.data.comparison.infra.exceptions.HandlerException;
import com.github.thestyleofme.data.comparison.infra.handler.BaseJobHandler;
import com.github.thestyleofme.data.comparison.infra.handler.comparison.BaseComparisonHandler;
import com.github.thestyleofme.data.comparison.infra.utils.CommonUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/22 15:21
 * @since 1.0.0
 */
@Component
@Slf4j
public class JobHandlerContext {

    private static final Map<String, BaseJobHandler> JOB_HANDLER_MAP = new HashMap<>();
    private static final Map<String, BaseComparisonHandler> COMPARISON_HANDLER_MAP = new HashMap<>();

    public void register(String engineType, BaseJobHandler baseJobHandler) {
        if (JOB_HANDLER_MAP.containsKey(engineType)) {
            log.error("engineType {} exists", engineType);
            throw new HandlerException("error.data.comparison.job.handler.engineType.exist");
        }
        BaseJobHandler proxyJobHandler = (BaseJobHandler) Proxy.newProxyInstance(
                baseJobHandler.getClass().getClassLoader(),
                new Class[]{BaseJobHandler.class},
                (proxy, method, args) -> {
                    try {
                        return method.invoke(baseJobHandler, args);
                    } catch (Exception e) {
                        // 抛异常需删除redis相关的key
                        ComparisonJob comparisonJob = (ComparisonJob) args[0];
                        log.error("delete comparison job[{}_{}] redis key", comparisonJob.getId(), comparisonJob.getJobName());
                        CommonUtil.deleteRedisKey(comparisonJob);
                        throw e;
                    }
                });
        JOB_HANDLER_MAP.put(engineType, proxyJobHandler);
    }

    public void register(String comparisonType, BaseComparisonHandler baseComparisonHandler) {
        if (COMPARISON_HANDLER_MAP.containsKey(comparisonType)) {
            log.error("comparisonType {} exists", comparisonType);
            throw new HandlerException("error.data.comparison.job.handler.comparisonType.exist");
        }
        COMPARISON_HANDLER_MAP.put(comparisonType, baseComparisonHandler);
    }


    public BaseJobHandler getJobHandler(String engineType) {
        BaseJobHandler baseJobHandler = JOB_HANDLER_MAP.get(engineType.toUpperCase());
        if (baseJobHandler == null) {
            throw new HandlerException("error.data.comparison.job.handler.engineType.not.exist");
        }
        return baseJobHandler;
    }

    public BaseComparisonHandler getComparisonHandler(String comparisonType) {
        BaseComparisonHandler baseComparisonHandler = COMPARISON_HANDLER_MAP.get(comparisonType.toUpperCase());
        if (baseComparisonHandler == null) {
            throw new HandlerException("error.data.comparison.job.handler.comparisonType.not.exist");
        }
        return baseComparisonHandler;
    }

}
