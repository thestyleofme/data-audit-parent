package com.github.thestyleofme.data.comparison.infra.context;

import java.lang.reflect.InvocationHandler;
import java.util.HashMap;
import java.util.Map;

import com.github.thestyleofme.data.comparison.domain.entity.ComparisonJob;
import com.github.thestyleofme.data.comparison.infra.exceptions.HandlerException;
import com.github.thestyleofme.data.comparison.infra.exceptions.RedisBloomException;
import com.github.thestyleofme.data.comparison.infra.handler.BaseJobHandler;
import com.github.thestyleofme.data.comparison.infra.handler.comparison.BaseComparisonHandler;
import com.github.thestyleofme.data.comparison.infra.handler.output.BaseOutputHandler;
import com.github.thestyleofme.data.comparison.infra.utils.CommonUtil;
import com.github.thestyleofme.data.comparison.infra.utils.HandlerUtil;
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
    private static final Map<String, BaseOutputHandler> OUTPUT_HANDLER_MAP = new HashMap<>();

    public void register(String engineType, BaseJobHandler baseJobHandler) {
        if (JOB_HANDLER_MAP.containsKey(engineType)) {
            log.error("engineType {} exists", engineType);
            throw new HandlerException("error.data.comparison.job.handler.engineType.exist");
        }
        BaseJobHandler proxyJobHandler = CommonUtil.createProxy(
                baseJobHandler.getClass().getClassLoader(),
                BaseJobHandler.class,
                deleteRedisKeyInvocationHandler(baseJobHandler)
        );
        JOB_HANDLER_MAP.put(engineType, proxyJobHandler);
    }

    public void register(String comparisonType, BaseComparisonHandler baseComparisonHandler) {
        if (COMPARISON_HANDLER_MAP.containsKey(comparisonType)) {
            log.error("comparisonType {} exists", comparisonType);
            throw new HandlerException("error.data.comparison.job.handler.comparisonType.exist");
        }
        COMPARISON_HANDLER_MAP.put(comparisonType, baseComparisonHandler);
    }

    public void register(String outputType, BaseOutputHandler baseOutputHandler) {
        if (OUTPUT_HANDLER_MAP.containsKey(outputType)) {
            log.error("outputType {} exists", outputType);
            throw new HandlerException("error.data.comparison.job.handler.outputType.exist");
        }
        BaseOutputHandler proxyOutputHandler = CommonUtil.createProxy(
                baseOutputHandler.getClass().getClassLoader(),
                BaseOutputHandler.class,
                deleteRedisKeyInvocationHandler(baseOutputHandler)
        );
        OUTPUT_HANDLER_MAP.put(outputType, proxyOutputHandler);
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

    public BaseOutputHandler getOutputHandler(String outputType) {
        BaseOutputHandler baseOutputHandler = OUTPUT_HANDLER_MAP.get(outputType.toUpperCase());
        if (baseOutputHandler == null) {
            throw new HandlerException("error.data.comparison.job.handler.outputType.not.exist");
        }
        return baseOutputHandler;
    }

    private InvocationHandler deleteRedisKeyInvocationHandler(Object obj) {
        return (proxy, method, args) -> {
            try {
                return method.invoke(obj, args);
            } catch (RedisBloomException e) {
                throw e;
            } catch (Exception e) {
                // 抛其他异常需删除redis相关的key
                ComparisonJob comparisonJob = (ComparisonJob) args[0];
                log.error("delete comparison job[{}_{}] redis key", comparisonJob.getId(), comparisonJob.getJobName());
                HandlerUtil.deleteRedisKey(comparisonJob);
                throw e;
            }
        };
    }

}
