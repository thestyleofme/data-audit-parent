package com.github.thestyleofme.data.comparison.transform.handler;

import java.lang.reflect.InvocationTargetException;

import com.github.thestyleofme.comparison.common.app.service.transform.BaseTransformHandler;
import com.github.thestyleofme.comparison.common.app.service.transform.TransformHandlerProxy;
import com.github.thestyleofme.comparison.common.infra.annotation.TransformType;
import com.github.thestyleofme.comparison.common.infra.exceptions.HandlerException;
import com.github.thestyleofme.comparison.common.infra.utils.CommonUtil;
import com.github.thestyleofme.comparison.common.infra.utils.HandlerUtil;
import com.github.thestyleofme.data.comparison.transform.exceptions.RedisBloomException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.stereotype.Component;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/12 15:49
 * @since 1.0.0
 */
@Component
@TransformType("BLOOM_FILTER")
@Slf4j
public class RedisBloomTransformHandlerProxy implements TransformHandlerProxy {

    @Override
    public BaseTransformHandler proxy(BaseTransformHandler baseTransformHandler) {
        return CommonUtil.createProxy(
                baseTransformHandler.getClass().getClassLoader(),
                BaseTransformHandler.class,
                (proxy, method, args) -> {
                    try {
                        Object invoke = method.invoke(baseTransformHandler, args);
                        // 正常执行完删除redis相关的key
                        HandlerUtil.deleteRedisKey(args);
                        return invoke;
                    } catch (RedisBloomException | InvocationTargetException e) {
                        throw new HandlerException(ExceptionUtils.getRootCauseMessage(e));
                    } catch (Exception e) {
                        // 抛其他异常需删除redis相关的key
                        HandlerUtil.deleteRedisKey(args);
                        throw e;
                    }
                }
        );
    }
}
