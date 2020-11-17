package com.github.thestyleofme.data.comparison.transform.handler;

import com.github.thestyleofme.comparison.common.app.service.transform.BaseTransformHandler;
import com.github.thestyleofme.comparison.common.app.service.transform.TransformHandlerProxy;
import com.github.thestyleofme.comparison.common.infra.annotation.TransformType;
import com.github.thestyleofme.comparison.common.infra.utils.CommonUtil;
import com.github.thestyleofme.comparison.common.infra.utils.HandlerUtil;
import com.github.thestyleofme.data.comparison.transform.exceptions.PrestoException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * @author siqi.hou@hand-china.com
 * @date 2020-11-16 19:45
 */
@TransformType("PRESTO")
@Component
@Slf4j
public class PrestoTransformHandlerProxy implements TransformHandlerProxy {

    @Override
    public BaseTransformHandler proxy(BaseTransformHandler baseTransformHandler) {
        return CommonUtil.createProxy(
                baseTransformHandler.getClass().getClassLoader(),
                BaseTransformHandler.class,
                (proxy, method, args) -> {
                    try {
                        Object invoke = method.invoke(baseTransformHandler, args);
                        // 正常执行完操作 // TODO
                        System.out.println("presto 执行完毕");
                        return invoke;
                    } catch (PrestoException e) {
                        throw e;
                    } catch (Exception e) {
                        // 抛其他异常需删除redis相关的key
                        HandlerUtil.deleteRedisKey(args);
                        throw e;
                    }
                }
        );
    }
}
