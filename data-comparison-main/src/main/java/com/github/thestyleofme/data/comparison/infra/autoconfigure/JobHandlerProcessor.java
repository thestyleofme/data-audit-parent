package com.github.thestyleofme.data.comparison.infra.autoconfigure;

import java.util.Optional;

import com.github.thestyleofme.comparison.common.app.service.deploy.BaseDeployHandler;
import com.github.thestyleofme.comparison.common.app.service.sink.BaseSinkHandler;
import com.github.thestyleofme.comparison.common.app.service.sink.SinkHandlerProxy;
import com.github.thestyleofme.comparison.common.app.service.transform.BaseTransformHandler;
import com.github.thestyleofme.comparison.common.app.service.transform.TransformHandlerProxy;
import com.github.thestyleofme.comparison.common.infra.annotation.DeployType;
import com.github.thestyleofme.comparison.common.infra.annotation.SinkType;
import com.github.thestyleofme.comparison.common.infra.annotation.TransformType;
import com.github.thestyleofme.data.comparison.infra.context.JobHandlerContext;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/22 15:19
 * @since 1.0.0
 */
@Component
public class JobHandlerProcessor implements BeanPostProcessor {

    private final JobHandlerContext jobHandlerContext;

    public JobHandlerProcessor(JobHandlerContext jobHandlerContext) {
        this.jobHandlerContext = jobHandlerContext;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, @NonNull String beanName) {
        Class<?> clazz = bean.getClass();
        doTransform(bean, clazz);
        doSink(bean, clazz);
        doDeploy(bean, clazz);
        return bean;
    }

    private void doTransform(Object bean, Class<?> clazz) {
        TransformType transformType = clazz.getAnnotation(TransformType.class);
        if (transformType != null) {
            String key = transformType.value();
            if (bean instanceof BaseTransformHandler) {
                jobHandlerContext.register(key.toUpperCase(), (BaseTransformHandler) bean);
            }
            if (bean instanceof TransformHandlerProxy) {
                jobHandlerContext.register(key.toUpperCase(), (TransformHandlerProxy) bean);
            }
        }
    }

    private void doSink(Object bean, Class<?> clazz) {
        SinkType sinkType = clazz.getAnnotation(SinkType.class);
        if (sinkType != null) {
            String value = sinkType.value();
            if (bean instanceof BaseSinkHandler) {
                jobHandlerContext.register(value.toUpperCase(), (BaseSinkHandler) bean);
            }
            if (bean instanceof SinkHandlerProxy) {
                jobHandlerContext.register(value.toUpperCase(), (SinkHandlerProxy) bean);
            }
        }
    }

    private void doDeploy(Object bean, Class<?> clazz) {
        DeployType deployType = clazz.getAnnotation(DeployType.class);
        Optional.ofNullable(deployType).map(DeployType::value).ifPresent(
                value -> {
                    if (bean instanceof BaseDeployHandler) {
                        jobHandlerContext.register(value.toUpperCase(), (BaseDeployHandler) bean);
                    }
                }
        );
    }
}
