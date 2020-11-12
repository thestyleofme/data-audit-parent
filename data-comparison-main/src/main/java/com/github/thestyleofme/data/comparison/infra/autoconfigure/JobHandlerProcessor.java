package com.github.thestyleofme.data.comparison.infra.autoconfigure;

import com.github.thestyleofme.comparison.common.app.service.sink.BaseSinkHandler;
import com.github.thestyleofme.comparison.common.app.service.sink.SinkHandlerProxy;
import com.github.thestyleofme.comparison.common.app.service.source.BaseSourceHandler;
import com.github.thestyleofme.comparison.common.app.service.transform.BaseTransformHandler;
import com.github.thestyleofme.comparison.common.app.service.transform.TransformHandlerProxy;
import com.github.thestyleofme.comparison.common.infra.annotation.SinkType;
import com.github.thestyleofme.comparison.common.infra.annotation.SourceType;
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
        SourceType sourceType = clazz.getAnnotation(SourceType.class);
        if (sourceType != null) {
            String value = sourceType.value();
            if (bean instanceof BaseSourceHandler) {
                jobHandlerContext.register(value.toUpperCase(), (BaseSourceHandler) bean);
            }
        }
        TransformType transformType = clazz.getAnnotation(TransformType.class);
        if (transformType != null) {
            String value = transformType.value();
            String type = transformType.type();
            if (bean instanceof BaseTransformHandler) {
                jobHandlerContext.register(String.format("%s_%s", value, type).toUpperCase(), (BaseTransformHandler) bean);
            }
            if(bean instanceof TransformHandlerProxy){
                jobHandlerContext.register(String.format("%s_%s", value, type).toUpperCase(), (TransformHandlerProxy) bean);
            }
        }
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
        return bean;
    }
}
