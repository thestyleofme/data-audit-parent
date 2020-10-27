package com.github.thestyleofme.data.comparison.infra.autoconfigure;

import com.github.thestyleofme.data.comparison.infra.annotation.ComparisonType;
import com.github.thestyleofme.data.comparison.infra.annotation.EngineType;
import com.github.thestyleofme.data.comparison.infra.annotation.OutputType;
import com.github.thestyleofme.data.comparison.infra.context.JobHandlerContext;
import com.github.thestyleofme.data.comparison.infra.handler.BaseJobHandler;
import com.github.thestyleofme.data.comparison.infra.handler.comparison.BaseComparisonHandler;
import com.github.thestyleofme.data.comparison.infra.handler.output.BaseOutputHandler;
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
        EngineType engineType = clazz.getAnnotation(EngineType.class);
        if (engineType != null) {
            String value = engineType.value();
            if (bean instanceof BaseJobHandler) {
                jobHandlerContext.register(value.toUpperCase(), (BaseJobHandler) bean);
            }
        }
        ComparisonType comparisonType = clazz.getAnnotation(ComparisonType.class);
        if (comparisonType != null) {
            String value = comparisonType.value();
            if (bean instanceof BaseComparisonHandler) {
                jobHandlerContext.register(value.toUpperCase(), (BaseComparisonHandler) bean);
            }
        }
        OutputType outputType = clazz.getAnnotation(OutputType.class);
        if (outputType != null) {
            String value = outputType.value();
            if (bean instanceof BaseOutputHandler) {
                jobHandlerContext.register(value.toUpperCase(), (BaseOutputHandler) bean);
            }
        }
        return bean;
    }
}
