package com.github.thestyleofme.data.comparison.infra.handler.transform.java;

import java.util.Map;

import com.github.thestyleofme.comparison.common.app.service.transform.BaseTransformHandler;
import com.github.thestyleofme.comparison.common.app.service.transform.HandlerResult;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.infra.annotation.TransformType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/12/02 14:50
 * @since 1.0.0
 */
@Component
@Slf4j
@TransformType(value = "JAVA")
public class JavaTransformHandler implements BaseTransformHandler {

    @Override
    public void handle(ComparisonJob comparisonJob,
                       Map<String, Object> env,
                       Map<String, Object> preTransform,
                       Map<String, Object> transformMap,
                       HandlerResult handlerResult) {
        // List<String> paramName = Arrays.asList("id", "name", "sex", "phone","address","education","state");
        // DataSelector.Result result = DataSelector.init(paramName)
        //         .addMain(list1())
        //         .addSub(list2())
        //         .select();
    }
}
