package com.github.thestyleofme.comparison.presto.handler.hook;

import java.util.Collections;
import java.util.List;

import com.github.thestyleofme.comparison.presto.handler.pojo.SkipCondition;
import org.springframework.stereotype.Component;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/30 17:50
 * @since 1.0.0
 */
@Component
public class DefaultPreTransformHook extends BasePreTransformHook {

    @Override
    public String getName() {
        return "DEFAULT";
    }

    @Override
    protected boolean execSqlAndComputeSkip(List<String> sqlList) {
        return true;
    }

    @Override
    protected List<String> generateSqlByCondition(List<SkipCondition> skipConditionList) {
        return Collections.emptyList();
    }

}
