package com.github.thestyleofme.comparison.common.infra.annotation;

import java.lang.annotation.*;

/**
 * @author siqi.hou@hand-china.com
 * @date 2020-11-19 11:45
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface DeployType {
    String value();
}
