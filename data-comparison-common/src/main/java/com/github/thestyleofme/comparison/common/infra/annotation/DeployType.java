package com.github.thestyleofme.comparison.common.infra.annotation;

import java.lang.annotation.*;

/**
 * <p>
 * description
 * </p>
 *
 * @author siqi.hou 2020/11/20 14:08
 * @since 1.0.0
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface DeployType {

    String value();
}
