package com.github.thestyleofme.comparison.common.infra.annotation;

import java.lang.annotation.*;

/**
 * <p>
 * description
 * </p>
 *
 * @author hsq
 * @date 2020-11-19 11:45
 * @since 1.0.0
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface DeployType {

    String value();
}
