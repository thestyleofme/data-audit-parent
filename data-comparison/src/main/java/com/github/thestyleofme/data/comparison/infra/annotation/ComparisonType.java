package com.github.thestyleofme.data.comparison.infra.annotation;

import java.lang.annotation.*;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/9/15 15:01
 * @since 1.0.0
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface ComparisonType {

    String value();
}
