package com.github.thestyleofme.comparison.common.infra.annotation;

import java.lang.annotation.*;

/**
 * <p></p>
 *
 * @author hsq 2020/12/03 14:59
 * @since 1.0.0
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface DataxReaderType {
    
    String value();
}
