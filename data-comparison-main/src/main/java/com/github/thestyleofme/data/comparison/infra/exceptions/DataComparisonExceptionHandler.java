package com.github.thestyleofme.data.comparison.infra.exceptions;

import com.github.thestyleofme.comparison.common.infra.exceptions.BaseException;
import com.github.thestyleofme.comparison.common.infra.utils.HandlerUtil;
import com.github.thestyleofme.data.comparison.infra.utils.LocaleUtil;
import com.github.thestyleofme.driver.core.domain.entity.Err;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * <p>
 * 全局统一异常处理
 * </p>
 *
 * @author isaac 2020/10/27 11:49
 * @since 1.0.0
 */
@Slf4j
@Order(Ordered.LOWEST_PRECEDENCE - 100)
@RestControllerAdvice
public class DataComparisonExceptionHandler {

    @ExceptionHandler(BaseException.class)
    public Err handleBaseException(BaseException e) {
        log.error("BaseException", e);
        return Err.of(LocaleUtil.getMessage(e.getCode(), e.getParams()));
    }

    @ExceptionHandler(Exception.class)
    public Err handleException(Exception e) {
        log.error("Exception", e);
        return Err.of(LocaleUtil.getMessage(HandlerUtil.getMessage(e)));
    }

}
