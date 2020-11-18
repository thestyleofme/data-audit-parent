package com.github.thestyleofme.data.comparison.infra.exceptions;

import com.github.thestyleofme.comparison.common.infra.exceptions.HandlerException;
import com.github.thestyleofme.comparison.common.infra.utils.HandlerUtil;
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

    @ExceptionHandler(HandlerException.class)
    public Err handleHandlerException(HandlerException e) {
        log.error("HandlerException", e);
        return Err.of(HandlerUtil.getMessage(e));
    }

    @ExceptionHandler(Exception.class)
    public Err handleException(Exception e) {
        log.error("Exception", e);
        return Err.of(HandlerUtil.getMessage(e));
    }

}
