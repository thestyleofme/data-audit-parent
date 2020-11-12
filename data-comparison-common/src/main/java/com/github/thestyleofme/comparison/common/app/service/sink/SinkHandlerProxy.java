package com.github.thestyleofme.comparison.common.app.service.sink;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/11 16:22
 * @since 1.0.0
 */
public interface SinkHandlerProxy {

    /**
     * 对BaseSinkHandler创建代理
     *
     * @param baseSinkHandler BaseSinkHandler
     * @return BaseSinkHandler
     */
    BaseSinkHandler proxy(BaseSinkHandler baseSinkHandler);
}
