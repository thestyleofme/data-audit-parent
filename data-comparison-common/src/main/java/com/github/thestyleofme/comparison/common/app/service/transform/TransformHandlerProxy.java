package com.github.thestyleofme.comparison.common.app.service.transform;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/11 16:22
 * @since 1.0.0
 */
public interface TransformHandlerProxy {

    /**
     * 对BaseTransformHandler创建代理
     *
     * @param baseTransformHandler BaseTransformHandler
     * @return BaseTransformHandler
     */
    BaseTransformHandler proxy(BaseTransformHandler baseTransformHandler);
}
