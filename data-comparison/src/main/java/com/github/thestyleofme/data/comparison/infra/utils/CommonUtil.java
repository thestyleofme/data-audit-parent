package com.github.thestyleofme.data.comparison.infra.utils;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/23 15:30
 * @since 1.0.0
 */
public class CommonUtil {

    private CommonUtil() {
    }

    @SuppressWarnings("unchecked")
    public static <T> T createProxy(ClassLoader classLoader,
                                    Class<T> clazz,
                                    InvocationHandler invocationHandler) {
        return (T) Proxy.newProxyInstance(
                classLoader,
                new Class[]{clazz},
                invocationHandler);
    }
}
