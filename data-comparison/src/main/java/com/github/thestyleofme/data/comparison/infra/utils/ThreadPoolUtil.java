package com.github.thestyleofme.data.comparison.infra.utils;

import java.util.concurrent.*;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * <p>
 * 线程池
 * </p>
 *
 * @author isacc 2019/07/29 16:29
 * @since 1.0
 */
public class ThreadPoolUtil {

    private ThreadPoolUtil() {
        throw new IllegalStateException("Utility class");
    }

    private static final ExecutorService executorService;

    static {
        /*
         * ThreadPoolExecutor参数解释
         *   1.corePoolSize 核心线程池大小
         *   2.maximumPoolSize 线程池最大容量大小
         *   3.keepAliveTime 线程池空闲时，线程存活的时间
         *   4.TimeUnit 时间单位
         *   5.ThreadFactory 线程工厂
         *   6.BlockingQueue任务队列
         *   7.RejectedExecutionHandler 线程拒绝策略
         */
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("jobExecutor-%d").build();
        executorService = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors() * 2 + 1,
                64,
                30L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(64),
                namedThreadFactory,
                new ThreadPoolExecutor.AbortPolicy());
    }

    public static ExecutorService getExecutorService() {
        return executorService;
    }
}
