package com.github.thestyleofme.comparison.common.infra.utils;

import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/23 15:30
 * @since 1.0.0
 */
@Slf4j
public class HandlerUtil {

    private HandlerUtil() {
    }

    /**
     * 获取原始的错误信息，如果没有cause则返回当前message
     *
     * @param e Exception
     * @return 错误信息
     */
    public static String getMessage(Exception e) {
        Throwable cause = e.getCause();
        if (cause == null) {
            return e.getMessage();
        }
        return cause.getMessage();
    }

    /**
     * 时间戳转为时分秒
     *
     * @param milliseconds 毫秒
     * @return java.lang.String
     */
    public static String timestamp2String(long milliseconds) {
        long seconds = milliseconds / 1000;
        long hour = seconds / 3600;
        long minute = (seconds - hour * 3600) / 60;
        long second = (seconds - hour * 3600 - minute * 60);

        StringBuilder sb = new StringBuilder();
        if (hour > 0) {
            sb.append(hour).append("h ");
        }
        if (minute > 0) {
            sb.append(minute).append("m ");
        }
        if (second > 0) {
            sb.append(second).append("s");
        }
        if (second == 0) {
            sb.append("<1s");
        }
        return sb.toString();
    }

}
