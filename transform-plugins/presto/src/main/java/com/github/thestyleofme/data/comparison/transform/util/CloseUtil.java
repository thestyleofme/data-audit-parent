package com.github.thestyleofme.data.comparison.transform.util;
/***
 * 关闭
 * @author siqi.hou@hand-china.com
 * @date 2020-11-18 16:29:55
 **/

public final class CloseUtil {

    private CloseUtil() {
        throw new IllegalStateException("Utils");
    }

    public static void close(AutoCloseable... closeables) {
        for (AutoCloseable closeable : closeables) {
            if (closeable != null) {
                try {
                    closeable.close();
                } catch (Exception e) {
                    // ignore
                }
            }
        }
    }

}