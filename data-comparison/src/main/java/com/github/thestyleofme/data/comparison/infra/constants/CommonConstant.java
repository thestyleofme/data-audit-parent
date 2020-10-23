package com.github.thestyleofme.data.comparison.infra.constants;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/23 11:33
 * @since 1.0.0
 */
public class CommonConstant {

    private CommonConstant() {
    }

    public static class RedisKey {
        private RedisKey() {
        }

        public static final String JOB_PREFIX = "hdsp:job:comparison:";
        public static final String JOB_FORMAT = JOB_PREFIX + "%d_%s";
        public static final String TARGET_PK_SUFFIX = ":pk";
        public static final String TARGET_PK = JOB_FORMAT + ":pk";
        public static final String TARGET_INDEX_SUFFIX = ":index";
        public static final String TARGET_INDEX = JOB_FORMAT + ":index";
    }

}
