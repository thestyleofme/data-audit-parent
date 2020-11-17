package com.github.thestyleofme.comparison.common.infra.constants;

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
    public static final String CONTACT = "%s_%s";
    public static final String TYPE = "type";
    public static final Integer FOUR = 4;

    public static class RedisKey {
        private RedisKey() {
        }

        public static final String JOB_PREFIX = "hdsp:job:comparison:";
        public static final String JOB_FORMAT = JOB_PREFIX + "%d_%s";
        public static final String TARGET_PK_SUFFIX = ":pk";
        public static final String TARGET_PK = JOB_FORMAT + ":pk";
        public static final String TARGET_INDEX_SUFFIX = ":index";
        public static final String TARGET_INDEX = JOB_FORMAT + TARGET_INDEX_SUFFIX;
    }

}
