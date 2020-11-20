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
    public static final String AUDIT = "AUDIT";
    public static final String DEPLOY = "DEPLOY";
    public static final String FAILED_FORMAT = "%s_FAILED";
    public static final String DEFAULT_PRESTO_USERNAME = "PRESTO";

    public static class Sink {

        private Sink() {
        }

        public static final String EXCEL = "excel";
    }

    public static class Deploy {

        private Deploy() {}

        public static final String EXCEL = "excel";
        public static final String PRESTO = "presto";

    }

}
