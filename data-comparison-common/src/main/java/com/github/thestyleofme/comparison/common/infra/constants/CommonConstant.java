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

    public static class Sink {

        private Sink() {
        }

        public static final String EXCEL = "excel";
    }

    public static class Deploy {
        public static final String EXCEL_DEPLOY = "excel";
        public static final String PRESTO_DEPLOY = "presto";

        private Deploy() {
        }
    }

}
