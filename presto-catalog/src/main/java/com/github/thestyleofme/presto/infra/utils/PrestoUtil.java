package com.github.thestyleofme.presto.infra.utils;

import java.util.regex.Pattern;

/**
 * @author siqi.hou@hand-china.com
 * @date 2020-11-18 17:17
 */
public class PrestoUtil {
    public static Pattern HTTP_PATTERN = Pattern.compile("http://(.*?):(.*?)");
}
