package com.github.thestyleofme.comparison.common.infra.utils;

import java.util.Map;
import java.util.regex.Pattern;

import com.github.thestyleofme.comparison.common.domain.AppConf;
import com.github.thestyleofme.comparison.common.domain.JobEnv;
import com.github.thestyleofme.comparison.common.domain.PrestoInfo;
import com.github.thestyleofme.comparison.common.infra.exceptions.HandlerException;
import com.github.thestyleofme.plugin.core.infra.utils.BeanUtils;
import org.springframework.util.CollectionUtils;

/**
 * @author siqi.hou@hand-china.com
 * @date 2020-11-19 16:42
 */
public class PrestoUtils {
    public static Pattern HTTP_PATTERN = Pattern.compile("http://(.*?):(.*?)");

    public static PrestoInfo getPrestoInfo(AppConf appConf,
                                           Map<String, Object> transformMap) {
        if (CollectionUtils.isEmpty(transformMap)) {
            throw new HandlerException("hdsp.xadt.error.transform.is_null");
        }
        // 优先使用transform中数据，其次从env中获取
        PrestoInfo prestoInfo = BeanUtils.map2Bean(transformMap, PrestoInfo.class);
        JobEnv jobEnv = BeanUtils.map2Bean(appConf.getEnv(), JobEnv.class);
        prestoInfo.setSourcePrestoCatalog(CommonUtil.requireNonNullElse(prestoInfo.getSourcePrestoCatalog(),
                jobEnv.getSourcePrestoCatalog()));
        prestoInfo.setSourceSchema(CommonUtil.requireNonNullElse(prestoInfo.getSourceSchema(), jobEnv.getSourceSchema()));
        prestoInfo.setSourceTable(CommonUtil.requireNonNullElse(prestoInfo.getSourceTable(), jobEnv.getSourceTable()));
        prestoInfo.setTargetPrestoCatalog(CommonUtil.requireNonNullElse(prestoInfo.getTargetPrestoCatalog(),
                jobEnv.getTargetPrestoCatalog()));
        prestoInfo.setTargetSchema(CommonUtil.requireNonNullElse(prestoInfo.getTargetSchema(), jobEnv.getTargetSchema()));
        prestoInfo.setTargetTable(CommonUtil.requireNonNullElse(prestoInfo.getTargetTable(), jobEnv.getTargetTable()));
        return prestoInfo;
    }

}
