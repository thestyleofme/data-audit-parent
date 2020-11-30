package com.github.thestyleofme.comparison.presto.handler.utils;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.github.thestyleofme.comparison.common.domain.ColMapping;
import com.github.thestyleofme.comparison.common.domain.JobEnv;
import com.github.thestyleofme.comparison.common.domain.SelectTableInfo;
import com.github.thestyleofme.comparison.common.infra.exceptions.HandlerException;
import com.github.thestyleofme.comparison.common.infra.utils.CommonUtil;
import com.github.thestyleofme.comparison.presto.handler.pojo.PrestoInfo;
import com.github.thestyleofme.plugin.core.infra.utils.BeanUtils;
import org.springframework.util.CollectionUtils;

/**
 * <p>
 * description
 * </p>
 *
 * @author hsq 2020/11/26 13:47
 * @since 1.0.0
 */
public class PrestoUtils {

    private PrestoUtils() {
    }

    public static final Pattern HTTP_PATTERN = Pattern.compile("http://(.*?):(.*?)");


    public static PrestoInfo getPrestoInfo(JobEnv jobEnv,
                                           Map<String, Object> transformMap) {
        if (CollectionUtils.isEmpty(transformMap)) {
            throw new HandlerException("hdsp.xadt.error.transform.is_null");
        }
        // 先从transform中取数据，再从env中获取
        PrestoInfo prestoInfo = BeanUtils.map2Bean(transformMap, PrestoInfo.class);
        List<ColMapping> joinMappingList = jobEnv.getJoinMapping().stream()
                .map(mapping -> BeanUtils.map2Bean(mapping, ColMapping.class))
                .sorted(Comparator.comparingInt(ColMapping::getIndex))
                .collect(Collectors.toList());
        List<ColMapping> colMappingList = CommonUtil.getColMappingList(jobEnv);
        prestoInfo.setJoinMapping(joinMappingList);
        prestoInfo.setColMapping(colMappingList);

        SelectTableInfo source = jobEnv.getSource();
        SelectTableInfo target = jobEnv.getTarget();
        prestoInfo.setSource(source);
        prestoInfo.setTarget(target);
        // 构建presto 中表的名字
        prestoInfo.setSourceTableName(SqlGeneratorUtil.getTableName(source));
        prestoInfo.setTargetTableName(SqlGeneratorUtil.getTableName(target));
        return prestoInfo;
    }

}
