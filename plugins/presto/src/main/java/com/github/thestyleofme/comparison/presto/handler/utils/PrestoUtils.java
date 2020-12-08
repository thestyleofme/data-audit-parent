package com.github.thestyleofme.comparison.presto.handler.utils;

import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.github.thestyleofme.comparison.common.domain.ComparisonInfo;
import com.github.thestyleofme.comparison.common.infra.constants.ErrorCode;
import com.github.thestyleofme.comparison.common.infra.exceptions.HandlerException;
import com.github.thestyleofme.comparison.common.infra.utils.CommonUtil;
import com.github.thestyleofme.comparison.presto.handler.pojo.PrestoInfo;
import com.github.thestyleofme.plugin.core.infra.utils.BeanUtils;
import com.github.thestyleofme.presto.app.service.ClusterService;
import com.github.thestyleofme.presto.domain.entity.Cluster;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

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


    public static PrestoInfo getPrestoInfo(Long tenantId,
                                           Map<String, Object> env,
                                           Map<String, Object> transformMap,
                                           ClusterService clusterService) {
        if (CollectionUtils.isEmpty(transformMap)) {
            throw new HandlerException(ErrorCode.TRANSFORM_IS_NULL);
        }
        // 先从transform中取数据，再从env中获取
        PrestoInfo prestoInfo = BeanUtils.map2Bean(transformMap, PrestoInfo.class);
        ComparisonInfo comparisonInfo = CommonUtil.getComparisonInfo(env);
        org.springframework.beans.BeanUtils.copyProperties(comparisonInfo, prestoInfo);
        // 尝试从cluster获取 presto 的 dataSourceCode
        if (StringUtils.isEmpty(prestoInfo.getDataSourceCode()) && !StringUtils.isEmpty(prestoInfo.getClusterCode())) {
            Cluster one = clusterService.getOne(new QueryWrapper<>(Cluster.builder()
                    .tenantId(tenantId).clusterCode(prestoInfo.getClusterCode()).build()));
            Optional.ofNullable(one)
                    .ifPresent(cluster -> {
                        if (!StringUtils.isEmpty(cluster.getDatasourceCode())) {
                            prestoInfo.setDataSourceCode(cluster.getDatasourceCode());
                        }
                        if (!StringUtils.isEmpty(cluster.getCoordinatorUrl())) {
                            prestoInfo.setCoordinatorUrl(cluster.getCoordinatorUrl());
                        }
                        if (!StringUtils.isEmpty(cluster.getUsername())) {
                            prestoInfo.setUsername(cluster.getUsername());
                        }
                    });
        }
        return prestoInfo;
    }

}
