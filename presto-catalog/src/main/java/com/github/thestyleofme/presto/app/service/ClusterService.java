package com.github.thestyleofme.presto.app.service;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.IService;
import com.github.thestyleofme.presto.api.dto.ClusterDTO;
import com.github.thestyleofme.presto.domain.entity.Cluster;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/09 14:35
 * @since 1.0.0
 */
public interface ClusterService extends IService<Cluster> {

    /**
     * 分页条件查询
     *
     * @param page       分页
     * @param clusterDTO ClusterDTO
     * @return IPage<ClusterDTO>
     */
    IPage<ClusterDTO> list(Page<Cluster> page, ClusterDTO clusterDTO);

    /**
     * 保存
     *
     * @param clusterDTO ClusterDTO
     * @return ClusterDTO
     */
    ClusterDTO save(ClusterDTO clusterDTO);
}
