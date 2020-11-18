package com.github.thestyleofme.presto.app.service.impl;

import java.util.stream.Collectors;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.github.thestyleofme.presto.api.dto.ClusterDTO;
import com.github.thestyleofme.presto.app.service.ClusterService;
import com.github.thestyleofme.presto.domain.entity.Cluster;
import com.github.thestyleofme.presto.infra.converter.BaseClusterConvert;
import com.github.thestyleofme.presto.infra.mapper.ClusterMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/09 15:18
 * @since 1.0.0
 */
@Service
@Slf4j
public class ClusterServiceImpl extends ServiceImpl<ClusterMapper, Cluster> implements ClusterService {

    @Override
    public IPage<ClusterDTO> list(Page<Cluster> page, ClusterDTO clusterDTO) {
        QueryWrapper<Cluster> queryWrapper = new QueryWrapper<>(
                BaseClusterConvert.INSTANCE.dtoToEntity(clusterDTO));
        Page<Cluster> entityPage = page(page, queryWrapper);
        final Page<ClusterDTO> dtoPage = new Page<>();
        BeanUtils.copyProperties(entityPage, dtoPage);
        dtoPage.setRecords(entityPage.getRecords().stream()
                .map(BaseClusterConvert.INSTANCE::entityToDTO)
                .collect(Collectors.toList()));
        return dtoPage;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public ClusterDTO save(ClusterDTO clusterDTO) {
        Cluster cluster = BaseClusterConvert.INSTANCE.dtoToEntity(clusterDTO);
        this.saveOrUpdate(cluster);
        return BaseClusterConvert.INSTANCE.entityToDTO(cluster);
    }
}
