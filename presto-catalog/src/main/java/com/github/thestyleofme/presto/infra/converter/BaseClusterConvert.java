package com.github.thestyleofme.presto.infra.converter;

import com.github.thestyleofme.presto.api.dto.ClusterDTO;
import com.github.thestyleofme.presto.domain.entity.Cluster;
import org.mapstruct.Mapper;
import org.mapstruct.factory.Mappers;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/22 11:40
 * @since 1.0.0
 */
@Mapper
public abstract class BaseClusterConvert {

    public static final BaseClusterConvert INSTANCE = Mappers.getMapper(BaseClusterConvert.class);

    /**
     * entityToDTO
     *
     * @param entity Cluster
     * @return ClusterDTO
     */
    public abstract ClusterDTO entityToDTO(Cluster entity);

    /**
     * dtoToEntity
     *
     * @param dto ClusterDTO
     * @return Cluster
     */
    public abstract Cluster dtoToEntity(ClusterDTO dto);

}
