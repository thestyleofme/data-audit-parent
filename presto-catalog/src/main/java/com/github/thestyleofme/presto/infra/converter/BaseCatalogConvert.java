package com.github.thestyleofme.presto.infra.converter;

import com.github.thestyleofme.presto.api.dto.CatalogDTO;
import com.github.thestyleofme.presto.domain.entity.Catalog;
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
public abstract class BaseCatalogConvert {

    public static final BaseCatalogConvert INSTANCE = Mappers.getMapper(BaseCatalogConvert.class);

    /**
     * entityToDTO
     *
     * @param entity Catalog
     * @return CatalogDTO
     */
    public abstract CatalogDTO entityToDTO(Catalog entity);

    /**
     * dtoToEntity
     *
     * @param dto CatalogDTO
     * @return Catalog
     */
    public abstract Catalog dtoToEntity(CatalogDTO dto);

}
