package com.github.thestyleofme.data.comparison.infra.converter;

import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJobGroup;
import com.github.thestyleofme.data.comparison.api.dto.ComparisonJobGroupDTO;
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
public abstract class BaseComparisonJobGroupConvert {

    public static final BaseComparisonJobGroupConvert INSTANCE = Mappers.getMapper(BaseComparisonJobGroupConvert.class);

    /**
     * entityToDTO
     *
     * @param entity Plugin
     * @return ComparisonJobGroup
     */
    public abstract ComparisonJobGroupDTO entityToDTO(ComparisonJobGroup entity);

    /**
     * dtoToEntity
     *
     * @param dto ComparisonJobGroupDTO
     * @return ComparisonJobGroup
     */
    public abstract ComparisonJobGroup dtoToEntity(ComparisonJobGroupDTO dto);

}
