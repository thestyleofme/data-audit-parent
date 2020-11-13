package com.github.thestyleofme.data.comparison.infra.converter;

import com.github.thestyleofme.comparison.common.domain.ComparisonJob;
import com.github.thestyleofme.data.comparison.api.dto.ComparisonJobDTO;
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
public abstract class BaseComparisonJobConvert {

    public static final BaseComparisonJobConvert INSTANCE = Mappers.getMapper(BaseComparisonJobConvert.class);

    /**
     * entityToDTO
     *
     * @param entity Plugin
     * @return ComparisonJobDTO
     */
    public abstract ComparisonJobDTO entityToDTO(ComparisonJob entity);

    /**
     * dtoToEntity
     *
     * @param dto ComparisonJobDTO
     * @return ComparisonJob
     */
    public abstract ComparisonJob dtoToEntity(ComparisonJobDTO dto);

}
