package com.github.thestyleofme.presto.app.service;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.IService;
import com.github.thestyleofme.presto.api.dto.CatalogDTO;
import com.github.thestyleofme.presto.domain.entity.Catalog;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/09 14:35
 * @since 1.0.0
 */
public interface CatalogService extends IService<Catalog> {

    /**
     * 分页条件查询
     *
     * @param page       分页
     * @param catalogDTO CatalogDTO
     * @return IPage<CatalogDTO>
     */
    IPage<CatalogDTO> list(Page<Catalog> page, CatalogDTO catalogDTO);

    /**
     * 保存catalog
     *
     * @param catalogDTO CatalogDTO
     * @return CatalogDTO
     */
    CatalogDTO save(CatalogDTO catalogDTO);

    /**
     * 删除catalog
     * @param tenantId tenantId
     * @param clusterCode clusterCode
     * @param catalogName catalogName
     */
    void delete(Long tenantId, String clusterCode, String catalogName);
}
