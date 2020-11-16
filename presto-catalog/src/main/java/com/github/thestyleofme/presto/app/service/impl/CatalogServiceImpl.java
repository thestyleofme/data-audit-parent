package com.github.thestyleofme.presto.app.service.impl;

import java.util.Optional;
import java.util.stream.Collectors;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.github.thestyleofme.presto.api.dto.CatalogDTO;
import com.github.thestyleofme.presto.app.service.CatalogService;
import com.github.thestyleofme.presto.app.service.ClusterService;
import com.github.thestyleofme.presto.domain.entity.Catalog;
import com.github.thestyleofme.presto.domain.entity.Cluster;
import com.github.thestyleofme.presto.infra.converter.BaseCatalogConvert;
import com.github.thestyleofme.presto.infra.exceptions.CatalogException;
import com.github.thestyleofme.presto.infra.mapper.CatalogMapper;
import com.github.thestyleofme.presto.infra.utils.CommonUtil;
import com.github.thestyleofme.presto.infra.utils.RestTemplateUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

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
public class CatalogServiceImpl extends ServiceImpl<CatalogMapper, Catalog> implements CatalogService {

    private final ClusterService clusterService;
    private final RestTemplate restTemplate;

    public CatalogServiceImpl(ClusterService clusterService, RestTemplate restTemplate) {
        this.clusterService = clusterService;
        this.restTemplate = restTemplate;
    }

    @Override
    public IPage<CatalogDTO> list(Page<Catalog> page, CatalogDTO catalogDTO) {
        QueryWrapper<Catalog> queryWrapper = new QueryWrapper<>(
                BaseCatalogConvert.INSTANCE.dtoToEntity(catalogDTO));
        Page<Catalog> entityPage = page(page, queryWrapper);
        final Page<CatalogDTO> dtoPage = new Page<>();
        BeanUtils.copyProperties(entityPage, dtoPage);
        dtoPage.setRecords(entityPage.getRecords().stream()
                .map(BaseCatalogConvert.INSTANCE::entityToDTO)
                .collect(Collectors.toList()));
        return dtoPage;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public CatalogDTO save(CatalogDTO catalogDTO) {
        Cluster cluster = Optional.ofNullable(clusterService.getOne(new QueryWrapper<>(Cluster.builder()
                .clusterCode(catalogDTO.getClusterCode())
                .tenantId(catalogDTO.getTenantId()).build())))
                .orElseThrow(() -> new IllegalArgumentException(String.format("not found cluster: [%s]",
                        catalogDTO.getClusterCode())));
        // 插表或更新表
        Catalog catalog = BaseCatalogConvert.INSTANCE.dtoToEntity(catalogDTO);
        this.saveOrUpdate(catalog);
        // presto保存catalog
        String url = cluster.getCoordinatorUrl();
        CommonUtil.genCatalogData(catalog);
        HttpEntity<String> requestEntity = new HttpEntity<>(CommonUtil.genCatalogData(catalog),
                RestTemplateUtil.applicationJsonHeaders());
        ResponseEntity<String> responseEntity = restTemplate.postForEntity(url + "/v1/catalog",
                requestEntity, String.class);
        if (!responseEntity.getStatusCode().is2xxSuccessful()) {
            throw new CatalogException("presto save catalog error, " + responseEntity.getBody());
        }
        return BaseCatalogConvert.INSTANCE.entityToDTO(catalog);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void delete(Long tenantId, String clusterCode, String catalogName) {
        Cluster cluster = Optional.ofNullable(clusterService.getOne(new QueryWrapper<>(Cluster.builder()
                .clusterCode(clusterCode)
                .tenantId(tenantId).build())))
                .orElseThrow(() -> new IllegalArgumentException(String.format("not found cluster: [%s]",
                        clusterCode)));
        // 删表
        this.remove(new QueryWrapper<>(Catalog.builder()
                .clusterCode(clusterCode)
                .catalogName(catalogName)
                .tenantId(tenantId).build()));
        // presto删除catalog
        MultiValueMap<String, Object> body = new LinkedMultiValueMap<>(1);
        HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(body);
        ResponseEntity<String> responseEntity = restTemplate.exchange(cluster.getCoordinatorUrl() + "/v1/catalog/{1}",
                HttpMethod.DELETE, requestEntity, String.class, catalogName);
        if (!responseEntity.getStatusCode().is2xxSuccessful()) {
            throw new CatalogException("presto delete catalog error, " + responseEntity.getBody());
        }
    }
}
