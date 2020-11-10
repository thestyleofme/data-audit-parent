package com.github.thestyleofme.presto.api.controller.v1;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.core.metadata.OrderItem;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.github.thestyleofme.presto.api.dto.ClusterDTO;
import com.github.thestyleofme.presto.app.service.ClusterService;
import com.github.thestyleofme.presto.domain.entity.Cluster;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/09 14:34
 * @since 1.0.0
 */
@RestController("prestoClusterController.v1")
@RequestMapping("/v1/{organizationId}/presto-cluster")
@Slf4j
public class ClusterController {

    private final ClusterService clusterService;

    public ClusterController(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @ApiOperation(value = "查询presto集群")
    @GetMapping
    public ResponseEntity<IPage<ClusterDTO>> list(@PathVariable(name = "organizationId") Long tenantId,
                                                  Page<Cluster> page,
                                                  ClusterDTO clusterDTO) {
        clusterDTO.setTenantId(tenantId);
        page.addOrder(OrderItem.desc(Cluster.FIELD_ID));
        return ResponseEntity.ok(clusterService.list(page, clusterDTO));
    }

    @ApiOperation(value = "保存presto集群")
    @PostMapping
    public ResponseEntity<ClusterDTO> save(@PathVariable(name = "organizationId") Long tenantId,
                                           ClusterDTO clusterDTO) {
        clusterDTO.setTenantId(tenantId);
        return ResponseEntity.ok(clusterService.save(clusterDTO));
    }

    @ApiOperation(value = "删除presto集群")
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable(name = "organizationId") Long tenantId,
                                             @PathVariable Long id) {
        clusterService.removeById(id);
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }

}
