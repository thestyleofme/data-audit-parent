package com.github.thestyleofme.data.comparison.api.controller.v1;

import javax.servlet.http.HttpServletResponse;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.core.metadata.OrderItem;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.github.thestyleofme.comparison.common.domain.DeployInfo;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.domain.entity.Reader;
import com.github.thestyleofme.data.comparison.api.dto.ComparisonJobDTO;
import com.github.thestyleofme.data.comparison.app.service.ComparisonJobService;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/22 11:08
 * @since 1.0.0
 */
@RestController("comparisonJobController.v1")
@RequestMapping("/v1/{organizationId}/comparison-job")
@Slf4j
public class ComparisonJobController {

    private final ComparisonJobService comparisonJobService;

    public ComparisonJobController(ComparisonJobService comparisonJobService) {
        this.comparisonJobService = comparisonJobService;
    }

    @ApiOperation(value = "查询数据稽核job")
    @GetMapping
    public ResponseEntity<IPage<ComparisonJobDTO>> list(@PathVariable(name = "organizationId") Long tenantId,
                                                        Page<ComparisonJob> page,
                                                        ComparisonJobDTO comparisonJobDTO) {
        comparisonJobDTO.setTenantId(tenantId);
        page.addOrder(OrderItem.desc(ComparisonJob.FIELD_ID));
        return ResponseEntity.ok(comparisonJobService.list(page, comparisonJobDTO));
    }

    @ApiOperation(value = "执行数据稽核任务")
    @GetMapping("/execute")
    public ResponseEntity<Void> execute(@PathVariable(name = "organizationId") Long tenantId,
                                        @RequestParam(value = "jobCode", required = false) String jobCode,
                                        @RequestParam(value = "groupCode", required = false) String groupCode) {
        comparisonJobService.execute(tenantId, jobCode, groupCode);
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }

    @ApiOperation(value = "数据补偿")
    @GetMapping("/deploy")
    public ResponseEntity<Void> deploy(@PathVariable(name = "organizationId") Long tenantId,
                                       DeployInfo deployInfo) {
        deployInfo.setTenantId(tenantId);
        comparisonJobService.deploy(deployInfo);
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }

    @ApiOperation(value = "生成datax reader")
    @PostMapping("/datax-reader")
    public ResponseEntity<Reader> getDataxReader(@PathVariable(name = "organizationId") Long tenantId,
                                                 @RequestBody ComparisonJob comparisonJob,
                                                 @RequestParam(value = "syncType", required = false, defaultValue = "0") Integer syncType) {
        Reader reader = comparisonJobService.getDataxReader(tenantId, comparisonJob, syncType);
        return ResponseEntity.ok(reader);
    }

    @ApiOperation(value = "下载稽核结果文件", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    @GetMapping(value = "/download")
    public void getDataxReader(@PathVariable(name = "organizationId") Long tenantId,
                               @RequestParam(value = "jobId") Long jobId,
                               HttpServletResponse response) {
        comparisonJobService.download(tenantId, jobId, response);
    }

    @ApiOperation(value = "保存数据稽核任务")
    @PostMapping
    public ResponseEntity<ComparisonJobDTO> save(@PathVariable(name = "organizationId") Long tenantId,
                                                 @RequestBody ComparisonJobDTO comparisonJobDTO) {
        comparisonJobDTO.setTenantId(tenantId);
        return ResponseEntity.ok(comparisonJobService.save(comparisonJobDTO));
    }

    @ApiOperation(value = "删除数据稽核任务")
    @DeleteMapping("/{jobId}")
    public ResponseEntity<Void> delete(@PathVariable(name = "organizationId") Long tenantId,
                                       @PathVariable Long jobId) {
        comparisonJobService.removeById(jobId);
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }

}
