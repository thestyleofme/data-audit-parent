package com.github.thestyleofme.data.comparison.infra.handler.transform.java;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.github.thestyleofme.comparison.common.app.service.transform.BaseTransformHandler;
import com.github.thestyleofme.comparison.common.app.service.transform.HandlerResult;
import com.github.thestyleofme.comparison.common.app.service.transform.TableDataHandler;
import com.github.thestyleofme.comparison.common.domain.ColMapping;
import com.github.thestyleofme.comparison.common.domain.SourceDataMapping;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.infra.annotation.TransformType;
import com.github.thestyleofme.comparison.common.infra.constants.RowTypeEnum;
import com.github.thestyleofme.comparison.common.infra.utils.CommonUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/12/02 14:50
 * @since 1.0.0
 */
@Component
@Slf4j
@TransformType(value = "JAVA")
public class JavaTransformHandler implements BaseTransformHandler {

    private final TableDataHandler tableDataHandler;

    public JavaTransformHandler(TableDataHandler tableDataHandler) {
        this.tableDataHandler = tableDataHandler;
    }

    @Override
    public void handle(ComparisonJob comparisonJob,
                       Map<String, Object> env,
                       Map<String, Object> preTransform,
                       Map<String, Object> transformMap,
                       HandlerResult handlerResult) {
        List<ColMapping> colMappingList = CommonUtil.getColMappingList(comparisonJob);
        List<String> sourceParamNameList = colMappingList.stream()
                .filter(ColMapping::isSelected)
                .map(ColMapping::getSourceCol)
                .collect(Collectors.toList());
        SourceDataMapping sourceDataMapping = tableDataHandler.handle(comparisonJob, env);
        DataSelector.Result result = DataSelector.init(sourceParamNameList)
                .addMain(sourceDataMapping.getSourceDataList())
                .addSub(sourceDataMapping.getTargetDataList())
                .select();
        Map<String, List<DataSelector.Result.Diff>> groupList = result.getDiffList()
                .stream()
                .collect(Collectors.groupingBy(DataSelector.Result.Diff::getType));
        handlerResult.setSourceUniqueDataList(groupList.get(RowTypeEnum.INSERT.name()).stream().
                map(DataSelector.Result.Diff::getData)
                .collect(Collectors.toList()));
        handlerResult.setTargetUniqueDataList(groupList.get(RowTypeEnum.DELETED.name()).stream().
                map(DataSelector.Result.Diff::getData)
                .collect(Collectors.toList()));
        handlerResult.setDifferentDataList(groupList.get(RowTypeEnum.DIFFERENT.name()).stream().
                map(DataSelector.Result.Diff::getData)
                .collect(Collectors.toList()));
    }
}
