package com.github.thestyleofme.data.comparison.infra.handler.transform.java;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.github.thestyleofme.comparison.common.app.service.transform.BaseTransformHandler;
import com.github.thestyleofme.comparison.common.app.service.transform.HandlerResult;
import com.github.thestyleofme.comparison.common.app.service.transform.TableDataHandler;
import com.github.thestyleofme.comparison.common.domain.AppConf;
import com.github.thestyleofme.comparison.common.domain.ColMapping;
import com.github.thestyleofme.comparison.common.domain.JobEnv;
import com.github.thestyleofme.comparison.common.domain.SourceDataMapping;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.infra.annotation.TransformType;
import com.github.thestyleofme.comparison.common.infra.constants.RowTypeEnum;
import com.github.thestyleofme.comparison.common.infra.utils.CommonUtil;
import com.github.thestyleofme.comparison.common.infra.utils.HandlerUtil;
import com.github.thestyleofme.plugin.core.infra.utils.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

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
                       Map<String, Object> transformMap,
                       HandlerResult handlerResult) {
        LocalDateTime startTime = LocalDateTime.now();
        AppConf appConf = JsonUtil.toObj(comparisonJob.getAppConf(), AppConf.class);
        JobEnv jobEnv = JsonUtil.toObj(JsonUtil.toJson(appConf.getEnv()), JobEnv.class);
        List<ColMapping> colMappingList = CommonUtil.getColMappingList(jobEnv);
        List<Map<String, Object>> indexMapping = jobEnv.getIndexMapping();
        List<String> paramNameList = colMappingList.stream()
                // indexMapping列默认被选择去做比对
                .filter(colMapping -> {
                    boolean anyMatch = indexMapping.stream()
                            .anyMatch(map -> map.containsValue(colMapping.getSourceCol()) ||
                                    map.containsValue(colMapping.getTargetCol()));
                    return colMapping.isSelected() || anyMatch;
                })
                .map(colMapping ->
                        colMapping.getSourceCol().equals(colMapping.getTargetCol()) ? colMapping.getSourceCol() :
                                String.format("%s -> %s", colMapping.getSourceCol(), colMapping.getTargetCol()))
                .collect(Collectors.toList());
        LocalDateTime preSelectTime = LocalDateTime.now();

        SourceDataMapping sourceDataMapping = tableDataHandler.handle(comparisonJob, env);
        LocalDateTime endSelectTime = LocalDateTime.now();
        DataSelector.Result result = DataSelector.init(paramNameList)
                .addMain(sourceDataMapping.getSourceDataList())
                .addSub(sourceDataMapping.getTargetDataList())
                .select(indexMapping);
        // 填充HandlerResult值
        genHandlerResult(handlerResult, result);
        LocalDateTime endTime = LocalDateTime.now();
        log.info("execute all time:{},select data time:{},transform time:{}",
                HandlerUtil.timestamp2String(Duration.between(startTime, endTime).toMillis()),
                HandlerUtil.timestamp2String(Duration.between(preSelectTime, endSelectTime).toMillis()),
                HandlerUtil.timestamp2String(Duration.between(endSelectTime, endTime).toMillis()));
    }

    private void genHandlerResult(HandlerResult handlerResult, DataSelector.Result result) {
        Map<String, List<DataSelector.Result.Diff>> groupList = result.getDiffList()
                .stream()
                .collect(Collectors.groupingBy(DataSelector.Result.Diff::getType));
        List<DataSelector.Result.Diff> insert = groupList.get(RowTypeEnum.INSERT.name());
        if (!CollectionUtils.isEmpty(insert)) {
            handlerResult.setSourceUniqueDataList(insert.stream().
                    map(DataSelector.Result.Diff::getData)
                    .collect(Collectors.toList()));
        }
        List<DataSelector.Result.Diff> delete = groupList.get(RowTypeEnum.DELETED.name());
        if (!CollectionUtils.isEmpty(delete)) {
            handlerResult.setTargetUniqueDataList(delete.stream().
                    map(DataSelector.Result.Diff::getData)
                    .collect(Collectors.toList()));
        }
        List<DataSelector.Result.Diff> update = groupList.get(RowTypeEnum.UPDATED.name());
        if (!CollectionUtils.isEmpty(update)) {
            handlerResult.setPkOrIndexSameDataList(update.stream().
                    map(DataSelector.Result.Diff::getData)
                    .collect(Collectors.toList()));
        }
        List<DataSelector.Result.Diff> different = groupList.get(RowTypeEnum.DIFFERENT.name());
        if (!CollectionUtils.isEmpty(different)) {
            handlerResult.setDifferentDataList(different.stream().
                    map(DataSelector.Result.Diff::getData)
                    .collect(Collectors.toList()));
        }
    }
}
