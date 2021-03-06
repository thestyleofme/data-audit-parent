package com.github.thestyleofme.comparison.common.infra.utils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.github.thestyleofme.comparison.common.domain.*;
import com.github.thestyleofme.comparison.common.domain.entity.ComparisonJob;
import com.github.thestyleofme.comparison.common.infra.constants.ErrorCode;
import com.github.thestyleofme.comparison.common.infra.exceptions.HandlerException;
import com.github.thestyleofme.plugin.core.infra.utils.BeanUtils;
import com.github.thestyleofme.plugin.core.infra.utils.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.util.StringUtils;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/23 15:30
 * @since 1.0.0
 */
@Slf4j
public class CommonUtil {

    private CommonUtil() {
    }

    @SuppressWarnings("unchecked")
    public static <T> T createProxy(ClassLoader classLoader,
                                    Class<T> clazz,
                                    InvocationHandler invocationHandler) {
        return (T) Proxy.newProxyInstance(
                classLoader,
                new Class[]{clazz},
                invocationHandler);
    }

    public static <T> T requireNonNullElse(T obj, T defaultObj) {
        return (obj != null) ? obj : Optional.ofNullable(defaultObj)
                .orElseThrow(() -> new HandlerException(ErrorCode.BOTH_PROPERTIES_IS_NULL));
    }

    @SafeVarargs
    public static <T> void requireAllNonNullElseThrow(String errorMsg, T... objs) {
        boolean allMatch = Stream.of(objs).allMatch(Objects::isNull);
        if (allMatch) {
            throw new HandlerException(errorMsg);
        }
    }

    /**
     * <p>
     * 根据大小获得每个批次的数量
     * </p>
     *
     * @author isaac 2020/10/27 16:06
     * @since 1.0.0
     */
    public static int calculateBatchSize(int listSize) {
        int defaultBatchSize = 1024;
        int pow = 10;
        int batchSize;
        if (listSize < defaultBatchSize * pow) {
            // 1024*10 -> 1万 1
            batchSize = defaultBatchSize;
        } else if (listSize < defaultBatchSize * pow * pow) {
            // 1024*100 -> 10万 8
            batchSize = listSize / 8;
        } else if (listSize < defaultBatchSize * pow * pow * pow) {
            // 1024*1000 -> 100万 16
            batchSize = listSize / 16;
        } else if (listSize < defaultBatchSize * pow * pow * pow * pow) {
            // 1024*10000 -> 1000万 32
            batchSize = listSize / 32;
        } else {
            batchSize = listSize / 64;
        }
        return batchSize;
    }

    /**
     * 分批
     *
     * @param list      等分批的集合
     * @param batchSize 每个批次的数量
     * @return List
     * @author isacc 2020/10/27 16:06
     */
    public static <T> List<List<T>> splitList(List<T> list, int batchSize) {
        int listSize = list.size();
        List<List<T>> listArray = new ArrayList<>();
        int toIndex = batchSize;
        for (int i = 0; i < listSize; i += batchSize) {
            if (i + batchSize > listSize) {
                // toIndex最后没有batchSize条数据则list中有几条
                toIndex = listSize - i;
            }
            listArray.add(list.subList(i, i + toIndex));
        }
        return listArray;
    }

    public static void deleteFile(ComparisonJob comparisonJob, String path) {
        if (!StringUtils.isEmpty(path)) {
            String excelName = String.format("%s/%d_%s.xlsx", path, comparisonJob.getTenantId(), comparisonJob.getJobCode());
            deleteFile(excelName);
        }
    }

    public static void deleteFile(String path) {
        File file = new File(path);
        if (file.exists()) {
            try {
                Files.delete(file.toPath());
                log.debug("the file[{}] successfully deleted", path);
            } catch (IOException e) {
                throw new HandlerException(ErrorCode.EXCEL_DELETE_ERROR, path);
            }
        }
    }

    public static List<ColMapping> getColMappingList(ComparisonJob comparisonJob) {
        AppConf appConf = JsonUtil.toObj(comparisonJob.getAppConf(), AppConf.class);
        JobEnv jobEnv = JsonUtil.toObj(JsonUtil.toJson(appConf.getEnv()), JobEnv.class);
        return getColMappingList(jobEnv);
    }

    public static List<ColMapping> getColMappingList(JobEnv jobEnv) {
        List<Map<String, Object>> colMapping = jobEnv.getColMapping();
        return colMapping.stream()
                .map(map -> BeanUtils.map2Bean(map, ColMapping.class))
                .sorted(Comparator.comparingInt(ColMapping::getIndex))
                .collect(Collectors.toList());
    }


    public static List<ColMapping> getJoinMappingList(JobEnv jobEnv) {
        return jobEnv.getIndexMapping().stream()
                .map(map -> BeanUtils.map2Bean(map, ColMapping.class))
                .collect(Collectors.toList());
    }

    public static JobEnv getJobEnv(ComparisonJob comparisonJob) {
        AppConf appConf = JsonUtil.toObj(comparisonJob.getAppConf(), AppConf.class);
        return JsonUtil.toObj(JsonUtil.toJson(appConf.getEnv()), JobEnv.class);
    }

    public static void completableFutureAllOf(List<CompletableFuture<?>> completableFutureList) {
        CompletableFuture<Void> future = CompletableFuture.allOf(completableFutureList.toArray(new CompletableFuture[0]));
        try {
            future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new HandlerException(e);
        } catch (
                ExecutionException e) {
            throw new HandlerException(e);
        }
    }

    /**
     * 在项目路径下创建文件夹
     *
     * @param dirEndName 文件夹名称
     * @return 文件夹绝对路径
     */
    public static String createDirPath(String dirEndName) {
        File file = new File(dirEndName);
        if (!file.exists()) {
            try {
                FileUtils.forceMkdir(file);
            } catch (IOException e) {
                // ignore
            }
        }
        return file.getAbsolutePath();
    }

    public static ComparisonInfo getComparisonInfo(Map<String, Object> env) {
        String envJson = JsonUtil.toJson(env);
        ComparisonInfo comparisonInfo = JsonUtil.toObj(envJson, ComparisonInfo.class);

        // 构建表的完整名字
        comparisonInfo.setSourceTableName(getTableName(comparisonInfo.getSource()));
        comparisonInfo.setTargetTableName(getTableName(comparisonInfo.getTarget()));
        return comparisonInfo;
    }

    public static String getTableName(SelectTableInfo tableInfo) {
        String tableName;
        String catalog = tableInfo.getCatalog();
        String table = tableInfo.getTable();
        String schema = tableInfo.getSchema();
        if (StringUtils.isEmpty(catalog)) {
            tableName = String.format("%s.%s", schema, table);
        } else {
            tableName = String.format("%s.%s.%s", catalog, schema, table);
        }
        return tableName;
    }
}
