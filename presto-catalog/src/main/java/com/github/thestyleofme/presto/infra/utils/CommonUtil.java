package com.github.thestyleofme.presto.infra.utils;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.github.thestyleofme.presto.domain.entity.Catalog;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/10 15:52
 * @since 1.0.0
 */
public class CommonUtil {

    private CommonUtil() {

    }

    public static String genCatalogData(Catalog catalog) {
        Map<String, Object> map = new HashMap<>(8);
        map.put("catalogName", catalog.getCatalogName());
        map.put("connectorName", catalog.getConnectorName());
        map.put("properties", JsonUtil.toObj(catalog.getProperties(), new TypeReference<Map<String, String>>() {
        }));
        return JsonUtil.toJson(map);
    }

}
