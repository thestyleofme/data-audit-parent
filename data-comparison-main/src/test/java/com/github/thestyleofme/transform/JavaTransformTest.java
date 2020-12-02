package com.github.thestyleofme.transform;

import java.util.*;

import com.github.thestyleofme.data.comparison.infra.handler.transform.java.DataSelector;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.util.CollectionUtils;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/12/02 16:20
 * @since 1.0.0
 */
@Slf4j
public class JavaTransformTest {

    @Test
    public void selectTest() {
        List<String> paramName = Arrays.asList("id", "name", "sex", "phone", "address", "education", "state");
        DataSelector.Result result = DataSelector.init(paramName)
                .addMain(list1())
                .addSub(list2())
                .select();
        Assert.assertFalse(CollectionUtils.isEmpty(result.getDiffList()));
    }

    private List<Map<String, Object>> list1() {
        List<Map<String, Object>> list1 = new ArrayList<>();

        Map<String, Object> map = new HashMap<>(8);
        map.put("id", "1");
        map.put("name", "name1");
        map.put("sex", "sex1");
        map.put("phone", "phone1");
        map.put("address", "address1");
        map.put("education", "education1");
        map.put("state", "未归档1");
        list1.add(map);

        map = new HashMap<>(8);
        map.put("id", "2");
        map.put("name", "name2");
        map.put("sex", "sex2");
        map.put("phone", "phone2");
        map.put("address", "address2");
        map.put("education", "education2");
        map.put("state", "未归档2");
        list1.add(map);

        map = new HashMap<>(8);
        map.put("id", "3");
        map.put("name", "name3");
        map.put("sex", "sex3");
        map.put("phone", "phone3");
        map.put("address", "address3");
        map.put("education", "education3");
        map.put("state", "未归档3");
        list1.add(map);
        return list1;
    }

    private List<Map<String, Object>> list2() {
        List<Map<String, Object>> list2 = new ArrayList<>();
        Map<String, Object> map = new HashMap<>(8);
        map.put("id", "4");
        map.put("name", "name4");
        map.put("sex", "sex4");
        map.put("phone", "phone4");
        map.put("address", "address4");
        map.put("education", "education4");
        map.put("state", "未归档4");
        list2.add(map);

        map = new HashMap<>(8);
        map.put("id", "2");
        map.put("name", "name22");
        map.put("sex", "sex2");
        map.put("phone", "phone2");
        map.put("address", "address2");
        map.put("education", "education2");
        map.put("state", "未归档2");
        list2.add(map);

        map = new HashMap<>(8);
        map.put("id", "3");
        map.put("name", "name3");
        map.put("sex", "sex3");
        map.put("phone", "phone3");
        map.put("address", "address3");
        map.put("education", "education3333");
        map.put("state", "未归档3");
        list2.add(map);
        return list2;
    }

}
