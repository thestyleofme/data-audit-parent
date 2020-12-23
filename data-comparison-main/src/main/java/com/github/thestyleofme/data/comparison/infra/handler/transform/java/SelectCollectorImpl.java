package com.github.thestyleofme.data.comparison.infra.handler.transform.java;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import com.github.thestyleofme.comparison.common.infra.constants.CommonConstant;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/12/02 11:22
 * @since 1.0.0
 */
@AllArgsConstructor
public class SelectCollectorImpl implements Collector<Map<String, Object>, SelectCollectorImpl.Result, SelectCollectorImpl.Result> {

    private final List<String> keys;
    private final List<String> indexList;
    private final boolean flag;

    @Override
    public Supplier<Result> supplier() {
        return Result::new;
    }

    @Override
    public BiConsumer<Result, Map<String, Object>> accumulator() {
        return (result, map) -> result.addValue(map, this.keys, this.indexList, this.flag);
    }

    @Override
    public BinaryOperator<Result> combiner() {
        return Result::merge;
    }

    @Override
    public Function<Result, Result> finisher() {
        return Function.identity();
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Collections.unmodifiableSet(EnumSet.of(Collector.Characteristics.IDENTITY_FINISH));
    }

    @Getter
    @Setter(value = AccessLevel.PRIVATE)
    static class Result {
        private Set<String> valueSet = new HashSet<>();
        private List<Map<String, Object>> indexValueList = new LinkedList<>();
        private Map<String, Set<Object>> valueMap = new HashMap<>();

        public String getFirstValue() {
            Optional<String> first = this.valueSet.stream().findFirst();
            return first.orElse(null);
        }

        private void addValue(Map<String, Object> map, List<String> keys, List<String> indexList, boolean flag) {
            addValue(this, map, keys, indexList, flag);
        }

        public static Result init(Map<String, Object> map, List<String> keys, List<String> indexList, boolean flag) {
            Result result = new Result();
            addValue(result, map, keys, indexList, flag);
            return result;
        }

        private static void addValue(Result result, Map<String, Object> map,
                                     List<String> keys,
                                     List<String> indexList,
                                     boolean flag) {
            List<String> valueList = new ArrayList<>();
            Map<String, Object> indexMap = new HashMap<>();
            for (String key : keys) {
                String col = SelectorUtil.getKey(key, flag);
                Object value = map.get(col);
                if (indexList.stream().allMatch(s -> s.contains(col))) {
                    Optional<String> optional = indexList.stream()
                            .filter(s -> flag ? s.split(CommonConstant.ARROW_CONCAT)[0].equals(col) :
                                    s.split(CommonConstant.ARROW_CONCAT)[1].equals(col))
                            .findFirst();
                    optional.ifPresent(s -> indexMap.put(s, value));
                }
                valueList.add(String.valueOf(value));
                Set<Object> objectSet = result.getValueMap().get(key);
                if (objectSet == null) {
                    objectSet = new TreeSet<>();
                }
                objectSet.add(String.valueOf(value));
                result.getValueMap().put(key, objectSet);
            }
            String valueStr = String.join("-", valueList);
            result.getIndexValueList().add(indexMap);
            result.getValueSet().add(valueStr);
        }

        private Result merge(Result result) {
            this.getValueSet().addAll(result.getValueSet());

            for (Map.Entry<String, Set<Object>> entry : result.getValueMap().entrySet()) {
                String key = entry.getKey();
                Set<Object> set = this.valueMap.get(key);
                if (set == null) {
                    set = new TreeSet<>();
                }
                set.addAll(entry.getValue());
                this.valueMap.put(key, set);
            }
            return this;
        }
    }
}
