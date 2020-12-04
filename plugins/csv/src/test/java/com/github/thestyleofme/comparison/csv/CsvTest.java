package com.github.thestyleofme.comparison.csv;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.github.thestyleofme.comparison.common.app.service.transform.HandlerResult;
import com.github.thestyleofme.comparison.common.domain.entity.Reader;
import com.github.thestyleofme.comparison.csv.pojo.DataxCsvReader;
import com.github.thestyleofme.plugin.core.infra.utils.JsonUtil;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;
import com.opencsv.exceptions.CsvException;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.springframework.util.StringUtils;

/**
 * <p></p>
 *
 * @author hsq 2020/12/02 11:49
 * @since 1.0.0
 */
@Slf4j
public class CsvTest {

    @Test
    public void writerTest() throws IOException {
        HandlerResult handlerResult = new HandlerResult();
        List<Map<String, Object>> list = handlerResult.getPkOrIndexSameDataList();
        List<String[]> csvList = list.stream()
                .map(map -> map.values().stream()
                        .map(o -> o == null ? "" : String.valueOf(o))
                        .toArray(String[]::new))
                .collect(Collectors.toList());

        CSVWriterBuilder builder = new CSVWriterBuilder(new FileWriter("src/test/resources/test.csv"));
        ICSVWriter icsvWriter = builder.withSeparator(',').build();
        String[] strings = {"id1", "name1", "test1"};
        csvList.add(strings);
        icsvWriter.writeAll(csvList);
        icsvWriter.close();
        assertTrue(icsvWriter.checkError());
    }


    @Test
    public void readerTest() throws IOException, CsvException {
        CSVReaderBuilder builder = new CSVReaderBuilder(new FileReader("src/test/resources/test.csv"));
        CSVReader reader = builder.build();
        List<String[]> strings = reader.readAll();
        for (String[] col : strings) {
            System.out.println(Arrays.toString(col));
        }
        assertFalse(strings.isEmpty());
    }

    @Test
    public void csvReaderTest() {
        DataxCsvReader reader = new DataxCsvReader();
        reader.setName("testName");
        reader.setParameter(DataxCsvReader.Parameter.builder()
                .path(new String[]{"/data/hdsp/test.csv"})
                .encoding("utf-8")
                .fieldDelimiter("\u0001")
                .build());
        Reader testReader = reader;
        String json = JsonUtil.toJson(testReader);
        System.out.println(json);
        assertFalse(StringUtils.isEmpty(json));
    }
}
