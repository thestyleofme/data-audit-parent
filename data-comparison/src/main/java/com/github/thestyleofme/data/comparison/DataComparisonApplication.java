package com.github.thestyleofme.data.comparison;

import org.mybatis.spring.annotation.MapperScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/19 10:57
 * @since 1.0.0
 */
@SpringBootApplication
@MapperScan({
        "com.github.thestyleofme.data.**.mapper"
})
public class DataComparisonApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataComparisonApplication.class);

    public static void main(String[] args) {
        try {
            SpringApplication.run(DataComparisonApplication.class, args);
        } catch (Exception e) {
            LOGGER.error("application start error", e);
        }
    }
}
