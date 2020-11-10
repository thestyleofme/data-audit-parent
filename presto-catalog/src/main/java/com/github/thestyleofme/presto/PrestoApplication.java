package com.github.thestyleofme.presto;

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
 * @author isaac 2020/11/09 14:38
 * @since 1.0.0
 */
@SpringBootApplication
@MapperScan({
        "com.github.thestyleofme.**.mapper"
})
public class PrestoApplication {

    private static final Logger logger = LoggerFactory.getLogger(PrestoApplication.class);

    public static void main(String[] args) {
        try {
            SpringApplication.run(PrestoApplication.class);
        } catch (Exception e) {
            logger.error("start error", e);
        }
    }
}
