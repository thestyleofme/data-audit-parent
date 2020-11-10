package com.github.thestyleofme.presto.infra.autoconfiguration;

import java.nio.charset.StandardCharsets;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.web.client.RestTemplate;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/11/10 15:47
 * @since 1.0.0
 */
@Configuration
public class RestTemplateConfiguration {

    @Bean
    public RestTemplate prestoRestTemplate() {
        RestTemplate restTemplate = new RestTemplate();
        // 这里string序列化使用utf8编码
        restTemplate.getMessageConverters().set(1, new StringHttpMessageConverter(StandardCharsets.UTF_8));
        return restTemplate;
    }
}
