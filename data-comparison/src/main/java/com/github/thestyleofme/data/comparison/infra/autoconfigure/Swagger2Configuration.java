package com.github.thestyleofme.data.comparison.infra.autoconfigure;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.service.Contact;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/19 11:26
 * @since 1.0.0
 */
@Configuration("dataComparisonSwagger2Configuration")
@EnableSwagger2
public class Swagger2Configuration {

    @Bean
    public Docket createRestApi() {
        return new Docket(DocumentationType.SWAGGER_2)
                .groupName("data_comparison")
                .apiInfo(this.apiInfo())
                .select()
                .apis(RequestHandlerSelectors.any())
                .paths(PathSelectors.any())
                .build();
    }

    private ApiInfo apiInfo() {
        return new ApiInfoBuilder()
                .title("Basic Data Comparison Restful APIs")
                .description("数据稽核")
                .termsOfServiceUrl("http://127.0.0.1")
                .contact(new Contact("阿骚", "", ""))
                .version("1.0.0")
                .build();
    }

}
