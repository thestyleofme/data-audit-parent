package com.github.thestyleofme.data.comparison.api.controller.v1;

import com.github.thestyleofme.data.comparison.app.service.DemoService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/19 11:08
 * @since 1.0.0
 */
@RestController("demoController.v1")
@RequestMapping("/v1/demo")
@Slf4j
public class DemoController {

    private final DemoService demoService;

    public DemoController(DemoService demoService) {
        this.demoService = demoService;
    }

    @GetMapping
    public void demo() {
        demoService.demo();
    }
}
