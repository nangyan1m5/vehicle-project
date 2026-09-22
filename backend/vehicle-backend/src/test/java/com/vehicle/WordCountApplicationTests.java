package com.vehicle; // 关键：改为后端项目的核心包名（与启动类一致）

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

// 指定启动类，解决找不到 @SpringBootConfiguration 的问题
@SpringBootTest(classes = VehicleApplication.class)
public class WordCountApplicationTests {

    @Test
    public void contextLoads() {
        // 空测试，仅验证上下文加载
    }
}
