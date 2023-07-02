package com.homewin.export;

import jakarta.annotation.Resource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class ExportApplicationTests {
    @Resource
    ExportInvoker<SopRelativesLoyalCustomers> invoker;
    @Test
    void contextLoads() {
        invoker.setData(new SopRelativesLoyalCustomers());
        invoker.export(sopRelativesLoyalCustomersService, "20230630002导出100W.xlsx", null, 1000000, "20230630002");
    }

}
