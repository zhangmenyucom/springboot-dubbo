package com.ang.provider;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ImportResource;

/**
 * @author taylor
 */
@ImportResource("classpath:appcontext-*.xml")
@SpringBootApplication
@ComponentScan(basePackages = "com.ang")
public class ProviderServer {

    public static void main(String... args) {
        System.out.println("服务启动中");
        SpringApplication.run(ProviderServer.class, args);
        System.out.println("服务启动完成");
    }
}
