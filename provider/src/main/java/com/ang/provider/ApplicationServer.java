package com.ang.provider;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportResource;

@ImportResource("classpath:appcontext-*.xml")
@SpringBootApplication
public class ApplicationServer {

	public static void main(String... args) {
		System.out.println("服务启动中");
		SpringApplication.run(ApplicationServer.class, args);
		System.out.println("服务启动完成");
	}
}
