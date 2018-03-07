package com.ang.consume;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportResource;

/**
 * @author taylor
 */
@ImportResource("classpath:appcontext-*.xml")
@SpringBootApplication
public class ConsumerServer {

	public static void main(String... args) {
		
		System.out.println("服务启动中");
		SpringApplication.run(ConsumerServer.class, args);
		System.out.println("服务启动完成");
	
	}
}
