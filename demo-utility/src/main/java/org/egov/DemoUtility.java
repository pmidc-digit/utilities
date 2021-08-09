package org.egov;


import org.egov.tracer.config.TracerConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;

@SpringBootApplication
@Configuration
@PropertySource("classpath:application.properties")
@Import({ TracerConfiguration.class })
public class DemoUtility
{	
	
	public static void main(String[] args) {
		
		SpringApplication.run(DemoUtility.class, args);
	}
	
}