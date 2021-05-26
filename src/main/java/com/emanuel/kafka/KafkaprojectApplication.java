package com.emanuel.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;

@SpringBootApplication
public class KafkaprojectApplication {

	@Value("${topic.name}")
	private String topicName;

	public static void main(String[] args) {
		SpringApplication.run(KafkaprojectApplication.class, args);

	}

	@Bean
	public NewTopic topic() {
		return TopicBuilder.name(topicName)
				.partitions(3)
				.replicas(3)
				.build();
	}
}
