package com.emanuel.kafka.config;

import com.emanuel.kafka.dto.ExampleDTO;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String group;

    @Value("${spring.kafka.consumer.auto-offset-reset}")
    private String offsetReset;

    @Bean
    public ConsumerFactory<String, ExampleDTO> consumerFactory() {

        return new DefaultKafkaConsumerFactory<>(consumerProps(),
                new StringDeserializer(),
                new JsonDeserializer<>(ExampleDTO.class));
    }

    //Consumindo as mensagem multi-threaded
    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, ExampleDTO>> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, ExampleDTO> listener =
                new ConcurrentKafkaListenerContainerFactory<>();
        listener.setConsumerFactory(consumerFactory());

        // Não falhar, caso ainda não existam os tópicos para consumo
        listener.getContainerProperties()
                .setMissingTopicsFatal(false);

        // ### AQUI
        // Commit manual do offset
        listener.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        // ### AQUI
        // Commits síncronos
        listener.getContainerProperties().setSyncCommits(Boolean.TRUE);
        return listener;
    }

    private Map<String, Object> consumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset);
        // ...
        return props;
    }

}
