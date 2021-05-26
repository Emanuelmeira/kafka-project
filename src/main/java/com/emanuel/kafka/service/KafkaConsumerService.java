package com.emanuel.kafka.service;


import com.emanuel.kafka.dto.ExampleDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@KafkaListener(topics = "${topic.name}", groupId = "${spring.kafka.consumer.group-id}")
public class KafkaConsumerService {

    // O Spring procura o metodo mais adequado para recebar a mensagem, caso não encontre, é chamado o metodo que contem (isDefault = true)

    @KafkaHandler
    public void consumer(@Payload ExampleDTO msg,
                         @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                         @Header(KafkaHeaders.OFFSET) int offset,
                         Acknowledgment ack) { // usado alguns parametros para obter dados da msg

        log.info("Received message [{}] from partition-{} with offset-{}", msg, partition, offset);

        // Para sistemas onde cada mensagem é importante, o commit manual APOS processamento da mensagem se faz necessario
        // Para sistemas onde algumas mensagens podem ser perdidas (Ex: localização de carro em uber) não e necessario


        // Commmit manual, que também será síncrono
        ack.acknowledge();
    }

    @KafkaHandler(isDefault = true)
    public void consumer(@Payload String msg) {
        log.info("Information: " + msg);
    }

//    @KafkaListener(topics = "${topic.name}", groupId = "${spring.kafka.consumer.group-id}")
//    public void consumer(final ConsumerRecord consumerRecord) {
//        log.info("key: " + consumerRecord.key());
//        log.info("Headers: " + consumerRecord.headers());
//        log.info("Partition: " + consumerRecord.partition());
//        log.info("Class: " + consumerRecord.value());
//    }
}
