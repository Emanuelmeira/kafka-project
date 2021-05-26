package com.emanuel.kafka.service;

import com.emanuel.kafka.dto.ExampleDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Service
public class KafkaProduceService {

    @Value("${topic.name}")
    private String topicName;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    public void getMetrics() {
        kafkaTemplate.metrics().forEach( (x, y) -> System.out.println(x + " __ " +y));
    }

    public void sendSimpleMessage(ExampleDTO msg) {
        kafkaTemplate.send(topicName, msg);
    }

    //Enviando com key, mensagens com key sempre caem na mesma partição, simulando um FIFO
    public void sendMessageWithKey(String key, ExampleDTO msg) {
        kafkaTemplate.send(topicName, key, msg);
    }

    //Enviando para uma partição especifica
    public void sendMessageToSpecificPartition(int partition, String key, ExampleDTO msg) {
        kafkaTemplate.send(topicName,partition, key, msg);
    }

    //Enviando e esperando confirmação de que foi gravado na partição
    public void sendMessageWithCallBack(ExampleDTO msg) {

        ListenableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topicName, msg);

        future.addCallback(new ListenableFutureCallback<>() {

            @Override
            public void onSuccess(SendResult<String, Object> result) {
                System.out.println("Sent message=[" + msg + "] with offset=[" + result.getRecordMetadata().offset() + "]");
            }

            @Override
            public void onFailure(Throwable ex) {
                System.out.println("Unable to send message=[" + msg + "] due to : " + ex.getMessage());
            }
        });
    }

}
