package com.emanuel.kafka.controller;

import com.emanuel.kafka.dto.ExampleDTO;
import com.emanuel.kafka.service.KafkaProduceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/producer")
public class KafkaController {

    @Autowired
    private KafkaProduceService kafkaProduceService;

    @GetMapping
    public void producerKafkaMessage(@RequestParam(value = "operation", defaultValue = "") String operation,
                                     @RequestParam(value = "quantity", defaultValue = "500") int quantity,
                                     @RequestParam(value = "partition", defaultValue = "1") int partition){

        for(int i=0; i<quantity; i++){

            var key = "key";
            var example = new ExampleDTO();
            example.setInformation(UUID.randomUUID().toString());
            example.setValue(i);

            if(operation.equals("")){
                kafkaProduceService.sendSimpleMessage(example);
            }else if(operation.equals("1") ){
                kafkaProduceService.sendMessageWithKey(key, example);
            }else if(operation.equals("2") ){
                kafkaProduceService.sendMessageWithCallBack(example);
            }else if(operation.equals("3") ){
                kafkaProduceService.sendMessageToSpecificPartition(partition, key, example);
            }

        }
    }



}
