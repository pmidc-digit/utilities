package org.egov.epass.chat.config;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;

@Slf4j
@Configuration
public class KafkaConsumers {

//    @KafkaListener(topics = "epass-karix-send-sms", group = "epass-karix-send-sms")
//    public void sendSms(ConsumerRecord<String, JsonNode> consumerRecord) {
//        log.info("Send SMS Record : Key - " + consumerRecord.key() + ", Value - " + consumerRecord.value().toString());
//    }

}
