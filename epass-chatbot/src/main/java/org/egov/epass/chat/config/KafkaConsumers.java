package org.egov.epass.chat.config;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.egov.epass.chat.model.Sms;
import org.egov.epass.chat.service.KarixSendSMSService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;

@Slf4j
@Configuration
public class KafkaConsumers {

    @Autowired
    private KarixSendSMSService karixSendSMSService;

    @KafkaListener(topics = "epass-karix-send-sms", id = "karix-send-sms-consumer")
    public void sendSms(ConsumerRecord<String, JsonNode> consumerRecord) {
        JsonNode data = consumerRecord.value();
        Sms sms = Sms.builder().mobileNumber(data.get("mobileNumber").asText()).text(data.get("text").asText()).build();
        karixSendSMSService.sendSMS(sms);
    }

}
