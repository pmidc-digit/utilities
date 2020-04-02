package org.egov.epass.chat.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.egov.epass.chat.model.Sms;
import org.egov.epass.chat.service.ChatService;
import org.egov.epass.chat.service.EpassUpdatesListener;
import org.egov.epass.chat.smsprovider.KarixSendSMSService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;

import java.io.IOException;

@Slf4j
@Configuration
public class KafkaConsumers {

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private KarixSendSMSService karixSendSMSService;
    @Autowired
    private ChatService chatService;
    @Autowired
    private EpassUpdatesListener epassUpdatesListener;

    @KafkaListener(topics = "${send.message.topic}")
    public void sendSms(ConsumerRecord<String, String> consumerRecord) throws IOException {
        JsonNode data = objectMapper.readTree(consumerRecord.value());
        Sms sms = Sms.builder().mobileNumber(data.get("mobileNumber").asText()).text(data.get("text").asText()).build();
        karixSendSMSService.sendSMS(sms);
    }

    @KafkaListener(topics = "karix-received-messages")
    public void processMessage(ConsumerRecord<String, String> consumerRecord) throws IOException {
        JsonNode chatNode = objectMapper.readTree(consumerRecord.value());
        chatService.processMessage(consumerRecord.key(), chatNode);
    }

    @KafkaListener(topics = "${epass.notifications.topic}")
    public void processUpdates(ConsumerRecord<String, String> consumerRecord) throws IOException {
        JsonNode epassUpdate = objectMapper.readTree(consumerRecord.value());
        epassUpdatesListener.getSmsForUpdates(consumerRecord.key(), epassUpdate);
    }

}
