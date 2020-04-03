package org.egov.epass.chat.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.egov.epass.chat.model.Sms;
import org.egov.epass.chat.service.ChatService;
import org.egov.epass.chat.service.EpassCreateNotification;
import org.egov.epass.chat.smsprovider.KarixSendSMSService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;

import java.io.IOException;
import java.util.List;

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
    private EpassCreateNotification epassCreateNotification;

    @KafkaListener(topics = "${send.message.topic}")
    public void sendSms(List<JsonNode> smsJsonList) throws IOException {
        List<Sms> smsList = objectMapper.readValue(smsJsonList.toString(), new TypeReference<List<Sms>>() {});
        karixSendSMSService.sendSMS(smsList.get(0));
    }

    @KafkaListener(topics = "karix-received-messages")
    public void processMessage(JsonNode chatNode) throws IOException {
        chatService.processMessage(null, chatNode);
    }

    @KafkaListener(topics = "${epass.notifications.topic}")
    public void processUpdates(JsonNode notification) throws IOException {
        epassCreateNotification.sendSmsForCreatedPass(null, notification);
    }

}
