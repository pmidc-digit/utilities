package org.egov.epass.chat.service;

import com.fasterxml.jackson.databind.JsonNode;
import org.egov.epass.chat.model.Sms;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class EpassUpdatesListener {

    @Autowired
    private KafkaTemplate<String, JsonNode> kafkaTemplate;

    @Value("${message.epass.updates}")
    private String messageTemplateForUpdates;

    public Sms getSmsForUpdates(String kafkaKey, JsonNode epassUpdate) {
        String mobileNumber = epassUpdate.at("/entity/phoneNumber").asText();

        String message = getSmsText(epassUpdate);

        return Sms.builder().mobileNumber(mobileNumber).build();
    }

    private String getSmsText(JsonNode epassUpdate) {
        return messageTemplateForUpdates;
    }

}
