package org.egov.epass.chat.service;

import com.fasterxml.jackson.databind.JsonNode;
import org.egov.epass.chat.model.Sms;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class EpassMessageFormatter {

    @Value("${sms.template.for.updates}")
    private String smsTemplateForUpdates;

    @Value("${sms.template.verify.response}")
    private String smsTemplateForVerifyResponse;

    @Autowired
    private TemplateMessageService templateMessageService;

    public Sms getSmsForUpdates(JsonNode jsonNode) {
        String mobileNumber = jsonNode.at("/entity/phoneNumber").asText();



        return Sms.builder().mobileNumber(mobileNumber).build();
    }

    public Sms getSmsForVerifyResponse(JsonNode jsonNode, String mobileNumber) {



        return Sms.builder().mobileNumber(mobileNumber).build();
    }


}
