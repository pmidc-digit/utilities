package egov.casemanagement.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import egov.dataupload.config.Configuration;
import egov.dataupload.producer.Producer;
import egov.dataupload.web.models.CaseCreateRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class SmsNotificationService {

    @Autowired
    private Configuration configuration;
    @Autowired
    private Producer producer;
    @Autowired
    private ObjectMapper objectMapper;

    @Value("${sms.create.case.template}")
    private String smsCreateCaseTemplate;

    public void sendCreateCaseSms(String mobileNumber) {

        ObjectNode smsRequest = objectMapper.createObjectNode();

        smsRequest.put("mobileNumber", mobileNumber);
        smsRequest.put("message", smsCreateCaseTemplate);

        producer.push(configuration.getSendSmsTopic(), null, smsRequest);

    }

}
