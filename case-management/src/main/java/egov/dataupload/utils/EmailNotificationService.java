package egov.dataupload.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import egov.Main;
import egov.dataupload.producer.Producer;
import egov.dataupload.web.models.Employee;
import egov.dataupload.web.models.EmployeeCreateRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

@Slf4j
@Service
public class EmailNotificationService {

    @Autowired
    private Producer producer;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private TenantService tenantService;

    @Value("${send.email.topic}")
    private String sendEmailTopic;

    @Value("${email.subject.onboard.health.details.collector}")
    private String emailSubjectOnboardHealthDetailsCollector;
    @Value("${email.subject.onboard.case.admin}")
    private String emailSubjectOnboardCaseAdmin;

    @Value("${isolation.health.collection.time}")
    private String isolationHealthCollectionTime;

    public void sendOnboardingEmployeeEmail(EmployeeCreateRequest employeeCreateRequest) throws Exception {
        Employee employee = employeeCreateRequest.getEmployee();
        String templateFileName = "";
        String subject = "";
        if(employee.getRoles().get(0).equalsIgnoreCase("ISOLATION_HEALTH_DETAILS_COLLECTOR")) {
            templateFileName = "EmailOnboardHealthDetailsCollector.txt";
            subject = emailSubjectOnboardHealthDetailsCollector;
        } else if(employee.getRoles().get(0).equalsIgnoreCase("ISOLATION_CASE_ADMIN")) {
            templateFileName = "EmailOnboardIsolationCaseManager.txt";
            subject = emailSubjectOnboardCaseAdmin;
        }

        String districtName = tenantService.getDistrictNameForTenantId(employee.getTenantId());

        URL url = Main.class.getClassLoader().getResource(templateFileName);
        String content = new String(Files.readAllBytes(Paths.get(url.toURI())));
        content = content.replace("<district-name>", districtName);

        content = content.replace("<time>", isolationHealthCollectionTime);

        ArrayNode emailTo = objectMapper.createArrayNode();
        emailTo.add(employee.getEmailId());

        ObjectNode email = objectMapper.createObjectNode();
        email.set("emailTo", emailTo);
        email.put("subject", subject);
        email.put("body", content);

        ObjectNode emailRequest = objectMapper.createObjectNode();
        emailRequest.set("requestInfo", objectMapper.convertValue(employeeCreateRequest.getRequestInfo(), JsonNode.class));
        emailRequest.set("email", email);

        producer.push(sendEmailTopic, null, emailRequest);
    }

}
