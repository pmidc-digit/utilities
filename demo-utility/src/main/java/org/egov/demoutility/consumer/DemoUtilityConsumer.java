package org.egov.demoutility.consumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;
import org.egov.common.contract.request.RequestInfo;
import org.egov.demoutility.config.PropertyManager;
import org.egov.demoutility.model.DemoUtilityRequest;
import org.egov.demoutility.model.Employee;
import org.egov.demoutility.model.Gender;
import org.egov.demoutility.model.GuardianRelation;
import org.egov.demoutility.model.Jurisdiction;
import org.egov.demoutility.model.Role;
import org.egov.demoutility.model.User;
import org.egov.demoutility.model.UserType;
import org.egov.demoutlity.utils.UtilityConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class DemoUtilityConsumer {

	@Autowired
	PropertyManager propertyConfiguration;
	
	@Autowired
	ObjectMapper objectMapper;
	
	@Autowired
	RestTemplate restTemplate;
	
	
	
	
	
	@KafkaListener(topics = { "${egov.demoutility.topic}"})
	public void listen(final HashMap<String, Object> record, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		DemoUtilityRequest demoRequest=null;
		try {
			log.debug("Consuming record: " + record);
			demoRequest = mapper.convertValue(record, DemoUtilityRequest.class);
		} catch (final Exception e) {
			log.error("Error while listening to value: " + record + " on topic: " + topic + ": " + e);
		}
		
		 //TODO enable after implementation
	}
	
	
	public void createEmployee(DemoUtilityRequest demoUtilityRequest) {
		
		String authToken=getAccessToken(propertyConfiguration.getSuperUser(), propertyConfiguration.getPassword(), UtilityConstants.TENANTID);

		for (int i = 0; i < demoUtilityRequest.getNoOfUsers(); i++) {
			Map<String, List<String>> roleWiseList = getRoleWiseList();
			
			for (int j = 0; j < 6; j++) {

				String username = RandomStringUtils.randomAlphabetic(6);
				// username=username+"_"+demoUtilityRequest.getShortCode();
				String mobileNumber = generateMobileNo();
				
				createEmployee(demoUtilityRequest,username,mobileNumber,authToken,roleWiseList.get(j));

			}

		}

	}


	private void createEmployee(DemoUtilityRequest demoUtilityRequest, String username, String mobileNumber,
			String authToken,List<String> roles) {
		
		RequestInfo requestInfo=new RequestInfo();
		requestInfo.setAuthToken(authToken);
		Employee employee=new Employee();
		employee.setEmployeeStatus(UtilityConstants.EMPLOYEED);
		User user=new User();
		user.setName(demoUtilityRequest.getApplicantName());
		user.setMobileNumber(mobileNumber);
		user.setUserName(username);
		user.setPassword(UtilityConstants.DEFAULT_PASSWORD);
		user.setGender(Gender.MALE.toString());
		user.setDob(Long.valueOf(UtilityConstants.DOB));
		user.setFatherOrHusbandName(UtilityConstants.FATHERORHUSBAND);
		user.setRelationship(GuardianRelation.FATHER);
		List<Role> roleList=new ArrayList<Role>();
		for(String role : roles) {
			
			Role roleObj=new Role();
			roleObj.setCode(role);
			roleObj.setTenantId(UtilityConstants.TENANTID);	
			roleList.add(roleObj);
		}
		user.setRoles(roleList);
		employee.setUser(user);
		Jurisdiction jurisdiction=new Jurisdiction();
		jurisdiction.setBoundaryType(UtilityConstants.BOUNDARY_TYPE);
		
		
	}


	private Map<String, List<String>> getRoleWiseList() {
		// TODO Auto-generated method stub
		Map<String,List<String>> roleList=new HashMap<String,List<String>>();
		List<String> employeeList1=Arrays.asList("CEMP","TL_CEMP","NOC_CEMP","CSR","WS_CEMP","SW_CEMP","FSM_COLLECTOR","COLL_RECEIPT_CREATOR","PT_CEMP");
		List<String> employeeList2=Arrays.asList("BPA_VERIFIER","BPAREG_DOC_VERIFIER","AIRPORT_AUTHORITY_APPROVER","TL_DOC_VERIFIER","NOC_DOC_VERIFIER","GRO","WS_DOC_VERIFIER","SW_DOC_VERIFIER","FSM_CREATOR_EMP","FSM_VIEW_EMP","FSM_EDITOR_EMP","EGF_BILL_CREATOR","EGF_VOUCHER_CREATOR","EGF_PAYMENT_APPROVER","PT_DOC_VERIFIER");
		List<String> employeeList3=Arrays.asList("BPA_FIELD_INSPECTOR","BPA_NOC_VERIFIER","TL_FIELD_INSPECTOR","NOC_FIELD_INSPECTOR","WS_FIELD_INSPECTOR","SW_FIELD_INSPECTOR","EGF_BILL_APPROVER","EGF_VOUCHER_APPROVER","EGF_PAYMENT_APPROVER","PT_FIELD_INSPECTOR");
        List<String> employeeList4=Arrays.asList("BPA_APPROVER","BPAREG_APPROVER","TL_APPROVER","NOC_APPROVER","WS_APPROVER","SW_APPROVER","COLL_REMIT_TO_BANK","PT_APPROVER");
        List<String> employeeList5=Arrays.asList("BPA_NOC_VERIFIER","PGR_LME","WS_CLERK","SW_CLERK");
        List<String> employeeList6=Arrays.asList("STADMIN","FSM_ADMIN","EGF_ADMINISTRATOR","EGF_REPORT_VIEW");
        roleList.put("employee1", employeeList1);
        roleList.put("employee2", employeeList2);
        roleList.put("employee3", employeeList3);
        roleList.put("employee4", employeeList4);
        roleList.put("employee5", employeeList5);
        roleList.put("employee6", employeeList6);
        
        return roleList;
        
	}
	
	private static String generateMobileNo() {

		long number = (long) Math.floor(Math.random() * 9_000_000_000L) + 1_000_000_000L;
		String mobileNumber = String.format("4%4s", number);

		mobileNumber = mobileNumber.substring(0, mobileNumber.length() - 1);

		return mobileNumber;

	}
	
	
	public String getAccessToken(String superUsername, String superUserPassword, String tenantId) {

		String access_token = null;
		Object record = getAccess(superUsername, superUserPassword, tenantId);
		Map tokenObject = objectMapper.convertValue(record, Map.class);

		if (tokenObject.containsKey("access_token")) {
			access_token = (String) tokenObject.get("access_token");
			log.info("Access token: {}",  access_token);
		}

		return access_token;

	}
	
	public Object getAccess(String userName, String password, String tenantId) {
    	log.info("Fetch access token for register with login flow");
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
            //headers.set("Authorization", "Basic ZWdvdi11c2VyLWNsaWVudDplZ292LXVzZXItc2VjcmV0");
            MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
            map.add("username", userName);
            map.add("password", password);
            map.add("grant_type", "password");
            map.add("scope", "read");
            map.add("tenantId", tenantId);
            map.add("userType", UserType.EMPLOYEE.name());

            HttpEntity<MultiValueMap<String, String>> request = new HttpEntity<MultiValueMap<String, String>>(map,
                    headers);
            return restTemplate.postForEntity(propertyConfiguration.getUserHost() + propertyConfiguration.getUserAuthUrl(), request, Map.class).getBody();

        } catch (Exception e) {
        	log.error("Error occurred while logging-in via register flow" + e);
            throw e;
        }
    }
	
	
}