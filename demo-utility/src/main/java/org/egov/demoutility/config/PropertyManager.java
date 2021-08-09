package org.egov.demoutility.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import lombok.Data;

@Data
@Component
public class PropertyManager {
	
	@Value("${egov.demoutility.topic}")
	public String demotopic;
	
	
	@Value("${egov.core.notification.email.topic}")
	public String emailNotificationTopic;
	
	@Value("${egov.user.host}")
	public String userHost;
	
	@Value("${egov.user.oauth.url}")
	public String userAuthUrl;
	
	@Value("${egov.hrms.superuser}")
	public String superUser;
	
	@Value("${egov.hrms.password}")
	public String password;

	
	

}
