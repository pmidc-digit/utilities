package egov.dataupload.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import egov.dataupload.models.AuditDetails;
import org.egov.common.contract.request.RequestInfo;

import java.time.Instant;
import java.util.Base64;

public class Utils {

    public static AuditDetails getAuditDetails(String by, boolean isCreate) {
        Long time = Instant.now().toEpochMilli();
        if(isCreate)
            return AuditDetails.builder().createdBy(by).lastModifiedBy(by).createdTime(time).lastModifiedTime(time).build();
        else
            return AuditDetails.builder().lastModifiedBy(by).lastModifiedTime(time).build();
    }

    public static RequestInfo getRequestInfo(ObjectMapper mapper, String requestInfoBase64) {
        try {
            String decoded = new String(Base64.getDecoder().decode(requestInfoBase64));
            return mapper.readValue(decoded, RequestInfo.class);

        } catch (JsonProcessingException e) {

            return new RequestInfo();
        }
    }

}
