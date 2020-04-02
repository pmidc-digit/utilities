package org.egov.epass.chat.model;

import lombok.*;

@Builder
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Sms {

    private String text;

    private String mobileNumber;

}
