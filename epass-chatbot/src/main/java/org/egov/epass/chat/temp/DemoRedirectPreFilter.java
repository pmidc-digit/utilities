package org.egov.epass.chat.temp;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.jndi.toolkit.url.Uri;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.utils.URLEncodedUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

@Component
@Slf4j
public class DemoRedirectPreFilter implements Filter {

    @Autowired
    private RestTemplate restTemplate;

    @Value("${demo.redirect.enabled}")
    private boolean demoRedirectEnabled;

    @Value("${demo.redirect.url}")
    private String demoRedirectUrl;
    @Value("${demo.redirect.prefix.keyword}")
    private String demoKeywordPrefix;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {

        if(! demoRedirectEnabled) {
            filterChain.doFilter(servletRequest, servletResponse);
            return;
        }

        if(checkIfDemoKeyword(servletRequest)) {
            callRedirectUrl(servletRequest);
        } else {
            filterChain.doFilter(servletRequest, servletResponse);
        }
    }


    boolean checkIfDemoKeyword(ServletRequest servletRequest) {
        String inputText = servletRequest.getParameter("Rawmessage");
        String upperCaseInput = inputText.toUpperCase();
        return upperCaseInput.indexOf(demoKeywordPrefix.toUpperCase()) == 0;
    }

    void callRedirectUrl(ServletRequest servletRequest) {

        Map<String, String[]> parameterMap = servletRequest.getParameterMap();
        String message = parameterMap.get("Rawmessage")[0];

        message = message.substring(demoKeywordPrefix.length() + 1);

        parameterMap.get("Rawmessage")[0] = message;
        MultiValueMap<String, String> queryParams = new LinkedMultiValueMap<>();

        parameterMap.forEach((String key, String[] value) -> {
            for(String s : value) {
                queryParams.add(key, s);
            }
        });

        String url = UriComponentsBuilder.fromHttpUrl(demoRedirectUrl).queryParams(queryParams).build().toUriString();

        JsonNode response = restTemplate.getForObject(url, JsonNode.class);

        log.info("Called redirect url");
    }

    @Override
    public void destroy() {

    }
}
