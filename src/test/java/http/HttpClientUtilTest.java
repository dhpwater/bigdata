package http;

import org.junit.Test;

import com.ns.http.HttpClientUtil;

public class HttpClientUtilTest {

//	@Test
	public void testSendHttpGet() {  
        String responseContent = HttpClientUtil.getInstance()  
                .sendHttpGet("http://10.67.1.64:8080/bcm/clusterInfo/get/1502330525480");  
        System.out.println("reponse content:" + responseContent);  
        
        
    }  
	
//	@Test
	public void testSendHttpsGet() {
		String responseContent = HttpClientUtil.getInstance()
				 .sendHttpsGet("https://10.67.1.181/user/backupLogin?username=enlhaXNuU3FjcA==&password=THFkMGFzcSFAIw==&returl=/bcm#/");  
		System.out.println("reponse content:" + responseContent);
	}
	
//	@Test
	public void testSendHttpPost(){
		String param  = HttpClientUtil.getInstance()  
                .sendHttpGet("http://10.67.1.64:8080/bcm/clusterInfo/get/1502330525480");  
		System.out.println("param :" + param);
		
		String responseContent = HttpClientUtil.getInstance()
				.sendHttpPost("http://10.67.1.64:8080/bcm/clusterInfo/set/1502330525480" ,param , "application/json");
		
		System.out.println("reponse content:" + responseContent);
	}
	
	
}
