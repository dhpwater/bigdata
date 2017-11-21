package com.ns.http;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

public class HttpsSSLClient {
	
	private static RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(15000).setConnectTimeout(15000)
			.setConnectionRequestTimeout(15000).build();

	/**
	 * 获取Https 请求客户端
	 * 
	 * @return
	 */
	public static CloseableHttpClient createSSLInsecureClient() {
		SSLContext sslcontext = createSSLContext();
		SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslcontext, new HostnameVerifier() {

			@Override
			public boolean verify(String paramString, SSLSession paramSSLSession) {
				return true;
			}
		});
		CloseableHttpClient httpclient = HttpClients.custom().setSSLSocketFactory(sslsf).build();
		return httpclient;
	}

	/**
	 * 获取初始化SslContext
	 * 
	 * @return
	 */
	private static SSLContext createSSLContext() {
		SSLContext sslcontext = null;
		try {
			sslcontext = SSLContext.getInstance("TLS");
			sslcontext.init(null, new TrustManager[] { new TrustAnyTrustManager() }, new SecureRandom());
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (KeyManagementException e) {
			e.printStackTrace();
		}
		return sslcontext;
	}

	/**
	 * 自定义静态私有类
	 */
	private static class TrustAnyTrustManager implements X509TrustManager {

		public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
		}

		public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
		}

		public X509Certificate[] getAcceptedIssuers() {
			return new X509Certificate[] {};
		}
	}
	
	public static void main(String[] args) throws ClientProtocolException, IOException{
		
		System.out.println("****************");
		CloseableHttpClient httpClient  = HttpsSSLClient.createSSLInsecureClient();
		
//		HttpGet httpGet = new HttpGet("https://www.baidu.com");
		
		HttpGet httpGet = new HttpGet("https://10.67.1.181/user/backupLogin?"
				+ "username=enlhaXNuU3FjcA==&password=THFkMGFzcSFAIw==&returl=/bcm#/");
		
		 httpGet.setConfig(requestConfig);  
         // 执行请求  
		 CloseableHttpResponse response = httpClient.execute(httpGet);  
		 HttpEntity entity = response.getEntity();  
		 String responseContent = EntityUtils.toString(entity, "UTF-8");  
		 
		 System.out.println(responseContent);
		 System.out.println("******************");
	}
}
