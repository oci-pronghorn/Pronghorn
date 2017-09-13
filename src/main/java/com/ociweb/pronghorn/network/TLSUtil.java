package com.ociweb.pronghorn.network;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TLSUtil {

	private static final Logger logger = LoggerFactory.getLogger(TLSUtil.class);
	
	public static void trustAllCerts(final String host) {
		logger.warn("WARNING: this scope will now accept all certs on host: "+host+". This is for testing only!");
		
		try {
		     SSLContext sc = SSLContext.getInstance("SSL");
		     TrustManager[] trustAllCerts = new TrustManager[]{
		    		 new X509TrustManager() {
		    			 public java.security.cert.X509Certificate[] getAcceptedIssuers() {
		    				 return null;
		    			 }
		    			 public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) {
		    			 }
		    			 public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) {
		    			 }
		    		 }
		     };
		     sc.init(null, trustAllCerts, new java.security.SecureRandom());
		     HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
		     
		     HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
		         public boolean verify(String hostname, SSLSession session) {
		        	 return hostname.equals(host);
		         }
		     });
		     
		 } catch (Exception e) {
		    throw new RuntimeException(e);
		 }
	}
}
