package com.ociweb.pronghorn.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;

public class TLSCertificateTrust {

	private static final Logger logger = LoggerFactory.getLogger(TLSCertificateTrust.class);
	
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
