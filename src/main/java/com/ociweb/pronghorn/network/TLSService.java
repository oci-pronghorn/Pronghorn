package com.ociweb.pronghorn.network;

import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Arrays;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TLSService {

	private static final Logger log = LoggerFactory.getLogger(TLSService.class);
			
	private final SSLContext context;
	private final KeyManager[] keyManagers;
	private final TrustManager[] trustManagers;
	
	//protocol The SSL/TLS protocol to be used. Java 1.6 will only run with up to TLSv1 protocol. Java 1.7 or higher also supports TLSv1.1 and TLSv1.2 protocols.
	private static final String PROTOCOL    = "TLSv1.2";
	private static final String PROTOCOL1_3 = "TLSv1.3"; //check Java version and move up to this ASAP.
	
	private static final boolean TRUST_ALL = true;
	
	private String[] cipherSuits;
	private String[] protocols = new String[]{PROTOCOL}; //[SSLv2Hello, TLSv1, TLSv1.1, TLSv1.2]
    
	
	public TLSService(KeyManagerFactory keyManagerFactory, TrustManagerFactory trustManagerFactory) {
		
		try {
			
			keyManagers = keyManagerFactory.getKeyManagers();			
			
			if (TRUST_ALL) {
				TrustManagerFactory trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
				log.warn("***** No trust manager in use, all connecions will be trusted. This is only appropriate for development and testing. *****");
				trustManagers = new TrustManager[] { 
					    new X509TrustManager() {     
					        public java.security.cert.X509Certificate[] getAcceptedIssuers() { 
					            return new X509Certificate[0];
					        } 
					        public void checkClientTrusted( 
					            java.security.cert.X509Certificate[] certs, String authType) {
					            } 
					        public void checkServerTrusted( 
					            java.security.cert.X509Certificate[] certs, String authType) {
					        }
					    } 
					}; 
				
			} else {
				trustManagers = trustManagerFactory.getTrustManagers();				
		
			}
			
			
			
	        context = SSLContext.getInstance(PROTOCOL);
			context.init(keyManagers, trustManagers, new SecureRandom());
			
			//run once first to determine which cypher suites we will be using.
			createSSLEngineServer();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
	}
	
	public int maxEncryptedContentLength() {
		return 33305;//java TLS engine requested this value;//Integer.MAX_VALUE;
	}
	
    public SSLEngine createSSLEngineClient(String host, int port) {
    	
    	SSLEngine result = context.createSSLEngine(host, port);
    	result.setEnabledCipherSuites(filterCipherSuits(result)); 
    	result.setEnabledProtocols(protocols);
    	
    	return result;
    }

    public SSLEngine createSSLEngineServer() {
    	
    	SSLEngine result = context.createSSLEngine();
    	result.setEnabledCipherSuites(filterCipherSuits(result)); 
    	result.setEnabledProtocols(protocols);
    	return result;
    }
    
	private String[] filterCipherSuits(SSLEngine result) {
		if (null==cipherSuits) {
    		
	    	String[] enabledCipherSuites = result.getSupportedCipherSuites();
	    	int count = 0;
	    	int i = enabledCipherSuites.length;
	    	while (--i>=0) {
	    		if (containsPerfectForward(enabledCipherSuites, i)) {
	    			if (doesNotContainWeakCipher(enabledCipherSuites, i)) {
	    				count++;
	    				//System.out.println(enabledCipherSuites[i]);
	    			}
	    		}
	    	}
	    	String[] temp = new String[count];
	    	i = enabledCipherSuites.length;
	    	int j = 0;
	    	while (--i>=0) {
	    		if (containsPerfectForward(enabledCipherSuites, i)) {
	    			if (doesNotContainWeakCipher(enabledCipherSuites, i)) {
	    				
	    				System.out.println("Cipher: "+enabledCipherSuites[i]);
	    				
	    				temp[j++]=enabledCipherSuites[i];
	    			}
	    		}
	    	}
	    	cipherSuits = temp;
    	}
		return cipherSuits;
	}

	private static boolean doesNotContainWeakCipher(String[] enabledCipherSuites, int i) {
		return !enabledCipherSuites[i].contains("DES_") &&
			   !enabledCipherSuites[i].contains("EXPORT") && 
			   !enabledCipherSuites[i].contains("AES128") && 
			  // !enabledCipherSuites[i].contains("_DHE_") && //hack which should not remain. //TODO: we need to sort to put ECC first.
			 //  !enabledCipherSuites[i].contains("_RSA_") && //testing  ECDSA, requires  ECDSA certificate first!!
			   !enabledCipherSuites[i].contains("NULL");
		
	}

	private static boolean containsPerfectForward(String[] enabledCipherSuites, int i) {
		return enabledCipherSuites[i].contains("DHE") || 
			   enabledCipherSuites[i].contains("EDH");
	}
	
}
