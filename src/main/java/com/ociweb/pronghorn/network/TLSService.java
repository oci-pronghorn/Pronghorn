package com.ociweb.pronghorn.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.InputStream;
import java.security.SecureRandom;

public class TLSService {
	private static final Logger logger = LoggerFactory.getLogger(TLSService.class);
	private final SSLContext context;
	private String[] cipherSuits;
	private final String[] protocols;
	
	public static final boolean LOG_CYPHERS = false;

	public static TLSService make(InputStream keyStoreInputStream, String keystorePassword, InputStream trustStoreInputStream, String keyPassword, boolean trustAll) {
		try {
			KeyManagerFactory keyManagerFactory = keyStoreInputStream != null ? TLSCertificateTrust.createKeyManagers(keyStoreInputStream, keystorePassword, keyPassword) : null;
			TrustManagerFactory trustManagerFactory = trustStoreInputStream != null ? TLSCertificateTrust.createTrustManagers(trustStoreInputStream, keystorePassword) : null;
			// null SecureRandom uses default impl of new SecureRandom()
			return new TLSService(keyManagerFactory, trustManagerFactory, trustAll, null);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private TLSService(KeyManagerFactory keyManagerFactory, TrustManagerFactory trustManagerFactory, boolean trustAll, SecureRandom secureRandom) {
		try {
			//protocol The SSL/TLS protocol to be used. Java 1.6 will only run with up to TLSv1 protocol. Java 1.7 or higher also supports TLSv1.1 and TLSv1.2 protocols.
			final String PROTOCOL    = "TLSv1.2";
			final String PROTOCOL1_3 = "TLSv1.3"; //check Java version and move up to this ASAP.

			this.protocols = new String[]{PROTOCOL}; //[SSLv2Hello, TLSv1, TLSv1.1, TLSv1.2]

			KeyManager[] keyManagers = keyManagerFactory != null ? keyManagerFactory.getKeyManagers() : null;

			TrustManager[] trustManagers = null;
			if (trustAll) {
				trustManagers = TLSCertificateTrust.trustManagerFactoryTrustAllCerts();
			}
			else if (trustManagerFactory != null) {
				trustManagers = trustManagerFactory.getTrustManagers();
			}
	        context = SSLContext.getInstance(PROTOCOL);
			context.init(keyManagers, trustManagers, secureRandom);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		//run once first to determine which cypher suites we will be using.
		createSSLEngineServer();
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
			//TODO: rewrite with recursive count...
	    	String[] enabledCipherSuites = result.getSupportedCipherSuites();
	    	int count = 0;
	    	int i = enabledCipherSuites.length;
	    	while (--i>=0) {
	    		if (containsPerfectForward(enabledCipherSuites, i)) {
	    			if (doesNotContainWeakCipher(enabledCipherSuites, i)) {
	    				count++;
	    			}
	    		}
	    	}
	    	String[] temp = new String[count];
	    	i = enabledCipherSuites.length;
	    	int j = 0;
	    	while (--i>=0) {
	    		if (containsPerfectForward(enabledCipherSuites, i)) {
	    			if (doesNotContainWeakCipher(enabledCipherSuites, i)) {
	    				if (LOG_CYPHERS) {
	    					logger.info("enable cipher suite: {}",enabledCipherSuites[i]);
	    				}
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
			   !enabledCipherSuites[i].contains("NULL");
	}

	private static boolean containsPerfectForward(String[] enabledCipherSuites, int i) {
		return enabledCipherSuites[i].contains("DHE") || 
			   enabledCipherSuites[i].contains("EDH");
	}
}
