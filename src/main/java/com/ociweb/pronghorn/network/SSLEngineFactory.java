package com.ociweb.pronghorn.network;

import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSLEngineFactory {

	private static final KeyManager[] keyManagers;
	private static final TrustManager[] trustManagers;
	
	//protocol The SSL/TLS protocol to be used. Java 1.6 will only run with up to TLSv1 protocol. Java 1.7 or higher also supports TLSv1.1 and TLSv1.2 protocols.
	private static final String PROTOCOL = "TLSv1.2";
	private static final SSLContext context;

	private static final Logger log = LoggerFactory.getLogger(SSLEngineFactory.class);
	
	private static final boolean TRUST_ALL = true;
	
	static {
		
		try {
			keyManagers = createKeyManagers("./src/main/resources/client.jks", "storepass", "keypass");
			
			if (TRUST_ALL) {
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
				trustManagers = createTrustManagers("./src/main/resources/trustedCerts.jks", "storepass");				
			}
			
			
	        context = SSLContext.getInstance(PROTOCOL);
			context.init(keyManagers, trustManagers, new SecureRandom());
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
	}
	
    /**
     * Creates the key managers required to initiate the {@link SSLContext}, using a JKS keystore as an input.
     *
     * @param filepath - the path to the JKS keystore.
     * @param keystorePassword - the keystore's password.
     * @param keyPassword - the key's passsword.
     * @return {@link KeyManager} array that will be used to initiate the {@link SSLContext}.
     * @throws Exception
     */
    private static KeyManager[] createKeyManagers(String filepath, String keystorePassword, String keyPassword) throws Exception {
        KeyStore keyStore = KeyStore.getInstance("JKS");
        InputStream keyStoreIS = new FileInputStream(filepath);
        try {
            keyStore.load(keyStoreIS, keystorePassword.toCharArray());
        } finally {
            if (keyStoreIS != null) {
                keyStoreIS.close();
            }
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, keyPassword.toCharArray());
        return kmf.getKeyManagers();
    }

    /**
     * Creates the trust managers required to initiate the {@link SSLContext}, using a JKS keystore as an input.
     *
     * @param filepath - the path to the JKS keystore.
     * @param keystorePassword - the keystore's password.
     * @return {@link TrustManager} array, that will be used to initiate the {@link SSLContext}.
     * @throws Exception
     */
    private static TrustManager[] createTrustManagers(String filepath, String keystorePassword) throws Exception {
        KeyStore trustStore = KeyStore.getInstance("JKS");
        InputStream trustStoreIS = new FileInputStream(filepath);
        try {
            trustStore.load(trustStoreIS, keystorePassword.toCharArray());
        } finally {
            if (trustStoreIS != null) {
                trustStoreIS.close();
            }
        }
        TrustManagerFactory trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustFactory.init(trustStore);
        return trustFactory.getTrustManagers();
    }
	
	private static String[] cipherSuits;
	private static String[] protocols = new String[]{"TLSv1.2"}; //[SSLv2Hello, TLSv1, TLSv1.1, TLSv1.2]
    
    public static SSLEngine createSSLEngine(String host, int port) {
    	
    	SSLEngine result = context.createSSLEngine(host, port);
    	result.setEnabledCipherSuites(filterCipherSuits(result)); 
    	result.setEnabledProtocols(protocols);
    	
    	return result;
    }

    public static SSLEngine createSSLEngine() {
    	
    	SSLEngine result = context.createSSLEngine();
    	result.setEnabledCipherSuites(filterCipherSuits(result)); 
    	result.setEnabledProtocols(protocols);
    	
    	return result;
    }
    
	private static String[] filterCipherSuits(SSLEngine result) {
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
