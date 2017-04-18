package com.ociweb.pronghorn.network;

import java.io.InputStream;
import java.security.KeyStore;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

//import io.netty.handler.ssl.SslContext;


import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSLEngineFactory {

	private static final Logger log = LoggerFactory.getLogger(SSLEngineFactory.class);

	
	private static final KeyManagerFactory keyManagerFactory;
	private static final TrustManagerFactory trustManagerFactory;
	
	static {
		
		try {
			InputStream keyInputStream = SSLEngineFactory.class.getResourceAsStream("/client.jks"); //new FileInputStream("./src/main/resources/client.jks")
			InputStream trustInputStream = SSLEngineFactory.class.getResourceAsStream("/trustedCerts.jks"); //new FileInputStream("./src/main/resources/trustedCerts.jks")
						
			keyManagerFactory = createKeyManagers(keyInputStream, "storepass", "keypass");
			trustManagerFactory = createTrustManagers(trustInputStream, "storepass");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}		
		
	}

	private static final TLSService service = new TLSService(keyManagerFactory, trustManagerFactory);
	public static final int maxEncryptedContentLength = //18713;
			                                            //1<<14;//
	                                                   service.maxEncryptedContentLength();
	
	static {
		log.warn("max size encrypted block {} ", maxEncryptedContentLength);
		
	}
	
	public static void init() {
		//NOTE: does not appear to do anything but this call ensure that the static values are all setup by the time this is called.
		assert(null!=service);
		assert(maxEncryptedContentLength>0);
		assert(null!=keyManagerFactory);
		assert(null!=trustManagerFactory);		
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
    private static KeyManagerFactory createKeyManagers(InputStream keyStoreIS, String keystorePassword, String keyPassword) throws Exception {

    	KeyStore keyStore = KeyStore.getInstance("JKS");
        try {
            keyStore.load(keyStoreIS, keystorePassword.toCharArray());
        } finally {
            if (keyStoreIS != null) {
                keyStoreIS.close();
            }
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, keyPassword.toCharArray());
        return kmf;
    }

    /**
     * Creates the trust managers required to initiate the {@link SSLContext}, using a JKS keystore as an input.
     *
     * @param filepath - the path to the JKS keystore.
     * @param keystorePassword - the keystore's password.
     * @return {@link TrustManager} array, that will be used to initiate the {@link SSLContext}.
     * @throws Exception
     */
    private static TrustManagerFactory createTrustManagers(InputStream trustStoreIS, String keystorePassword) throws Exception {
        KeyStore trustStore = KeyStore.getInstance("JKS");
        try {
            trustStore.load(trustStoreIS, keystorePassword.toCharArray());            
        } finally {
            if (trustStoreIS != null) {
                trustStoreIS.close();
            }
        }
        TrustManagerFactory trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustFactory.init(trustStore);
        return trustFactory;
    }	

    public static SSLEngine createSSLEngine(String host, int port) {
    	return service.createSSLEngineClient(host, port);
    }

    public static SSLEngine createSSLEngine() {
    	return service.createSSLEngineServer();
    }


    

	
}
