package com.ociweb.pronghorn.network;

import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;

import javax.net.ssl.SSLContext;

//import io.netty.handler.ssl.SslContext;


import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSLEngineFactory {

	private static final Logger log = LoggerFactory.getLogger(SSLEngineFactory.class);

	
	private static final KeyManagerFactory keyManagerFactory;
	private static final TrustManagerFactory trustManagerFactory;
	
	static {
		
		try {
			keyManagerFactory = createKeyManagers("./src/main/resources/client.jks", "storepass", "keypass");
			trustManagerFactory = createTrustManagers("./src/main/resources/trustedCerts.jks", "storepass");
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
	
    /**
     * Creates the key managers required to initiate the {@link SSLContext}, using a JKS keystore as an input.
     *
     * @param filepath - the path to the JKS keystore.
     * @param keystorePassword - the keystore's password.
     * @param keyPassword - the key's passsword.
     * @return {@link KeyManager} array that will be used to initiate the {@link SSLContext}.
     * @throws Exception
     */
    private static KeyManagerFactory createKeyManagers(String filepath, String keystorePassword, String keyPassword) throws Exception {
    	InputStream keyStoreIS = new FileInputStream(filepath);

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
    private static TrustManagerFactory createTrustManagers(String filepath, String keystorePassword) throws Exception {
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
        return trustFactory;
    }
	

    public static SSLEngine createSSLEngine(String host, int port) {
    	return service.createSSLEngineClient(host, port);
    }

    public static SSLEngine createSSLEngine() {
    	return service.createSSLEngineServer();
    }
    

	
}
