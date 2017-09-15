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
	private static KeyManagerFactory keyManagerFactory;
	private static TrustManagerFactory trustManagerFactory;
    private static TLSService privateService;

    public static void init(TLSPolicy certSource) {
        // TODO: once server coordinator and client coordinator share the same policy
        // this init can be moved up and called once.
        // Then we should be smart about being server or client only
        if (keyManagerFactory != null) {
            return;
        }
        try {
            // Server Identity
            InputStream keyInputStream = certSource.keyInputStream();
            // All the internet sites client trusts
            InputStream trustInputStream = certSource.trustInputStream();

            String keyPassword = certSource.keyPassword();
            String keyStorePassword = certSource.keyStorePassword();

            keyManagerFactory = createKeyManagers(keyInputStream, keyStorePassword, keyPassword);
            trustManagerFactory = createTrustManagers(trustInputStream, keyStorePassword);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
	
	public static TLSService getService() {
		if (privateService==null) {
			privateService = new TLSService(keyManagerFactory, trustManagerFactory);
		}
		return privateService;
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
    	return getService().createSSLEngineClient(host, port);
    }

    public static SSLEngine createSSLEngine() {
    	return getService().createSSLEngineServer();
    }


    

	
}
