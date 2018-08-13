package com.ociweb.pronghorn.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;

public class TLSCertificateTrust {

	private static final Logger logger = LoggerFactory.getLogger(TLSCertificateTrust.class);

	/**
	 * Creates the key managers required to initiate the {@link SSLContext}, using a JKS keystore as an input.
	 *
	 * @param keystorePassword - the keystore's password.
	 * @param keyPassword - the key's passsword.
	 * @return {@link KeyManager} array that will be used to initiate the {@link SSLContext}.
	 * @throws Exception
	 */
	public static KeyManagerFactory createKeyManagers(InputStream keyStoreIS, String keystorePassword, String keyPassword) throws Exception  {
		KeyStore keyStore = KeyStore.getInstance("JKS");
		keyStore.load(keyStoreIS, keystorePassword.toCharArray());
		KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
		kmf.init(keyStore, keyPassword.toCharArray());
		return kmf;
	}

	/**
	 * Creates the trust managers required to initiate the {@link SSLContext}, using a JKS keystore as an input.
	 *
	 * @param keystorePassword - the keystore's password.
	 * @return {@link TrustManager} array, that will be used to initiate the {@link SSLContext}.
	 * @throws Exception
	 */
	public static TrustManagerFactory createTrustManagers(InputStream trustStoreIS, String keystorePassword) throws Exception {
		KeyStore trustStore = KeyStore.getInstance("JKS");
		trustStore.load(trustStoreIS, keystorePassword.toCharArray());
		TrustManagerFactory trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		trustFactory.init(trustStore);
		return trustFactory;
	}

	public static TrustManager[] trustManagerFactoryTrustAllCerts() throws NoSuchAlgorithmException {
		logger.warn("WARNING: No trust manager in use, all connecions will be trusted. This is only appropriate for development and testing.");
		return new TrustManager[] {
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
	}

	public static void httpsURLConnectionTrustAllCerts(final String host) {
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
