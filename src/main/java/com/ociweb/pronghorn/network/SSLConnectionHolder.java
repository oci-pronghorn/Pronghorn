package com.ociweb.pronghorn.network;

import java.io.InputStream;

public abstract class SSLConnectionHolder {

	public final boolean isTLS;
	final SSLEngineFactory engineFactory;

	SSLConnectionHolder(boolean isTLS) {
		this.isTLS = isTLS;
		if (isTLS) {
			// TODO: client/server differentiation and application overrides
			TLSCertificates tls = new TLSCertificates() {
				@Override
				public InputStream keyInputStream() {
					return TLSCertificates.class.getResourceAsStream("/client.jks");
				}

				@Override
				public InputStream trustInputStream() {
					return TLSCertificates.class.getResourceAsStream("/trustedCerts.jks");
				}

				@Override
				public String keyStorePassword() {
					return "storepass";
				}

				@Override
				public String keyPassword() {
					return "keypass";
				}
			};
			this.engineFactory = new SSLEngineFactory(tls);
		}
		else {
			this.engineFactory = null;
		}
	}

	public abstract SSLConnection connectionForSessionId(long hostId);
}
