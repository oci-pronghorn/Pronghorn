package com.ociweb.pronghorn.network;

public abstract class SSLConnectionHolder {

	public final boolean isTLS;
	final SSLEngineFactory engineFactory;

	SSLConnectionHolder(TLSCertificates tlsCerificates) {
		this.isTLS = tlsCerificates != null;
		if (tlsCerificates != null) {
			// TODO: client/server differentiation and application overrides
			TLSCertificates tls = TLSCertificates.defaultCerts;
			this.engineFactory = new SSLEngineFactory(tls);
		}
		else {
			this.engineFactory = null;
		}
	}

	public abstract SSLConnection connectionForSessionId(long hostId);
}
