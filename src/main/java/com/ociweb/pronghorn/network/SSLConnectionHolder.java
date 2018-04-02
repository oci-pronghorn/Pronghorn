package com.ociweb.pronghorn.network;

public abstract class SSLConnectionHolder {

	public final boolean isTLS;
	public final SSLEngineFactory engineFactory;

	SSLConnectionHolder(TLSCertificates tlsCerificates) {
		this.isTLS = tlsCerificates != null;
		if (tlsCerificates != null) {
			this.engineFactory = new SSLEngineFactory(tlsCerificates);
		}
		else {
			this.engineFactory = null;
		}
	}

	public abstract SSLConnection connectionForSessionId(long hostId);
}
