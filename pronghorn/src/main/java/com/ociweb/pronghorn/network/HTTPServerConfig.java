package com.ociweb.pronghorn.network;

import com.ociweb.pronghorn.network.config.HTTPHeader;

public interface HTTPServerConfig {
	HTTPServerConfig setDefaultPath(String defaultPath);
	HTTPServerConfig setHost(String host);
	HTTPServerConfig setTLS(TLSCertificates certificates);
	HTTPServerConfig useInsecureServer();
	HTTPServerConfig disableEPoll();
	HTTPServerConfig setMaxConnectionBits(int bits);
	HTTPServerConfig setMinConnections(int connections);
		
	HTTPServerConfig setMaxRequestSize(int maxRequestSize);
	HTTPServerConfig setMaxResponseSize(int maxResponseSize);	
	HTTPServerConfig setMaxQueueIn(int maxQueueIn);
	HTTPServerConfig setMaxQueueOut(int maxQueueOut);
	HTTPServerConfig setEncryptionUnitsPerTrack(int value);
	HTTPServerConfig setDecryptionUnitsPerTrack(int value);
	HTTPServerConfig setConcurrentChannelsPerEncryptUnit(int value);
	HTTPServerConfig setConcurrentChannelsPerDecryptUnit(int value);
	HTTPServerConfig setMinimumInputPipeMemory(int bytes);
	HTTPServerConfig logTraffic(String basePath, int fileCount, long fileSizeLimit, boolean logResponses);
	HTTPServerConfig logTraffic(boolean logResponses);
	HTTPServerConfig logTraffic();
	
	@Deprecated //use the the setTLS method and TLSCerts.define()... as an argument
	HTTPServerConfig setClientAuthRequired(boolean value);
	HTTPServerConfig setServiceName(String name);
	
	int getMaxConnectionBits();
	int getEncryptionUnitsPerTrack();
	int getDecryptionUnitsPerTrack();
	int getConcurrentChannelsPerEncryptUnit();
	int getConcurrentChannelsPerDecryptUnit();
	boolean isTLS();
	TLSCertificates getCertificates();	
	ServerConnectionStruct connectionStruct();
	String bindHost();
	int bindPort();
	String defaultHostPath();	
	int getMaxRequestSize();
		
	@Deprecated
	ServerPipesConfig buildServerConfig(int tracks);
	@Deprecated //modify code to use buildServerCoordinator
	ServerPipesConfig buildServerConfig();
	
	HTTPServerConfig echoHeaders(int maxSingleHeaderLength, HTTPHeader ... headers);
	boolean requireClientAuth();
	String serviceName();
	ServerCoordinator buildServerCoordinator();
	int getMaxQueueIn();
	int getMaxQueueOut();


}

