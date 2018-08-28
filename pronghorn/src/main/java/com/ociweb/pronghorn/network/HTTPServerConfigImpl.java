package com.ociweb.pronghorn.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.config.HTTPHeader;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.PipeConfigManager;
import com.ociweb.pronghorn.struct.StructRegistry;

public class HTTPServerConfigImpl implements HTTPServerConfig {
	
	private final static Logger logger = LoggerFactory.getLogger(HTTPServerConfigImpl.class);
	
	public enum BridgeConfigStage {
	    Construction,
	    DeclareConnections,
	    DeclareBehavior,
	    Finalized;

	    /**
	     *
	     * @param stage BridgeConfigStage arg used for comparison
	     * @throws UnsupportedOperationException if stage != this
	     */
	    public void throwIfNot(BridgeConfigStage stage) {
			if (stage != this) {
				throw new UnsupportedOperationException("Cannot invoke method in " + this.toString() + "; must be in " + stage.toString());
			}
	    }
	}
	
	private String defaultHostPath = "";
	private String bindHost = null;
	private int bindPort = -1;
	private int maxConnectionBits = 15; //default of 32K
	private int encryptionUnitsPerTrack = 2; //default of 2 per track or none without TLS
	private int decryptionUnitsPerTrack = 2; //default of 2 per track or none without TLS
	private int concurrentChannelsPerEncryptUnit = 2; //default 2, for low memory usage
	private int concurrentChannelsPerDecryptUnit = 2; //default 2, for low memory usage
	private TLSCertificates serverTLS = TLSCerts.define();
	private BridgeConfigStage configStage = BridgeConfigStage.Construction;
	private int maxRequestSize = 1<<15;//default of 32K
	private int maxResponseSize = 1<<12;//default of 4K
	private final PipeConfigManager pcm;
    private int tracks = 1;//default 1, for low memory usage
	private LogFileConfig logFile;	

	private String serviceName = "Server";
	
	private final ServerConnectionStruct scs;
	
	public HTTPServerConfigImpl(int bindPort, 
			                    PipeConfigManager pcm, 
			                    StructRegistry recordTypeData) {
		this.bindPort = bindPort;
		if (bindPort<=0 || (bindPort>=(1<<16))) {
			throw new UnsupportedOperationException("invalid port "+bindPort);
		}

		this.pcm = pcm;		
		
		//NOTE: this is set at the minimum sizes to support example, template and favicon.ico files
		this.pcm.ensureSize(ServerResponseSchema.class, 4, 2048); //TODO: may drop this to 1024 that would be nice.		
		
		this.scs = new ServerConnectionStruct(recordTypeData);
		beginDeclarations();

	}
	
	public ServerConnectionStruct connectionStruct() {
		return scs;
	}

	@Override
	public ServerCoordinator buildServerCoordinator() {
		finalizeDeclareConnections();
		
		return new ServerCoordinator(
				getCertificates(),
				bindHost(), 
				bindPort(),
				connectionStruct(),
				requireClientAuth(),
				serviceName(),
				defaultHostPath(), 
				buildServerConfig());
	}
	
	public void beginDeclarations() {
		this.configStage = BridgeConfigStage.DeclareConnections;
	}
	
	public final int getMaxConnectionBits() {
		return maxConnectionBits;
	}

	public final int getEncryptionUnitsPerTrack() {
		return encryptionUnitsPerTrack;
	}

	public final int getDecryptionUnitsPerTrack() {
		return decryptionUnitsPerTrack;
	}

	public final int getConcurrentChannelsPerEncryptUnit() {
		return concurrentChannelsPerEncryptUnit;
	}

	public final int getConcurrentChannelsPerDecryptUnit() {
		return concurrentChannelsPerDecryptUnit;
	}
	
	public final boolean isTLS() {
		return serverTLS != null;
	}

	public final TLSCertificates getCertificates() {
		return serverTLS;
	}

	public final String bindHost() {
		assert(null!=bindHost) : "finalizeDeclareConnections() must be called before this can be used.";
		return bindHost;
	}

	public final int bindPort() {
		return bindPort;
	}

	public final String defaultHostPath() {
		return defaultHostPath;
	}

	@Override
	public HTTPServerConfig setMaxRequestSize(int maxRequestSize) {
		this.maxRequestSize = maxRequestSize;
		return this;
	}
	
	@Override
	public HTTPServerConfig setMaxResponseSize(int maxResponseSize) {
		pcm.ensureSize(ServerResponseSchema.class, 4, maxResponseSize);	
		//logger.info("\nsetting the max response size for {}  to {}", ServerResponseSchema.class, maxResponseSize);
		
		this.maxResponseSize = maxResponseSize;
		return this;
	}
	
	
	@Override
	public HTTPServerConfig setDefaultPath(String defaultPath) {
		configStage.throwIfNot(BridgeConfigStage.DeclareConnections);
		assert(null != defaultPath);
		this.defaultHostPath = defaultPath;
		return this;
	}

	@Override
	public HTTPServerConfig setHost(String host) {
		configStage.throwIfNot(BridgeConfigStage.DeclareConnections);
		this.bindHost = host;
		return this;
	}

	@Override
	public HTTPServerConfig setTLS(TLSCertificates certificates) {
		configStage.throwIfNot(BridgeConfigStage.DeclareConnections);
		assert(null != certificates);
		this.serverTLS = certificates;
		return this;
	}

	@Override
	public HTTPServerConfig useInsecureServer() {
		configStage.throwIfNot(BridgeConfigStage.DeclareConnections);
		this.serverTLS = null;
		return this;
	}

	@Override
	public HTTPServerConfig setMinConnections(int connections) {
		return setMaxConnectionBits((int)Math.ceil(Math.log(connections)/Math.log(2)));
	}
	
	@Override
	public HTTPServerConfig setMaxConnectionBits(int bits) {
		configStage.throwIfNot(BridgeConfigStage.DeclareConnections);
		if (bits<1) {
			throw new UnsupportedOperationException("Must support at least 1 connection");
		}
		if (bits>30) {
			throw new UnsupportedOperationException("Can not support "+(1L<<bits)+" connections");
		}
		
		this.maxConnectionBits = bits;
		return this;
	}

	@Override
	public HTTPServerConfig setEncryptionUnitsPerTrack(int value) {
		configStage.throwIfNot(BridgeConfigStage.DeclareConnections);
		this.encryptionUnitsPerTrack = value;
		return this;
	}

	@Override
	public HTTPServerConfig setDecryptionUnitsPerTrack(int value) {
		configStage.throwIfNot(BridgeConfigStage.DeclareConnections);
		this.decryptionUnitsPerTrack = value;
		return this;
	}

	@Override
	public HTTPServerConfig setConcurrentChannelsPerEncryptUnit(int value) {
		configStage.throwIfNot(BridgeConfigStage.DeclareConnections);
		this.concurrentChannelsPerEncryptUnit = value;
		return this;
	}

	@Override
	public HTTPServerConfig setConcurrentChannelsPerDecryptUnit(int value) {
		configStage.throwIfNot(BridgeConfigStage.DeclareConnections);
		this.concurrentChannelsPerDecryptUnit = value;
		return this;
	}

	public void finalizeDeclareConnections() {
		this.bindHost = NetGraphBuilder.bindHost(this.bindHost);
		this.configStage = BridgeConfigStage.DeclareBehavior;
	}
	
	@Override
	public ServerPipesConfig buildServerConfig(int tracks) {
		setTracks(tracks);
		return buildServerConfig();
		
	}

	public ServerPipesConfig buildServerConfig() {
		int incomingMsgFragCount = defaultComputedChunksCount();

		pcm.addConfig(new PipeConfig<HTTPRequestSchema>(HTTPRequestSchema.instance, 
						Math.max(incomingMsgFragCount-2, 2), 
						getMaxRequestSize()));
			
		pcm.ensureSize(ServerResponseSchema.class, 4, 512);
		
		return new ServerPipesConfig(
				logFile,
				isTLS(),
				getMaxConnectionBits(),
		   		this.tracks,
				getEncryptionUnitsPerTrack(),
				getConcurrentChannelsPerEncryptUnit(),
				getDecryptionUnitsPerTrack(),
				getConcurrentChannelsPerDecryptUnit(),				
				//one message might be broken into this many parts
				incomingMsgFragCount,
				getMaxRequestSize(),
				getMaxResponseSize(),
				pcm);
	}

	private int getMaxResponseSize() {
		return maxResponseSize;
	}

	private int defaultComputedChunksCount() {
		return Math.min(32, 2+(getMaxRequestSize()/1500));
	}

	public int getMaxRequestSize() {
		return this.maxRequestSize;
	}

	@Override
	public HTTPServerConfig logTraffic() {
		logFile = new LogFileConfig(LogFileConfig.defaultPath(),
				                    LogFileConfig.DEFAULT_COUNT, 
				                    LogFileConfig.DEFAULT_SIZE,
				                    false);
		//logging the full response often picks up binary and large data
		//this should only be turned on with an explicit true
		return this;
	}
	
	@Override
	public HTTPServerConfig logTraffic(boolean logResponse) {
		logFile = new LogFileConfig(LogFileConfig.defaultPath(),
				                    LogFileConfig.DEFAULT_COUNT, 
				                    LogFileConfig.DEFAULT_SIZE,
				                    logResponse);
		return this;
	}
	
	@Override
	public HTTPServerConfig logTraffic(String basePath, int fileCount, long fileSizeLimit, boolean logResponse) {
		logFile = new LogFileConfig(basePath,fileCount,fileSizeLimit,logResponse);
		return this;
	}

	@Override
	public HTTPServerConfig echoHeaders(int maxSingleHeaderLength, HTTPHeader... headers) {
		scs.headersToEcho(maxSingleHeaderLength, headers);
		return this;
		
	}

	public HTTPServerConfig setTracks(int tracks) {
		if (tracks>=1) {
			this.tracks = tracks;
		} else {
			throw new UnsupportedOperationException("Tracks must be 1 or more");
		}
		return this;
		
	}

	@Override
	public HTTPServerConfig setClientAuthRequired(boolean value) {
		if (serverTLS instanceof TLSCerts) {
			serverTLS = ((TLSCerts)serverTLS).clientAuthRequired(value);
		} else {
			throw new UnsupportedOperationException("This value should be set by passing TLSCertificates to setTLS ");
		}
		return this;
	}

	@Override
	public HTTPServerConfig setServiceName(String name) {
		serviceName = name;
		return this;
	}

	@Override
	public boolean requireClientAuth() {
		return serverTLS.clientAuthRequired();
	}

	@Override
	public String serviceName() {
		return serviceName;
	}
		
}
