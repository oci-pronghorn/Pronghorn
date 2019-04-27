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
	private int maxConnectionBits = 14; //default of 16K, TODO: Must be larger!!!
	//TODO: reduce connection memory to allow us to bump this up.
	//      the echo code is not yet implemented. 
	//      start time array could be shorter
	//      objects held by socket connection must be reviewed
	
	
	private int encryptionUnitsPerTrack = 1; //default of 1 per track or none without TLS
	private int decryptionUnitsPerTrack = 1; //default of 1 per track or none without TLS
	private int concurrentChannelsPerEncryptUnit = 2; //default 2, for low memory usage
	private int concurrentChannelsPerDecryptUnit = 2; //default 2, for low memory usage
	private TLSCertificates serverTLS = TLSCerts.define();
	private BridgeConfigStage configStage = BridgeConfigStage.Construction;
	
	private int maxRequestSize = 1<<9;//default of 1024	
	private final static int ICO_FILE_SIZE = 1<<11;//2k minimum to match ico file size, TODO: reduce ico to 1K?
	
	private int maxResponseSize = ICO_FILE_SIZE;//default of 2k
	
	//smaller to use less memory default use
	private int maxQueueIn = 16; ///from router to modules
	private int maxQueueOut = 8; //from orderSuper to ChannelWriter
	
	private int socketToParserBlocks = 4;
	private int minMemoryInputPipe = 1<<10; //1Kminum input pipe.
	
	public final PipeConfigManager pcmIn;
	public final PipeConfigManager pcmOut;
	
    int tracks = 1;//default 1, for low memory usage
	private LogFileConfig logFile;	

	private String serviceName = "Server";	
	private final ServerConnectionStruct scs;
	
	public HTTPServerConfigImpl(int bindPort, 
			                    PipeConfigManager pcmIn,
			                    PipeConfigManager pcmOut,
			                    StructRegistry recordTypeData) {
		this.bindPort = bindPort;
		if (bindPort<=0 || (bindPort>=(1<<16))) {
			throw new UnsupportedOperationException("invalid port "+bindPort);
		}

		this.pcmIn = pcmIn;
		this.pcmOut = pcmOut;
	
		//NOTE: this is set at the minimum sizes to support example, template and favicon.ico files
		this.pcmOut.ensureSize(ServerResponseSchema.class, 4, Math.max(ICO_FILE_SIZE, maxResponseSize));	
		
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
		pcmOut.ensureSize(ServerResponseSchema.class, 4, maxResponseSize);	
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

	@Override
	public HTTPServerConfig setMinimumInputPipeMemory(int bytes) {
		this.minMemoryInputPipe = Math.max(bytes, this.minMemoryInputPipe);
		return this;
	}
	
	public int getMinimumInputPipeMemory() {
		return this.minMemoryInputPipe;
	}
	
	@Override
	public HTTPServerConfig setMaxQueueIn(int maxQueueIn) {
		this.maxQueueIn = Math.max(this.maxQueueIn, maxQueueIn);		

		//for after router and before module, limited since all the data is cached in previous pipe
		//and we do not want to use all the memory here.
		this.pcmIn.ensureSize(HTTPRequestSchema.class, Math.max(maxQueueIn,1<<12), 0); 
       
		return this;
	}
	
	@Override
	public HTTPServerConfig setMaxQueueOut(int maxQueueOut) {
		this.maxQueueOut = Math.max(this.maxQueueOut, maxQueueOut);
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
				
		pcmOut.ensureSize(ServerResponseSchema.class, 4, 512);
		int blocksFromSocket = 32;
		int queueIn = 2; //2-1024
		int queueOut = 4; //4-256
				
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
				blocksFromSocket, minMemoryInputPipe,
				getMaxRequestSize(),
				getMaxResponseSize(),
				queueIn,
				queueOut,
				pcmIn,pcmOut);
	}

	public int getMaxResponseSize() {
		return maxResponseSize;
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
	
	public int tracks() {
		return tracks;
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
		return null==serverTLS ? false :serverTLS.clientAuthRequired();
	}

	@Override
	public String serviceName() {
		return serviceName;
	}

	public LogFileConfig logFileConfig() {
		return logFile;
	}

	@Override
	public int getMaxQueueIn() {
		return maxQueueIn;
	}

	@Override
	public int getMaxQueueOut() {
		return maxQueueOut;
	}

	@Override
	public HTTPServerConfig disableEPoll() {
		// -Djava.nio.channels.spi.SelectorProvider=sun.nio.ch.PollSelectorProvider
		System.setProperty("java.nio.channels.spi.SelectorProvider","sun.nio.ch.PollSelectorProvider");
		System.setProperty("epoll.native.enabled","false");
		return this;
	}

	public int getSocketToParserBlocks() {
		return socketToParserBlocks;
	}

		
}
