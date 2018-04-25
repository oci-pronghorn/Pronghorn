package com.ociweb.pronghorn.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.network.schema.ServerConnectionSchema;
import com.ociweb.pronghorn.pipe.PipeConfig;

public class ServerPipesConfig {
	
	
	public final int serverRequestUnwrapUnits;
	public final int serverResponseWrapUnitsAndOutputs;
	public final int serverPipesPerOutputEngine;
	public final int serverSocketWriters;
	public final LogFileConfig logFile;
	public final int serverOutputMsg;
	
	public final int fromRouterToModuleCount;
	public final int releaseMsg;
	
	public final int moduleParallelism; //scale of compute modules
	public final int maxConnectionBitsOnServer; //max connected users
	public final int maxConcurrentInputs; //concurrent actions count
	public final int maxConcurrentOutputs;

	
	public int fromRouterToModuleBlob; //may grow based on largest post required
	private int serverBlobToWrite; //may need to grow based on largest payload required

	private static final Logger logger = LoggerFactory.getLogger(ServerPipesConfig.class);
	
	
    public final PipeConfig<ReleaseSchema> releaseConfig;
    
    public final PipeConfig<ServerConnectionSchema> newConnectionsConfig;
	    
    //byte buffer must remain small because we will have a lot of these for all the partial messages
    public final PipeConfig<NetPayloadSchema> incomingDataConfig;

    //also used when the TLS is not enabled                 must be less than the outgoing buffer size of socket?
    private PipeConfig<NetPayloadSchema> fromOrderWraperConfig;
        
    public final PipeConfig<NetPayloadSchema> handshakeDataConfig;
	
	public int writeBufferMultiplier;
  
	public ServerPipesConfig(
			LogFileConfig logFile,
			 boolean isTLS, 
			 int maxConnectionBits,
			 int tracks,
			 int encryptUnitsPerTrack,
			 int concurrentChannelsPerEncryptUnit,
			 int decryptUnitsPerTrack,
			 int concurrentChannelsPerDecryptUnit
			 ) {
		this(logFile, isTLS, maxConnectionBits, tracks,
				encryptUnitsPerTrack, concurrentChannelsPerEncryptUnit,
				decryptUnitsPerTrack, concurrentChannelsPerDecryptUnit,
				4,512);
	}
	
	public ServerPipesConfig(LogFileConfig logFile, boolean isTLS, 
							 int maxConnectionBits,
							 int tracks,
							 int encryptUnitsPerTrack,
							 int concurrentChannelsPerEncryptUnit,
							 int decryptUnitsPerTrack,
							 int concurrentChannelsPerDecryptUnit, 
							 int partialPartsIn,  //make larger for many fragments
							 int maxRequestSize //make larger for large posts
							 ) {
		
		if (isTLS && (maxRequestSize< (1<<15))) {
			maxRequestSize = (1<<15);//TLS requires this larger payload size
		}

		//these may need to be exposed.. they can impact performance
		this.fromRouterToModuleCount   = 4; //count of messages from router to module	    
		this.serverOutputMsg           = 16; //count of outgoing responses to writer
	    //largest file to be cached in file server
   
	    this.logFile = logFile;
	    this.moduleParallelism = tracks;
	    this.maxConnectionBitsOnServer = maxConnectionBits;
		
	    this.serverResponseWrapUnitsAndOutputs = encryptUnitsPerTrack*moduleParallelism;
	    this.serverPipesPerOutputEngine = concurrentChannelsPerEncryptUnit;	
	    this.maxConcurrentOutputs = serverPipesPerOutputEngine*serverResponseWrapUnitsAndOutputs;
		
		/////////		
		//Note how each value builds on the next.		
		//		int outputWrapUnits = 4;//must be divisible by moduleParallelism
		//		int outputConcurrency = 12; //must be divisible by wrap units and moduleParallelism
		//		serverPipesPerOutputEngine = outputConcurrency/outputWrapUnits;
		//		serverResponseWrapUnitsAndOutputs = outputWrapUnits;
		//////////////////////
		
		////////
		//Note the unwrap input behaves the same as the above wrapped output
	    this.serverRequestUnwrapUnits = decryptUnitsPerTrack*moduleParallelism;
	    this.maxConcurrentInputs = serverRequestUnwrapUnits*concurrentChannelsPerDecryptUnit;
		////////
		
		// do not need multiple writers until we have giant load
	    this.serverSocketWriters       = (moduleParallelism >= 4) ? (isTLS?1:2) : 1;


		//defaults which are updated by method calls
	    this.fromRouterToModuleBlob		    = Math.max(maxRequestSize, 1<<9); //impacts post performance
	    this.serverBlobToWrite               = 1<<15; //Must NOT be smaller than the file write output (modules), bigger values support combined writes when tls is off
		int targetServerWriteBufferSize = 1<<23;
		this.writeBufferMultiplier           = targetServerWriteBufferSize/ serverBlobToWrite; //write buffer on server
		
		this.releaseMsg                      = 2048;
				
		this.releaseConfig = new PipeConfig<ReleaseSchema>(ReleaseSchema.instance,releaseMsg);
	    
		this.newConnectionsConfig = new PipeConfig<ServerConnectionSchema>(ServerConnectionSchema.instance, 100);
	    

	    //byte buffer must remain small because we will have a lot of these for all the partial messages
		this.incomingDataConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance,
	    								partialPartsIn, 
	    								maxRequestSize);//make larger if we are suporting posts. 1<<20); //Make same as network buffer in bytes!??   Do not make to large or latency goes up

		this.handshakeDataConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, 
	    		Math.max(maxConcurrentInputs>>1,4), 1<<15); //must be 1<<15 at a minimum for handshake
	    	    
	}

	public void ensureServerCanRead(int length) {
		fromRouterToModuleBlob =  Math.max(fromRouterToModuleBlob, length);
	}
	
	public void ensureServerCanWrite(int length) {
		serverBlobToWrite =  Math.max(serverBlobToWrite, length);
	}
	
	public PipeConfig<NetPayloadSchema> orderWrapConfig() {
		if (null==fromOrderWraperConfig) {
			//also used when the TLS is not enabled                 must be less than the outgoing buffer size of socket?
			fromOrderWraperConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance,
					                  serverOutputMsg, 
					                  serverBlobToWrite);  //must be 1<<15 at a minimum for handshake
		}		
		return fromOrderWraperConfig;
	}
	
	
}
