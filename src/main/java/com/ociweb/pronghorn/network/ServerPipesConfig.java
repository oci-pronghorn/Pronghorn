package com.ociweb.pronghorn.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.network.schema.ServerConnectionSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.PipeConfigManager;

public class ServerPipesConfig {
	
	
	public final int serverRequestUnwrapUnits;
	public final int serverResponseWrapUnitsAndOutputs;
	public final int serverPipesPerOutputEngine;
	public final int serverSocketWriters;
	public final LogFileConfig logFile;
	public final int serverOutputMsg;
	
	public final int releaseMsg;
	
	public final int moduleParallelism; //scale of compute modules
	public final int maxConnectionBitsOnServer; //max connected users
	public final int maxConcurrentInputs; //concurrent actions count
	public final int maxConcurrentOutputs;
	
	public final int fromRouterToModuleCount;
	public final int fromRouterToModuleBlob; //may grow based on largest post required

	private int serverBlobToWrite; //may need to grow based on largest payload required

	private static final Logger logger = LoggerFactory.getLogger(ServerPipesConfig.class);
	
	
    //byte buffer must remain small because we will have a lot of these for all the partial messages
    //also used when the TLS is not enabled                 must be less than the outgoing buffer size of socket?
    private      PipeConfig<NetPayloadSchema> fromOrderWraperConfig;     
  
    
    //this cannot be put in the PCM or it will collide..
    public final PipeConfig<NetPayloadSchema> incomingDataConfig; //also meets handshake req when TLS is used

	
    //This list of configs is kept inside the PCM, some of the above are to be moved down here...
    //final PipeConfig<NetPayloadSchema> fromOrderWraperConfig;     
    //final PipeConfig<ServerConnectionSchema> newConnectionsConfig;	    
    //final PipeConfig<ReleaseSchema> releaseConfig;    
	//final PipeConfig<HTTPRequestSchema> routerToModuleConfig
	//final PipeConfig<ServerResponseSchema> config = ServerResponseSchema.instance.newPipeConfig(4, 512);	
    public final PipeConfigManager pcm; //TODO: move all the above configs to this PCM...
    
	public int writeBufferMultiplier;

	public ServerPipesConfig(LogFileConfig logFile, boolean isTLS, 
							 int maxConnectionBits,
							 int tracks,
							 int encryptUnitsPerTrack,
							 int concurrentChannelsPerEncryptUnit,
							 int decryptUnitsPerTrack,
							 int concurrentChannelsPerDecryptUnit, 
							 int partialPartsIn,  //make larger for many fragments
							 int maxRequestSize, //make larger for large posts
							 int maxResponseSize,
							 PipeConfigManager pcm				 
			) {
	
		if (isTLS && (maxRequestSize< (1<<15))) {
			maxRequestSize = (1<<15);//TLS requires this larger payload size
		}
		if (isTLS && (maxResponseSize< (1<<15))) {
			maxResponseSize = (1<<15);//TLS requires this larger payload size
		}
		
		//keep the waiting packets from getting out of hand, limit this value
		partialPartsIn = Math.min(32, partialPartsIn);
		

		//these may need to be exposed.. they can impact performance
		this.fromRouterToModuleCount   = 4; //count of messages from router to module	    
		this.serverOutputMsg           = 16; //count of outgoing responses to writer
	    //largest file to be cached in file server
   
		this.pcm = pcm;
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

	    this.serverBlobToWrite               = maxResponseSize; //Must NOT be smaller than the file write output (modules), bigger values support combined writes when tls is off
		int targetServerWriteBufferSize = 1<<23;
		this.writeBufferMultiplier           = targetServerWriteBufferSize/ serverBlobToWrite; //write buffer on server
		
		this.releaseMsg                      = 2048;
				
		pcm.addConfig(new PipeConfig<ReleaseSchema>(ReleaseSchema.instance,releaseMsg));

	    //byte buffer must remain small because we will have a lot of these for all the partial messages
		//however for large posts we make this large for fast data reading
		//in addition this MUST be 1<15 in var size when TLS is in use.
		this.incomingDataConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance,
	    								partialPartsIn, 
	    								maxRequestSize);
	
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
