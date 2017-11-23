package com.ociweb.pronghorn.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.network.schema.ServerConnectionSchema;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class ServerPipesConfig {
	
	
	public final int serverRequestUnwrapUnits;
	public final int serverResponseWrapUnitsAndOutputs;
	public final int serverPipesPerOutputEngine;
	public final int serverSocketWriters;
	
	public final int serverOutputMsg;
	
	public final int fromRouterToModuleCount;
	public final int fromRouterToModuleBlob;
	public final int releaseMsg;
	
	public final int moduleParallelism; //scale of compute modules
	public final int maxConnectionBitsOnServer; //max connected users
	public final int maxConcurrentInputs; //concurrent actions count
	public final int maxConcurrentOutputs;

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
  
	
	public ServerPipesConfig(boolean isTLS, 
							 int maxConnectionBits,
							 int tracks,
							 int encryptUnitsPerTrack,
							 int concurrentChannelsPerEncryptUnit,
							 int decryptUnitsPerTrack,
							 int concurrentChannelsPerDecryptUnit) {
		
		//these may need to be exposed..
	    fromRouterToModuleCount 	  = 1<<8;//impacts performance
	    //largest file to be cached in file server
		fromRouterToModuleBlob		  = 1<<12;//impacts performance

	    serverOutputMsg               = isTLS? 32: 1<<8; //512;//important for outgoing data and greatly impacts performance
	    int serverInputMsg            = isTLS? 8 : 1<<8;
	    									//TODO: set based on the socket values on the server??
	    int serverInputBlobs          = isTLS? 1<<15 : 1<<8; //TODO: bump up for large posts 

		
		moduleParallelism = tracks;
		maxConnectionBitsOnServer = maxConnectionBits;
		
		serverResponseWrapUnitsAndOutputs = encryptUnitsPerTrack*moduleParallelism;
		serverPipesPerOutputEngine = concurrentChannelsPerEncryptUnit;	
		maxConcurrentOutputs = serverPipesPerOutputEngine*serverResponseWrapUnitsAndOutputs;
		
		/////////		
		//Note how each value builds on the next.		
		//		int outputWrapUnits = 4;//must be divisible by moduleParallelism
		//		int outputConcurrency = 12; //must be divisible by wrap units and moduleParallelism
		//		serverPipesPerOutputEngine = outputConcurrency/outputWrapUnits;
		//		serverResponseWrapUnitsAndOutputs = outputWrapUnits;
		//////////////////////
		
		////////
		//Note the unwrap input behaves the same as the above wrapped output
		serverRequestUnwrapUnits = decryptUnitsPerTrack*moduleParallelism;
		maxConcurrentInputs = serverRequestUnwrapUnits*concurrentChannelsPerDecryptUnit;
		////////
		
		// do not need multiple writers until we have giant load
		serverSocketWriters       = (moduleParallelism >= 4) ? (isTLS?1:2) : 1;
		writeBufferMultiplier     = (moduleParallelism >= 4) ? 16 : 4; //write buffer on server



		
		serverBlobToWrite             = 1<<15; //Must NOT be smaller than the file write output (modules), bigger values support combined writes when tls is off
		releaseMsg                    = 1024;
				
	    releaseConfig = new PipeConfig<ReleaseSchema>(ReleaseSchema.instance,releaseMsg);
	    
	    newConnectionsConfig = new PipeConfig<ServerConnectionSchema>(ServerConnectionSchema.instance, 100);
	    

	    //byte buffer must remain small because we will have a lot of these for all the partial messages
	    incomingDataConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance,
	    		                                             serverInputMsg, 
	    		                                             serverInputBlobs);//make larger if we are suporting posts. 1<<20); //Make same as network buffer in bytes!??   Do not make to large or latency goes up

	    handshakeDataConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, 
	    		Math.max(maxConcurrentInputs>>1,4), 1<<15); //must be 1<<15 at a minimum for handshake
	    	    
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
