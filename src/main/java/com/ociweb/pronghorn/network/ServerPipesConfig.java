package com.ociweb.pronghorn.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.network.schema.ServerConnectionSchema;
import com.ociweb.pronghorn.pipe.PipeConfig;

public class ServerPipesConfig {
	
	public final int maxPartialResponsesServer;
	public final int maxConnectionBitsOnServer; 	
	public final int serverRequestUnwrapUnits;
	public final int serverResponseWrapUnits;
	public final int serverPipesPerOutputEngine;
	public final int serverSocketWriters;
	
	public final int serverInputBlobs; 
	private int serverBlobToWrite; //may need to grow if requested upon startup.
	
	public final int serverInputMsg;
	
	public final int serverOutputMsg;
	
	public final int fromProcessorCount;
	public final int fromProcessorBlob;
	public final int releaseMsg;

	private static final Logger logger = LoggerFactory.getLogger(ServerPipesConfig.class);
	
	
    public final PipeConfig<ReleaseSchema> releaseConfig;
    
    public final PipeConfig<ServerConnectionSchema> newConnectionsConfig;
	    
    //byte buffer must remain small because we will have a lot of these for all the partial messages
    public final PipeConfig<NetPayloadSchema> incomingDataConfig;

    //also used when the TLS is not enabled                 must be less than the outgoing buffer size of socket?
    private PipeConfig<NetPayloadSchema> fromOrderWraperConfig;
    
    
    public final PipeConfig<NetPayloadSchema> handshakeDataConfig;
	
    public final int processorCount;
	public int writeBufferMultiplier;
    
	public ServerPipesConfig(boolean isLarge, boolean isTLS) {
		this(isLarge,isTLS,-1);
	}
	
	public ServerPipesConfig(boolean isLarge, boolean isTLS, int processors) {
		int cores = Runtime.getRuntime().availableProcessors();
		if (cores>64) {
			cores = cores >> 2;
		}
		if (cores<4) {
			cores = 4;
		}
		if (1==processors && !isLarge && !isTLS){
			cores = 1;
		}
		
		processorCount = processors > 0? processors : (isLarge ? (isTLS?4:8) : 2);
		
		//logger.info("cores in use {}", cores);
		
		if (isLarge) {
						
			maxPartialResponsesServer     = 32;//256;    // (big memory consumption!!) concurrent partial messages 
			maxConnectionBitsOnServer 	  = 20;       //1M open connections on server	    	
				
			serverInputMsg                = isTLS? 8 : 64; 
			serverInputBlobs              = 1<<14;

			serverOutputMsg               = isTLS? 32:512;
			serverBlobToWrite             = 1<<15;
			
			fromProcessorCount 			  = isTLS?512:2048;//impacts performance
			fromProcessorBlob 			  = 1<<10;
			
			serverSocketWriters           = isTLS?1:2;
			releaseMsg                    = 1024;
						
			serverRequestUnwrapUnits      = isTLS?4:2;  //server unwrap units - need more for handshaks and more for posts
			serverResponseWrapUnits 	  = isTLS?8:4;    //server wrap units
			serverPipesPerOutputEngine 	  = isTLS?4:8;//multiplier against server wrap units for max simultanus user responses.
			writeBufferMultiplier         = 16;
		} else {	//small
			maxPartialResponsesServer     = processors==1 ? 2 : 4;    //4 concurrent partial messages 
			maxConnectionBitsOnServer     = 12;    //4k  open connections on server	    	
		
			serverInputMsg                = isTLS? 8 : 48; 
			serverInputBlobs              = isTLS? 1<<15 : 1<<8;  

			serverOutputMsg               = isTLS?8:16; //important for outgoing data and greatly impacts performance
			serverBlobToWrite             = 1<<15; //Must NOT be smaller than the file write output (modules), bigger values support combined writes when tls is off
			
			fromProcessorCount 			  = isTLS?64:256; //impacts performance
			fromProcessorBlob			  = 1<<7;
			
			serverSocketWriters           = 1;
			releaseMsg                    = 1024;//256;
						
			serverRequestUnwrapUnits      = isTLS?2:1;  //server unwrap units - need more for handshaks and more for posts
			serverResponseWrapUnits 	  = processors==1?1:(isTLS?4:2);    //server wrap units
			serverPipesPerOutputEngine 	  = processors==1?1:(isTLS?2:1);//multiplier against server wrap units for max simultanus user responses.
			writeBufferMultiplier         = 4;
		}
		
				
	    releaseConfig = new PipeConfig<ReleaseSchema>(ReleaseSchema.instance,releaseMsg);
	    
	    newConnectionsConfig = new PipeConfig<ServerConnectionSchema>(ServerConnectionSchema.instance, 100);
	    
	    //byte buffer must remain small because we will have a lot of these for all the partial messages
	    incomingDataConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, serverInputMsg, serverInputBlobs);//make larger if we are suporting posts. 1<<20); //Make same as network buffer in bytes!??   Do not make to large or latency goes up

	    handshakeDataConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, maxPartialResponsesServer>>1, 1<<15); //must be 1<<15 at a minimum for handshake
	    	    
	}
	
	public void ensureServerCanWrite(int length) {
		serverBlobToWrite =  Math.max(serverBlobToWrite, length);
	}
	
	public PipeConfig<NetPayloadSchema> orderWrapConfig() {
		if (null==fromOrderWraperConfig) {
			//also used when the TLS is not enabled                 must be less than the outgoing buffer size of socket?
			fromOrderWraperConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance,
					                  serverOutputMsg, serverBlobToWrite);  //must be 1<<15 at a minimum for handshake
		}		
		return fromOrderWraperConfig;
	}
	
	
}
