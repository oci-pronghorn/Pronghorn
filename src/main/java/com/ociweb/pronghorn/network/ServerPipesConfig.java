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
	public final int serverBlobToEncrypt;
	public final int serverBlobToWrite;
	public final int serverInputMsg;
	
	public final int serverMsgToEncrypt;
	public final int serverOutputMsg;
	
	public final int fromProcessorCount;
	public final int fromProcessorBlob;
	public final int releaseMsg;

	private static final Logger logger = LoggerFactory.getLogger(ServerPipesConfig.class);
	
	
    public final PipeConfig<ReleaseSchema> releaseConfig;
    
    public final PipeConfig<ServerConnectionSchema> newConnectionsConfig;
	

    
    //byte buffer must remain small because we will have a lot of these for all the partial messages
    //TODO: if we get a series of very short messages this will fill up causing a hang. TODO: we can get parser to release and/or server reader to combine.
    public final PipeConfig<NetPayloadSchema> incomingDataConfig;
    //must be large to hold high volumes of throughput.  //NOTE: effeciency of supervisor stage controls how long this needs to be
    public final PipeConfig<NetPayloadSchema> toWraperConfig;
    //also used when the TLS is not enabled                 must be less than the outgoing buffer size of socket?
    public final PipeConfig<NetPayloadSchema> fromWraperConfig;
    public final PipeConfig<NetPayloadSchema> handshakeDataConfig;
	
    public final int processorCount;
	public int writeBufferMultiplier;
    
	public ServerPipesConfig(boolean isLarge, boolean isTLS) {
		int cores = Runtime.getRuntime().availableProcessors();
		if (cores>64) {
			cores = cores >> 2;
		}
		if (cores<4) {
			cores = 4;
		}
		
		processorCount = isLarge ? (isTLS?4:16) : 4;
		
		logger.trace("cores in use {}", cores);
		
		if (isLarge) {
						
			maxPartialResponsesServer     = 32;//256;    // (big memory consumption!!) concurrent partial messages 
			maxConnectionBitsOnServer 	  = 20;       //1M open connections on server	    	
				
			serverInputMsg                = isTLS? 8 : 64; 
			serverInputBlobs              = 1<<14;
			
			serverMsgToEncrypt            = 512;
			serverBlobToEncrypt           = 1<<15;
			
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
			maxPartialResponsesServer     = 8;    //8 concurrent partial messages 
			maxConnectionBitsOnServer     = 12;    //4k  open connections on server	    	
		
			serverInputMsg                = isTLS? 8 : 48; 
			serverInputBlobs              = isTLS? 1<<15 : 1<<8;  
			
			serverMsgToEncrypt            = 8; //may cause backing up and one side processing?
			serverBlobToEncrypt           = 1<<15; //Must NOT be smaller than the file write output (modules) ??? ONLY WHEN WE ARE USE ING TLS
			
			serverOutputMsg               = isTLS?16:32; //important for outgoing data and greatly impacts performance
			serverBlobToWrite             = 1<<15; //Must NOT be smaller than the file write output (modules), bigger values support combined writes when tls is off
			
			fromProcessorCount 			  = isTLS?64:256; //impacts performance
			fromProcessorBlob			  = 1<<7;
			
			serverSocketWriters           = 1;
			releaseMsg                    = 1024;//256;
						
			serverRequestUnwrapUnits      = isTLS?2:1;  //server unwrap units - need more for handshaks and more for posts
			serverResponseWrapUnits 	  = isTLS?8:4;    //server wrap units
			serverPipesPerOutputEngine 	  = isTLS?2:2;//multiplier against server wrap units for max simultanus user responses.
			writeBufferMultiplier         = 4;
		}
		
		
		
	    releaseConfig = new PipeConfig<ReleaseSchema>(ReleaseSchema.instance,releaseMsg);
	    
	    newConnectionsConfig = new PipeConfig<ServerConnectionSchema>(ServerConnectionSchema.instance, 100);
		
    
	    //byte buffer must remain small because we will have a lot of these for all the partial messages
	    //TODO: if we get a series of very short messages this will fill up causing a hang. TODO: we can get parser to release and/or server reader to combine.
	    incomingDataConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, serverInputMsg, serverInputBlobs);//make larger if we are suporting posts. 1<<20); //Make same as network buffer in bytes!??   Do not make to large or latency goes up
	    
	    //must be large to hold high volumes of throughput.  //NOTE: effeciency of supervisor stage controls how long this needs to be
	    toWraperConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, serverMsgToEncrypt, serverBlobToEncrypt); //from super should be 2x of super input //must be 1<<15 at a minimum for handshake
	    
	    //also used when the TLS is not enabled                 must be less than the outgoing buffer size of socket?
	    fromWraperConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, serverOutputMsg, serverBlobToWrite);  //must be 1<<15 at a minimum for handshake
	            
	    handshakeDataConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, maxPartialResponsesServer>>1, 1<<15); //must be 1<<15 at a minimum for handshake
	    
	    

	}
	
}
