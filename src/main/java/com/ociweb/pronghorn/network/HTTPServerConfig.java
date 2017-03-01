package com.ociweb.pronghorn.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LoggerFactoryBinder;

import com.ociweb.pronghorn.network.config.HTTPContentTypeDefaults;
import com.ociweb.pronghorn.network.config.HTTPHeaderKeyDefaults;
import com.ociweb.pronghorn.network.config.HTTPRevisionDefaults;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.config.HTTPVerbDefaults;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.network.schema.ServerConnectionSchema;
import com.ociweb.pronghorn.pipe.PipeConfig;

public class HTTPServerConfig {
	
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
	
	public final int fromRouterMsg;
	public final int fromRouterBlob;
	public final int releaseMsg;
	
	
	public final HTTPSpecification<HTTPContentTypeDefaults, HTTPRevisionDefaults, HTTPVerbDefaults, HTTPHeaderKeyDefaults> httpSpec = HTTPSpecification.defaultSpec();
	

	private static final Logger logger = LoggerFactory.getLogger(HTTPServerConfig.class);
	
	
    public final PipeConfig<ReleaseSchema> releaseConfig;
    
    public final PipeConfig<ServerConnectionSchema> newConnectionsConfig;
	
	//Why? the router is helped with lots of extra room for write?  - may need to be bigger for posts.
    public final PipeConfig<HTTPRequestSchema> routerToModuleConfig;
    //byte buffer must remain small because we will have a lot of these for all the partial messages
    //TODO: if we get a series of very short messages this will fill up causing a hang. TODO: we can get parser to release and/or server reader to combine.
    public final PipeConfig<NetPayloadSchema> incomingDataConfig;
    //must be large to hold high volumes of throughput.  //NOTE: effeciency of supervisor stage controls how long this needs to be
    public final PipeConfig<NetPayloadSchema> toWraperConfig;
    //also used when the TLS is not enabled                 must be less than the outgoing buffer size of socket?
    public final PipeConfig<NetPayloadSchema> fromWraperConfig;
    public final PipeConfig<NetPayloadSchema> handshakeDataConfig;
	
	
	public HTTPServerConfig(boolean isLarge, boolean isTLS) {
		int cores = Runtime.getRuntime().availableProcessors();
		if (cores>64) {
			cores = cores >> 2;
		}
		if (cores<4) {
			cores = 4;
		}
		
		logger.info("cores in use {}", cores);
		
		if (isLarge) {
			
			
			maxPartialResponsesServer     = 32;//256;    // (big memory consumption!!) concurrent partial messages 
			maxConnectionBitsOnServer 	  = 20;       //1M open connections on server	    	
				
			serverInputMsg                = 20;
			serverInputBlobs              = 1<<14;
			
			serverMsgToEncrypt            = 512;
			serverBlobToEncrypt           = 1<<15;
			
			serverOutputMsg               = isTLS?128:512;
			serverBlobToWrite             = 1<<15;
			
			fromRouterMsg 				  = isTLS?512:2048;//impacts performance
			fromRouterBlob 				  = 1<<10;
			
			serverSocketWriters           = isTLS?1:4;
			releaseMsg                    = 512;
						
			serverRequestUnwrapUnits      = isTLS?4:2;  //server unwrap units - need more for handshaks and more for posts
			serverResponseWrapUnits 	  = isTLS?8:4;    //server wrap units
			serverPipesPerOutputEngine 	  = isTLS?4:8;//multiplier against server wrap units for max simultanus user responses.
			
		} else {	//small
			maxPartialResponsesServer     = 32;    //16 concurrent partial messages 
			maxConnectionBitsOnServer     = 12;    //4k  open connections on server	    	
		
			serverInputMsg                = isTLS? 8 : 8; 
			serverInputBlobs              = isTLS? 1<<15 : 1<<8;  
			
			serverMsgToEncrypt            = 128;
			serverBlobToEncrypt           = 1<<15; //Must NOT be smaller than the file write output (modules) ??? ONLY WHEN WE ARE USE ING TLS
			
			serverOutputMsg               = isTLS?32:128; //important for outgoing data and greatly impacts performance
			serverBlobToWrite             = 1<<15; //Must NOT be smaller than the file write output (modules), bigger values support combined writes when tls is off
			
			fromRouterMsg 			      = isTLS?256:2048; //impacts performance
			fromRouterBlob				  = 1<<7;
			
			serverSocketWriters           = 1;
			releaseMsg                    = 256;
						
			serverRequestUnwrapUnits      = isTLS?4:2;  //server unwrap units - need more for handshaks and more for posts
			serverResponseWrapUnits 	  = isTLS?8:4;    //server wrap units
			serverPipesPerOutputEngine 	  = isTLS?4:8;//multiplier against server wrap units for max simultanus user responses.
		}
		
		
		
	    releaseConfig = new PipeConfig<ReleaseSchema>(ReleaseSchema.instance,releaseMsg);
	    
	    newConnectionsConfig = new PipeConfig<ServerConnectionSchema>(ServerConnectionSchema.instance, 10);
		
		//Why? the router is helped with lots of extra room for write?  - may need to be bigger for posts.
	    routerToModuleConfig = new PipeConfig<HTTPRequestSchema>(HTTPRequestSchema.instance, fromRouterMsg, fromRouterBlob);///if payload is smaller than average file size will be slower
	  
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
