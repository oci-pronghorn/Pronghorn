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
	
	public final int releaseMsg;
	
	public final int moduleParallelism; //scale of compute modules
	public final int maxConnectionBitsOnServer; //max connected users
	public final int maxConcurrentInputs; //concurrent actions count
	public final int maxConcurrentOutputs;

	public final int fromRouterToModuleCount;
	public final int fromRouterToModuleBlob; //may grow based on largest post required

	private static final Logger logger = LoggerFactory.getLogger(ServerPipesConfig.class);
	
    public final PipeConfigManager pcmIn; 
    public final PipeConfigManager pcmOut; 
    
	public ServerPipesConfig(LogFileConfig logFile, boolean isTLS, 
							 int maxConnectionBits,
							 int tracks,
							 int encryptUnitsPerTrack,
							 int concurrentChannelsPerEncryptUnit,
							 int decryptUnitsPerTrack,
							 int concurrentChannelsPerDecryptUnit, 
							 int partialPartsIn,  //make larger for many fragments
							 int partsInBuffer, //full buffer from socket to parser
							 int maxRequestSize, //make larger for large posts
							 int maxResponseSize,
							 int queueLengthIn, //router to modules
							 int queueLengthOut, // from superOrder to channel writer
							 PipeConfigManager pcmIn,
							 PipeConfigManager pcmOut
							 
			) {
	
		if (isTLS && (maxRequestSize< (SSLUtil.MinTLSBlock))) {
			maxRequestSize = (SSLUtil.MinTLSBlock);//TLS requires this larger payload size
			this.ensureServerCanWrite(SSLUtil.MinTLSBlock);
		}
	
		if (isTLS && (maxResponseSize< (SSLUtil.MinTLSBlock))) {
			maxResponseSize = (SSLUtil.MinTLSBlock);//TLS requires this larger payload size
		}

		if (partialPartsIn<8) {
			logger.warn("network buffer is very small ({}) and server is likely to drop incoming data.",partialPartsIn);
		}

		this.fromRouterToModuleCount = queueLengthIn; // 2 - 1024
		this.pcmIn = pcmIn;
		this.pcmOut = pcmOut;
		if (pcmIn==null || pcmOut==null) {
			throw new NullPointerException();
		}
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
		
	    //Need more writers than readers, the OS assumes many threads respond but few read.
	    this.serverSocketWriters = 2*Math.max(1,NetGraphBuilder.computeGroupsFromTracks(moduleParallelism, isTLS));

	    //defaults which are updated by method calls
	    this.fromRouterToModuleBlob		    = Math.max(maxRequestSize, 1<<9); //impacts post performance
	    		
		this.releaseMsg                      = 2048;
				
		pcmIn.ensureSize(ReleaseSchema.class,  releaseMsg, 0);
		pcmOut.ensureSize(ReleaseSchema.class,  releaseMsg, 0);

		int blockSize = partsInBuffer/partialPartsIn;
		pcmIn.ensureSize(NetPayloadSchema.class, partialPartsIn, 
				Math.max(maxRequestSize, 
						 isTLS ? (Math.max(blockSize, SSLUtil.MinTLSBlock)) : blockSize				
						)
				);
			
		
		pcmIn.ensureSize(HTTPRequestSchema.class, queueLengthIn, fromRouterToModuleBlob);
		
		//maxResponseSize Must NOT be smaller than the file write output (modules), bigger values support combined writes when tls is off
		pcmOut.ensureSize(NetPayloadSchema.class, queueLengthOut, maxResponseSize);

	}
	
	public void ensureServerCanWrite(int length) {
		if (null!=pcmOut) {
			pcmOut.ensureSize(NetPayloadSchema.class, 0, length);
		}
	}
	
	
}
