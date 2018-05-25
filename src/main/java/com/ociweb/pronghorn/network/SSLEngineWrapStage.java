package com.ociweb.pronghorn.network;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.Pipe;

import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class SSLEngineWrapStage extends PronghornStage {

	private final SSLConnectionHolder      ccm;
	private final Pipe<NetPayloadSchema>[] encryptedContent; 
	private final Pipe<NetPayloadSchema>[] plainContent;
	private ByteBuffer[]                   secureBuffers;
	private Logger                         logger = LoggerFactory.getLogger(SSLEngineWrapStage.class);
	
	private long          totalNS;
	private int           calls;
	private final boolean isServer;
	private int           shutdownCount;
	private static final int     SIZE_HANDSHAKE_AND_DISCONNECT = Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_DISCONNECT_203)
														+Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_DISCONNECT_203);

	private final int min = (1<<15)-1;
	
	protected SSLEngineWrapStage(GraphManager graphManager, SSLConnectionHolder ccm, boolean isServer,
			                     Pipe<NetPayloadSchema>[] plainContent, Pipe<NetPayloadSchema>[] encryptedContent) {
		
		super(graphManager, plainContent, encryptedContent);

		shutdownCount = plainContent.length;		
		
		this.ccm = ccm;
		this.encryptedContent = encryptedContent;
		this.plainContent = plainContent;
		this.isServer = isServer;
		assert(encryptedContent.length==plainContent.length);
		
		int c = encryptedContent.length;
		secureBuffers = new ByteBuffer[c];
		while (--c>=0) {		
						
			int encLen = encryptedContent[c].maxVarLen;
			int plnLen = plainContent[c].maxVarLen;
			
			int bufferSize = Math.max(encLen,plnLen);
			if (bufferSize<min) {
				if (encLen<min) {
					encryptedContent[c].creationStack();
				}
				if (plnLen<min) {
					plainContent[c].creationStack();
				}
				throw new UnsupportedOperationException("ERROR: buffer size must be larger than "+min+" but found Enc:"+encLen+" Pln:"+plnLen);
			}
		}		
			
		GraphManager.addNota(graphManager, GraphManager.HEAVY_COMPUTE, GraphManager.HEAVY_COMPUTE, this);

		GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "bisque1", this);
		
	}

	@Override
	public void startup() {
		
		//must allocate buffers for the out of order content 
		int c = encryptedContent.length;
		secureBuffers = new ByteBuffer[c];
		while (--c>=0) {						
			int bufferSize = Math.max(encryptedContent[c].maxVarLen,plainContent[c].maxVarLen);			
			secureBuffers[c] = ByteBuffer.allocateDirect(bufferSize);
		}				
		
	}
	
	@Override
	public void run() {
		long start = System.nanoTime();
		calls++;
		
		boolean didWork;
		
		do {
			didWork = false;
			int i = encryptedContent.length;
			while (--i >= 0) {
							
				final Pipe<NetPayloadSchema> targetPipe = encryptedContent[i];
				final Pipe<NetPayloadSchema> sourcePipe = plainContent[i];

		        
//				//no content to wrap on server
//				if (Pipe.contentRemaining(sourcePipe)>0) {
//					System.err.println(sourcePipe);
////					System.err.println("input data to be wrapped "+isServer+" "+i+" source "+sourcePipe.contentRemaining(sourcePipe));
//				}
//				if (Pipe.contentRemaining(targetPipe)>0) {
//					System.err.println("output data wrapped "+isServer+"  "+i+" target "+targetPipe.contentRemaining(targetPipe));
//				}
				
				try {
					
					didWork |= SSLUtil.engineWrap(ccm, sourcePipe, targetPipe, secureBuffers[i], isServer);	
			
				} catch (Throwable t) {
					t.printStackTrace();
					requestShutdown();
					System.exit(0);
					return;
				}
		
				
				/////////////////////////////////////
				//close the connection logic
				//if connection is open we must finish the handshake.
				////////////////////////////////////
				if (Pipe.hasRoomForWrite(targetPipe, SIZE_HANDSHAKE_AND_DISCONNECT)
					&& Pipe.peekMsg(sourcePipe, NetPayloadSchema.MSG_DISCONNECT_203)) {
					
					//logger.info("WRAP FOUND DISCONNECT MESSAGE A server:"+isServer);
					
					int msgId = Pipe.takeMsgIdx(sourcePipe);
					assert(NetPayloadSchema.MSG_DISCONNECT_203 == msgId);
										
					long connectionId = Pipe.takeLong(sourcePipe); //NetPayloadSchema.MSG_DISCONNECT_203_FIELD_CONNECTIONID_201);
					long time = System.currentTimeMillis();
					
					BaseConnection connection = ccm.connectionForSessionId(connectionId);
					if (null!=connection) {
						SSLUtil.handShakeWrapIfNeeded(connection, targetPipe, secureBuffers[i], isServer, time);					
					}				
					
					Pipe.addMsgIdx(targetPipe, NetPayloadSchema.MSG_DISCONNECT_203);
					Pipe.addLongValue(connectionId, targetPipe); // NetPayloadSchema.MSG_DISCONNECT_203_FIELD_CONNECTIONID_201, connectionId);
					
					Pipe.confirmLowLevelWrite(targetPipe, Pipe.sizeOf(targetPipe, NetPayloadSchema.MSG_DISCONNECT_203));
					Pipe.publishWrites(targetPipe);
										
					Pipe.confirmLowLevelRead(sourcePipe, Pipe.sizeOf(sourcePipe, msgId));
					Pipe.releaseReadLock(sourcePipe);
				} 
				
				///////////////////////////
				//shutdown this stage logic
				///////////////////////////
				if (Pipe.peekMsg(sourcePipe, -1)) {
					int msg = Pipe.takeInt(sourcePipe);
					assert(-1 == msg);
					Pipe.confirmLowLevelRead(sourcePipe, Pipe.EOF_SIZE);
					Pipe.releaseReadLock(sourcePipe);
					if (--shutdownCount<=0) {
						requestShutdown();
						break;
					}
				}
				
			}
			
		} while (didWork && shutdownCount>0);//only exit if we pass over all pipes and there is no work to do.
		
		totalNS += System.nanoTime()-start;
		
	}

    @Override
    public void shutdown() {
    	    	
    	int j = encryptedContent.length;
    	while (--j>=0) {
    		try {
    			Pipe.publishEOF(encryptedContent[j]);
    		} catch (NullPointerException npe) {
    			//ignore, we are shutting down and never started up first.
    		}
    	}    	
    	
    	boolean debug=false;
    	
    	if (debug) {    	
	    	long totalBytesOfContent = 0;
	    	int i = plainContent.length;
	    	while (--i>=0) {
	    		totalBytesOfContent += Pipe.getBlobRingTailPosition(plainContent[i]);
	    	}
	    	
	
			float mbps = (float) ( (8_000d*totalBytesOfContent)/ (double)totalNS);
	    	logger.info("wrapped total bytes "+totalBytesOfContent+"    "+mbps+"mbps");
	    	logger.info("wrapped total time "+totalNS+"ns total callls "+calls);
    	}
    	
    }
	
}
