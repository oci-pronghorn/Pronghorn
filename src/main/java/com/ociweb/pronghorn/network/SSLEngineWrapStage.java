package com.ociweb.pronghorn.network;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class SSLEngineWrapStage extends PronghornStage {

	private final SSLConnectionHolder        ccm;
	private final Pipe<NetPayloadSchema>[] encryptedContent; 
	private final Pipe<NetPayloadSchema>[] plainContent;
	private ByteBuffer[]                         secureBuffers;
	private Logger                               logger = LoggerFactory.getLogger(SSLEngineWrapStage.class);
	
	private long          totalNS;
	private int           calls;
	private final boolean isServer;
	private int           shutdownCount;
	private final int     SIZE_HANDSHAKE_AND_DISCONNECT;
	private final int     groupId;
	
	
	protected SSLEngineWrapStage(GraphManager graphManager, SSLConnectionHolder ccm, boolean isServer,
			                     Pipe<NetPayloadSchema>[] plainContent, Pipe<NetPayloadSchema>[] encryptedContent, int  groupId) {
		
		super(graphManager, plainContent, encryptedContent);

		shutdownCount = plainContent.length;
		SIZE_HANDSHAKE_AND_DISCONNECT = Pipe.sizeOf(encryptedContent[0], NetPayloadSchema.MSG_DISCONNECT_203)
				+Pipe.sizeOf(encryptedContent[0], NetPayloadSchema.MSG_DISCONNECT_203);
		
		logger.info("WRAP FOUND DISCONNECT MESSAGE B");
		
		this.ccm = ccm;
		this.encryptedContent = encryptedContent;
		this.plainContent = plainContent;
		this.isServer = isServer;
		assert(encryptedContent.length==plainContent.length);
		
		this.groupId = groupId;
		
	}

	@Override
	public void startup() {
		
		//must allocate buffers for the out of order content 
		int c = plainContent.length;
		secureBuffers = new ByteBuffer[c];
		while (--c>=0) {
			secureBuffers[c] = ByteBuffer.allocate(plainContent[c].maxAvgVarLen*2);
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
					didWork |= SSLUtil.engineWrap(ccm, sourcePipe, targetPipe, secureBuffers[i], isServer, groupId);	
					
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
				if (PipeWriter.hasRoomForFragmentOfSize(targetPipe, SIZE_HANDSHAKE_AND_DISCONNECT)
					&& PipeReader.peekMsg(sourcePipe, NetPayloadSchema.MSG_DISCONNECT_203)) {
					
					//logger.info("WRAP FOUND DISCONNECT MESSAGE A server:"+isServer);
					
					PipeReader.tryReadFragment(sourcePipe);
					long connectionId = PipeReader.readLong(sourcePipe, NetPayloadSchema.MSG_DISCONNECT_203_FIELD_CONNECTIONID_201);
					
					SSLConnection connection = ccm.get(connectionId, groupId);
					if (null!=connection) {
						assert(connection.isDisconnecting()) : "should only receive disconnect messages on connections which are disconnecting.";
						SSLUtil.handShakeWrapIfNeeded(connection, targetPipe, secureBuffers[i], isServer);					
					}				
					
					PipeWriter.tryWriteFragment(targetPipe, NetPayloadSchema.MSG_DISCONNECT_203);
					PipeWriter.writeLong(targetPipe, NetPayloadSchema.MSG_DISCONNECT_203_FIELD_CONNECTIONID_201, connectionId);
					PipeWriter.publishWrites(targetPipe);
					
					PipeReader.releaseReadLock(sourcePipe);
				} 
				
				///////////////////////////
				//shutdown this stage logic
				///////////////////////////
				if (PipeReader.peekMsg(sourcePipe, -1)) {
					PipeReader.tryReadFragment(sourcePipe);
					PipeReader.releaseReadLock(sourcePipe);
					if (--shutdownCount<=0) {
						System.err.println("shutdown SSLEngineWrap");
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
    		PipeWriter.publishEOF(encryptedContent[j]);
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
