package com.ociweb.pronghorn.network;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.NetParseAckSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class SSLEngineUnWrapStage extends PronghornStage {

	private final SSLConnectionHolder ccm;
	private final Pipe<NetPayloadSchema>[] encryptedContent; 
	private final Pipe<NetPayloadSchema>[] outgoingPipeLines;
	private final Pipe<NetParseAckSchema> handshakeAck; //to allow for the release of the pipe when we do not need it.
	private final Pipe<NetPayloadSchema>  handshakePipe;
	private ByteBuffer[]                          buffers;
	private ByteBuffer[]                          workspace;
	private Logger logger = LoggerFactory.getLogger(SSLEngineUnWrapStage.class);
	
	private long totalNS;
	private int calls;
	private ByteBuffer secureBuffer;
	private final boolean isServer;
	private int groupId;
	private int shutdownCount;
	
	private int idx = 1;
	
	public SSLEngineUnWrapStage(GraphManager graphManager, SSLConnectionHolder ccm, 
			                       Pipe<NetPayloadSchema>[] encryptedContent, 
			                       Pipe<NetPayloadSchema>[] outgoingPipeLines,
			                       Pipe<NetParseAckSchema> ack,
			                       Pipe<NetPayloadSchema> handshakePipe, boolean isServer, int groupId) {
		super(graphManager, encryptedContent, join(outgoingPipeLines, handshakePipe, ack));
		this.ccm = ccm;
		this.encryptedContent = encryptedContent;
		this.outgoingPipeLines = outgoingPipeLines;
		this.handshakeAck = ack;
		
		assert(outgoingPipeLines.length>0);
		assert(encryptedContent.length>0);
		assert(encryptedContent.length == outgoingPipeLines.length);
		
		this.handshakePipe = handshakePipe;
		
		this.isServer = isServer;
		this.groupId = groupId;
		this.shutdownCount = encryptedContent.length;
	}

	public SSLEngineUnWrapStage(GraphManager graphManager, SSLConnectionHolder ccm, 
            Pipe<NetPayloadSchema>[] encryptedContent, 
            Pipe<NetPayloadSchema>[] outgoingPipeLines,
            Pipe<NetParseAckSchema> ack,
            boolean isServer, int groupId) {
		super(graphManager, encryptedContent, join(outgoingPipeLines,ack));
		this.ccm = ccm;
		this.encryptedContent = encryptedContent;
		this.outgoingPipeLines = outgoingPipeLines;
		this.handshakeAck = ack;
		this.handshakePipe = null;
		this.isServer = isServer;
		this.groupId = groupId;
		assert(encryptedContent.length == outgoingPipeLines.length);
		
		int minOut = minVarLength(outgoingPipeLines);
		int maxIn  = maxVarLength(encryptedContent);
		if (maxIn*2>minOut) {
			//unwrap is an all or nothing operation, if we have left over data in the roller and we combine it
			//with the most recent read then the output target may require 2x the payload space, so the pipes must be built to hold this.
			throw new UnsupportedOperationException("output payloads must be 2x larger than input payloads, in:"+maxIn+" out:"+minOut);
		}
		
//		//TODO: could see a nice performance improvment if we can bump this up.
//		if (maxIn > SSLUtil.MAX_ENCRYPTED_PACKET_LENGTH) {
//			throw new UnsupportedOperationException("max buffer to decrypt must be less than "+SSLUtil.MAX_ENCRYPTED_PACKET_LENGTH+" but "+maxIn+" was used. (Limitiation from OpenSSL)");
//		}
		
		
	}
	
	@Override
	public void startup() {
		
		//must allocate buffers for the out of order content 
		int c = encryptedContent.length;
		buffers = new ByteBuffer[c];
		while (--c>=0) {
		//	int size = SSLUtil.MAX_ENCRYPTED_PACKET_LENGTH;//
			int size = encryptedContent[c].maxAvgVarLen*2;
			
			buffers[c] = ByteBuffer.allocateDirect(size);
			
//			if (size > SSLUtil.MAX_ENCRYPTED_PACKET_LENGTH) {
//				throw new UnsupportedOperationException("max buffer to decrypt must be less than "+SSLUtil.MAX_ENCRYPTED_PACKET_LENGTH+" but "+size+" was used. (Limitiation from OpenSSL)");
//			}
			
		}				
		
		//we use this workspace to ensure that temp data used by TLS is not exposed to the pipe.
		this.workspace = new ByteBuffer[]{ByteBuffer.allocateDirect(1<<15),ByteBuffer.allocateDirect(0)};
		
		this.secureBuffer = null==handshakePipe? null : ByteBuffer.allocate(outgoingPipeLines[0].maxAvgVarLen*2);
		
	}
	
	
	
	@Override
	public void run() {
		
		
		long start = System.nanoTime();
		calls++;	
		
		int didWork;
		
		do {
			didWork=0;
		
			//int i = encryptedContent.length;
			
			//int tmp = idx;
			idx = encryptedContent.length;
			while (//--idx != tmp) { //
					--idx >= 0) {
				
				Pipe<NetPayloadSchema> source = encryptedContent[idx];
				Pipe<NetPayloadSchema> target = outgoingPipeLines[idx];
				
				
//				//TODO: is there a debug method we can write for this in general?
//				//no content to wrap on server
//				if (Pipe.contentRemaining(source)>0) {
//					System.err.println("input data to be unwrapped "+isServer+" "+idx+" source "+source.contentRemaining(source));
//				}
//				if (Pipe.contentRemaining(target)>0) { //TODO: why is this negative?
//					System.err.println("output data unwrapped "+isServer+"  "+idx+" target "+target.contentRemaining(target));
//				}
				
				ByteBuffer rolling = buffers[idx];			
				workspace[0].clear();
				workspace[1].clear();
				
				int temp = SSLUtil.engineUnWrap(ccm, source, target, rolling, workspace, isServer ? handshakePipe : null, handshakeAck, secureBuffer, groupId);			
				if (temp<0) {
					if (--shutdownCount == 0) {
						requestShutdown();
						return;
					}
					
				}
				didWork |= temp;
//				if (0==idx) {
//					idx=encryptedContent.length;
//				}
		
			}			
		} while (didWork!=0);
		
		
		
		
		totalNS += System.nanoTime()-start;
		
				
	}
	

	@Override
	public void shutdown() {
		
//		if (isServer) {
//			new Exception("XXXXXXXXXXXXXXXXXXXXXXXXXX shut down server unwrap check the pipes ").printStackTrace();
//			int i = buffers.length;
//			while (--i>=0) {
//				System.err.println("XXXXXXXXXXXXXXXXXXxxxxxx " +buffers[i]);
//				System.err.println("XXXXXXXXXXXXXXXXXXINxxxx " +encryptedContent[i]+" "+Pipe.contentRemaining(encryptedContent[i])); //has ODD work tail??
//				System.err.println("XXXXXXXXXXXXXXXXXXOUTxxx " +outgoingPipeLines[i]+" "+Pipe.contentRemaining(outgoingPipeLines[i]));
//				
//				
//			}
//		}
//		
		
		int i = buffers.length;
		while (--i>=0) {
			
			if (buffers[i].position()>0) {
				logger.warn("unwrap found unconsumed data in buffer {} of value {} ",i, buffers[i]);
			}
			
		}
		
		boolean debug = false;
		if (debug) {
			long totalBytesOfContent = 0;
			i = outgoingPipeLines.length;
			while (--i>=0) {
				
				totalBytesOfContent += Pipe.getBlobRingTailPosition(outgoingPipeLines[i]);
			}
			
			float mbps = (float) ( (8_000d*totalBytesOfContent)/ (double)totalNS);
			
			logger.info("unwrapped total bytes "+totalBytesOfContent+"    "+mbps+"mbps");
			logger.info("unwrapped total time "+totalNS+"ns total callls "+calls);
		}
	}
	

}
