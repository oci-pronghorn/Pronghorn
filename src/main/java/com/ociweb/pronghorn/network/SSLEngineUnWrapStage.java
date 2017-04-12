package com.ociweb.pronghorn.network;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class SSLEngineUnWrapStage extends PronghornStage {

	private final SSLConnectionHolder ccm;
	private final Pipe<NetPayloadSchema>[] encryptedContent; 
	private final Pipe<NetPayloadSchema>[] outgoingPipeLines;
	private final Pipe<ReleaseSchema> handshakeRelease; //to allow for the release of the pipe when we do not need it.
	private final Pipe<NetPayloadSchema>  handshakePipe;
	private ByteBuffer[]                          rollings;
	private ByteBuffer[]                          workspace;
	private Logger logger = LoggerFactory.getLogger(SSLEngineUnWrapStage.class);
	
	private long totalNS;
	private int calls;
	private ByteBuffer secureBuffer;
	private final boolean isServer;
	private int groupId;
	private int shutdownCount;
	
	private int idx;
	
	public SSLEngineUnWrapStage(GraphManager graphManager, SSLConnectionHolder ccm, 
			                       Pipe<NetPayloadSchema>[] encryptedContent, 
			                       Pipe<NetPayloadSchema>[] outgoingPipeLines,
			                       Pipe<ReleaseSchema> relesePipe,
			                       Pipe<NetPayloadSchema> handshakePipe, boolean isServer, int groupId) {
		super(graphManager, encryptedContent, join(outgoingPipeLines, handshakePipe, relesePipe));
		this.ccm = ccm;
		this.encryptedContent = encryptedContent;
		this.outgoingPipeLines = outgoingPipeLines;
		this.handshakeRelease = relesePipe;
		
		assert(outgoingPipeLines.length>0);
		assert(encryptedContent.length>0);
		assert(encryptedContent.length == outgoingPipeLines.length);
		
		this.handshakePipe = handshakePipe;
		
		this.supportsBatchedPublish = false;
		
		this.isServer = isServer;
		this.groupId = groupId;
		this.shutdownCount = encryptedContent.length;

	}

	
	@Override
	public void startup() {
		
		idx = encryptedContent.length;
		
		//must allocate buffers for the out of order content 
		int c = encryptedContent.length;
		rollings = new ByteBuffer[c];
		while (--c>=0) {
		//	int size = SSLUtil.MAX_ENCRYPTED_PACKET_LENGTH;//
			int size = encryptedContent[c].maxAvgVarLen*2;
			
			rollings[c] = ByteBuffer.allocateDirect(size);
			
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

			int m = 100;//maximum iterations before taking a short break.
			
			while ((--idx>=0) && (--m>=0)) {
				
				Pipe<NetPayloadSchema> source = encryptedContent[idx];
				Pipe<NetPayloadSchema> target = outgoingPipeLines[idx];

				
//				//TODO: is there a debug method we can write for this in general?
//				//no content to wrap on server
//				if (Pipe.contentRemaining(source)>0) {
//					logger.info("input data to be unwrapped "+isServer+" "+idx+" source "+source.contentRemaining(source));
//				}
//				if (Pipe.contentRemaining(target)>0) { //TODO: why is this negative?
//					logger.info("output data unwrapped "+isServer+"  "+idx+" target "+target.contentRemaining(target));
//				}
							
				
				int temp = SSLUtil.engineUnWrap(ccm, source, target, rollings[idx], workspace, handshakePipe, handshakeRelease, secureBuffer, groupId, isServer);			
				if (temp<0) {
					if (--shutdownCount == 0) {
						requestShutdown();
						return;
					}
					break;
				} else {				
					didWork |= temp;
				}
		
			}			
			
			//loop back arround
			if (idx<=0) {
				idx= encryptedContent.length;
			}
			
		} while (didWork!=0);
		
		
		
		
		totalNS += System.nanoTime()-start;
		
				
	}
	

	@Override
	public void shutdown() {
		
		if (null==rollings) {
			//never started up so just exit
			return;
		}
		
		int i = rollings.length;
		while (--i>=0) {
			
			if (rollings[i].position()>0) {
				logger.warn("unwrap found unconsumed data in buffer {} of value {} ",i, rollings[i]);
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
