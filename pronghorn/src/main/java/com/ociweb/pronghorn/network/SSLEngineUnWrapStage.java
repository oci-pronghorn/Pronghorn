package com.ociweb.pronghorn.network;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

/**
 * Unwraps encrypted content for HTTPS/SSL.
 *
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class SSLEngineUnWrapStage extends PronghornStage {

	public static boolean showRolling;
	
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

	private int shutdownCount;
	
	private int idx;

	/**
	 *
	 * @param graphManager
	 * @param ccm
	 * @param encryptedContent _in_ Encrypted content to be unencrypted.
	 * @param outgoingPipeLines _out_ Unencrypted content.
	 * @param relesePipe _out_ Acknowledgment for release.
	 * @param handshakePipe _out_ Responds with a handshake.
	 * @param isServer
	 */
	public SSLEngineUnWrapStage(GraphManager graphManager, SSLConnectionHolder ccm, 
			                       Pipe<NetPayloadSchema>[] encryptedContent, 
			                       Pipe<NetPayloadSchema>[] outgoingPipeLines,
			                       Pipe<ReleaseSchema> relesePipe,
			                       Pipe<NetPayloadSchema> handshakePipe, boolean isServer) {
		super(graphManager, encryptedContent, join(outgoingPipeLines, handshakePipe, relesePipe));
		this.ccm = ccm;
		this.encryptedContent = encryptedContent;
		this.outgoingPipeLines = outgoingPipeLines;
		this.handshakeRelease = relesePipe;
				
		assert(outgoingPipeLines.length>0);
		assert(encryptedContent.length>0);
		assert(encryptedContent.length == outgoingPipeLines.length);
		
		if (encryptedContent.length<=0) {
			throw new UnsupportedOperationException("Must have at least 1 input pipe");		
		}
		if (outgoingPipeLines.length<=0) {
			throw new UnsupportedOperationException("Must have at least 1 output pipe");
		}
		
		
		this.handshakePipe = handshakePipe;
		
		this.supportsBatchedPublish = false;
		
		this.isServer = isServer;

		this.shutdownCount = encryptedContent.length;
		
		GraphManager.addNota(graphManager, GraphManager.HEAVY_COMPUTE, GraphManager.HEAVY_COMPUTE, this);
		
		GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "bisque1", this);
		
	}

	
	@Override
	public void startup() {
		
		idx = encryptedContent.length;
		
		//must allocate buffers for the out of order content 
		int c = encryptedContent.length;
		rollings = new ByteBuffer[c];
		while (--c>=0) {
		//	int size = SSLUtil.MAX_ENCRYPTED_PACKET_LENGTH;//
			int size = encryptedContent[c].maxVarLen*2;
			
			rollings[c] = ByteBuffer.allocateDirect(size);
			
//			if (size > SSLUtil.MAX_ENCRYPTED_PACKET_LENGTH) {
//				throw new UnsupportedOperationException("max buffer to decrypt must be less than "+SSLUtil.MAX_ENCRYPTED_PACKET_LENGTH+" but "+size+" was used. (Limitiation from OpenSSL)");
//			}
			
		}				
		
		//we use this workspace to ensure that temp data used by TLS is not exposed to the pipe.
		this.workspace = new ByteBuffer[]{ByteBuffer.allocateDirect(1<<15),ByteBuffer.allocateDirect(0)};
		
		this.secureBuffer = null==handshakePipe? null : ByteBuffer.allocate(outgoingPipeLines[0].maxVarLen*2);
		
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

				if ((!Pipe.isEmpty(source)) && Pipe.hasContentToRead(source)) {
				
					Pipe<NetPayloadSchema> target = outgoingPipeLines[idx];
	
					int temp = SSLUtil.engineUnWrap(ccm, source, target, rollings[idx], workspace, handshakePipe, handshakeRelease, 
							                        secureBuffer, isServer);			
					
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
				
				totalBytesOfContent += Pipe.getBlobTailPosition(outgoingPipeLines[i]);
			}
			
			float mbps = (float) ( (8_000d*totalBytesOfContent)/ (double)totalNS);
			
			logger.info("unwrapped total bytes "+totalBytesOfContent+"    "+mbps+"mbps");
			logger.info("unwrapped total time "+totalNS+"ns total callls "+calls);
		}
	}
	

}
