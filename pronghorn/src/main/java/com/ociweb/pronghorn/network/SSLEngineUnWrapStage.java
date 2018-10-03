package com.ociweb.pronghorn.network;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
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

	private ByteBuffer[]                          workspace;
	private Logger logger = LoggerFactory.getLogger(SSLEngineUnWrapStage.class);
	

	private final boolean isServer;

	private int shutdownCount;
	private final int rollingSize;

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

		if (encryptedContent.length != outgoingPipeLines.length) {
			throw new UnsupportedOperationException("Must have the same count of input and output pipes");
		};
		
		if (encryptedContent.length<=0) {
			throw new UnsupportedOperationException("Must have at least 1 input pipe");		
		}
		if (outgoingPipeLines.length<=0) {
			throw new UnsupportedOperationException("Must have at least 1 output pipe");
		}
		
		this.rollingSize = PronghornStage.maxVarLength(encryptedContent)*2;
				
		this.handshakePipe = handshakePipe;
		
		this.supportsBatchedPublish = false;
		
		this.isServer = isServer;

		this.shutdownCount = encryptedContent.length;
		
		GraphManager.addNota(graphManager, GraphManager.HEAVY_COMPUTE, GraphManager.HEAVY_COMPUTE, this);
		
		GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "bisque1", this);
		
	}

	private ByteBuffer[] rollerArray;

	
	@Override
	public void startup() {
		
		////////////////////////////////////////////////////////
		//NOTE: a single output pipe may contain many client responses
		//      as a result we need multiple rollers.
		////////////////////////////////////////////////////////
		
		//maxConnections array with a hash into these location. with session and hash..
		//keep connection array for match..
		
		int len = encryptedContent.length;// : ccm.maxConnections();
		
		this.rollerArray = new ByteBuffer[len];
		
		int x = len;
		while (--x>=0) {
			this.rollerArray[x] = ByteBuffer.allocateDirect(rollingSize);
		}
		
		//we use this workspace to ensure that temp data used by TLS is not exposed to the pipe.
		this.workspace = new ByteBuffer[]{ByteBuffer.allocateDirect(SSLUtil.MinTLSBlock),ByteBuffer.allocateDirect(0)};

	}
	
	
	
	@Override
	public void run() {	
		
		long start = System.nanoTime();
		
		int didWork;
		
		do {
			didWork=0;

			int idx = encryptedContent.length;
			while ((--idx>=0) ) {
				Pipe<NetPayloadSchema> source = encryptedContent[idx];
				if ((!Pipe.isEmpty(source)) && Pipe.hasContentToRead(source)) {
				
					Pipe<NetPayloadSchema> target = outgoingPipeLines[idx];
	
					
					ByteBuffer roller = rollerArray[idx];
															
					
					int temp = SSLUtil.engineUnWrap(ccm, source, target, roller, 
							                        workspace, handshakePipe, handshakeRelease, 
							                        isServer);			
					
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
					
		} while (didWork!=0);
			
	}
	

	@Override
	public void shutdown() {
		
		if (null==rollerArray) {
			//never started up so just exit
			return;
		}
		
		int i = rollerArray.length;
		while (--i>=0) {			
			if (null!=rollerArray[i] && (rollerArray[i].position()>0)) {
				logger.warn("unwrap found unconsumed data in buffer {} of value {} ",i, rollerArray[i]);
			}
		}

	}
	

}
