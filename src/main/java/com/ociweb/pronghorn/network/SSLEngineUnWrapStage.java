package com.ociweb.pronghorn.network;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.ClientNetRequestSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class SSLEngineUnWrapStage extends PronghornStage {

	private final ClientConnectionManager ccm;
	private final Pipe<NetPayloadSchema>[] encryptedContent; 
	private final Pipe<NetPayloadSchema>[] outgoingPipeLines;
	private final Pipe<ClientNetRequestSchema>[] handshakePipe;
	private ByteBuffer[]                          buffers;
	private ByteBuffer[]                          workspace;
	private Logger logger = LoggerFactory.getLogger(SSLEngineUnWrapStage.class);
	
	private long totalNS;
	private int calls;
	private ByteBuffer secureBuffer;
	private final boolean isServer;

	public SSLEngineUnWrapStage(GraphManager graphManager, ClientConnectionManager ccm, 
			                       Pipe<NetPayloadSchema>[] encryptedContent, 
			                       Pipe<NetPayloadSchema>[] outgoingPipeLines) {
		this(graphManager,ccm,encryptedContent,outgoingPipeLines,new Pipe[outgoingPipeLines.length], false);
	}
	
	public SSLEngineUnWrapStage(GraphManager graphManager, ClientConnectionManager ccm, 
            Pipe<NetPayloadSchema>[] encryptedContent, 
            Pipe<NetPayloadSchema>[] outgoingPipeLines,
            Pipe<ClientNetRequestSchema>[] handshakePipe) {
		this(graphManager, ccm, encryptedContent, outgoingPipeLines, handshakePipe, true);
	}
	
	protected SSLEngineUnWrapStage(GraphManager graphManager, ClientConnectionManager ccm, 
			                       Pipe<NetPayloadSchema>[] encryptedContent, 
			                       Pipe<NetPayloadSchema>[] outgoingPipeLines,
			                       Pipe<ClientNetRequestSchema>[] handshakePipe, boolean isServer) {
		super(graphManager, encryptedContent, outgoingPipeLines);
		this.ccm = ccm;
		this.encryptedContent = encryptedContent;
		this.outgoingPipeLines = outgoingPipeLines;
		this.handshakePipe = handshakePipe;
		this.isServer = isServer;
		assert(encryptedContent.length == outgoingPipeLines.length);
	}

	@Override
	public void startup() {
		
		//must allocate buffers for the out of order content 
		int c = encryptedContent.length;
		buffers = new ByteBuffer[c];
		while (--c>=0) {
			buffers[c] = ByteBuffer.allocate(encryptedContent[c].maxAvgVarLen*2);
		}				
		        
		//we use this workspace to ensure that temp data used by TLS is not exposed to the pipe.
		this.workspace = new ByteBuffer[]{ByteBuffer.allocate(1<<15),ByteBuffer.allocate(0)};
		
		this.secureBuffer = null==handshakePipe? null : ByteBuffer.allocate(outgoingPipeLines[0].maxAvgVarLen*2);
		
	}
	
	
	@Override
	public void run() {
		long start = System.nanoTime();
		calls++;	
						
		int i = encryptedContent.length;
		while (--i >= 0) {
			ClientConnection.engineUnWrap(ccm, encryptedContent[i], outgoingPipeLines[i], buffers[i], workspace, handshakePipe[i], secureBuffer);
		}
		totalNS += System.nanoTime()-start;
		
	}

	@Override
	public void shutdown() {
		int i = buffers.length;
		while (--i>=0) {
			
			if (buffers[i].position()>0) {
				logger.warn("unwrap found unconsumed data in buffer");
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
