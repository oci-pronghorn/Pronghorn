package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLEngineResult.Status;
import javax.xml.crypto.NodeSetData;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.ClientNetRequestSchema;
import com.ociweb.pronghorn.network.schema.ClientNetResponseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class ClientConnection {

	private static final Logger log = LoggerFactory.getLogger(ClientConnection.class);

	static boolean isShuttingDown =  false;
	
	private final SSLEngine engine;
	private final SocketChannel socketChannel;
	private SelectionKey key; //only registered after handshake is complete.
	private boolean isValid = true;
	private boolean isDisconnecting = false;
	
	private final byte[] connectionGUID;
	
	private final int pipeIdx;
	private long id=-1;
	
	private long requestsSent;
	private long responsesReceived;
	
	private final int userId;
	private final String host;
	private final int port;
	
	private static InetAddress testAddr;
	
	private long closeTimeLimit = Long.MAX_VALUE;
	private long TIME_TILL_CLOSE = 10_000;

	public int localRunningBytesProduced;
	
	
	static {
		
		try {
			testAddr = InetAddress.getByName("www.google.com");
		} catch (UnknownHostException e) {
			log.error("no network connection.");
			System.exit(-1);
		}
	
	}
	
	private boolean hasNetworkConnectivity() {
		try {
			return testAddr.isReachable(10_000);
		} catch (IOException e) {
			return false;
		}
	}
	
	public ClientConnection(String host, int port, int userId, int pipeIdx) throws IOException {
		assert(port<=65535);
		
		// RFC 1035 the length of a FQDN is limited to 255 characters
		this.connectionGUID = new byte[(2*host.length())+6];
		buildGUID(connectionGUID, host, port, userId);
		this.pipeIdx = pipeIdx;
		this.userId = userId;
		this.host = host;
		this.port = port;
				
		this.engine = SSLEngineFactory.createSSLEngine(host, port);
		this.engine.setUseClientMode(true);
				
		this.socketChannel = SocketChannel.open();
		this.socketChannel.configureBlocking(false);  
		this.socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
		this.socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true); 
		this.socketChannel.setOption(StandardSocketOptions.SO_RCVBUF, 1<<18); //.25 MB
						
		try {
			InetSocketAddress remote = new InetSocketAddress(host, port);
			this.socketChannel.connect(remote);
		} catch (UnresolvedAddressException uae) {
			
			if (hasNetworkConnectivity()) {
				log.error("unable to find {}:{}",host,port);
				throw uae;
			} else {
				log.error("No network connection.");
				System.exit(-1);						
			}
		}
		this.socketChannel.finishConnect(); //call again later to confirm its done.	
		
	}
	

	
	public String getHost() {
		return host;
	}
	public int getPort() {
		return port;
	}
	
	public int getUserId() {
		return userId;
	}
	
	public void incRequestsSent() {
		closeTimeLimit = Long.MAX_VALUE;
		requestsSent++;		
	}
	
	public void waitForMatch() {
		while (responsesReceived<requestsSent) {
			Thread.yield();
		}
	}
	
	public boolean incResponsesReceived() {
		assert(1+responsesReceived<=requestsSent) : "received more responses than requests were sent";
		boolean result = (++responsesReceived)==requestsSent;
		if (result) {
			
			if (isShuttingDown) {
				close();
			} else {
				closeTimeLimit = System.currentTimeMillis()+TIME_TILL_CLOSE;
			}
			
		}
		return result;
	}
	
	public SelectionKey getSelectionKey() {
		return key;
	}
	
	public long getId() {
		return this.id;
	}

	public void setId(long id) {
		assert(this.id == -1);
		this.id = id;	
	}

	public int requestPipeLineIdx() {
		return pipeIdx;
	}

	public static int buildGUID(byte[] target, CharSequence host, int port, int userId) {
		//TODO: if we find a better hash for host port user we can avoid this trie lookup. TODO: performance improvement.
		//new Exception("build guid").printStackTrace();
		
		int pos = 0;
    	for(int i = 0; i<host.length(); i++) {
    		short c = (short)host.charAt(i);
    		target[pos++] = (byte)(c>>8);
    		target[pos++] = (byte)(c);
    	}
    	target[pos++] = (byte)(port>>8);
    	target[pos++] = (byte)(port);
    	
    	target[pos++] = (byte)(userId>>24);
    	target[pos++] = (byte)(userId>>16);
    	target[pos++] = (byte)(userId>>8);
    	target[pos++] = (byte)(userId);
    	return pos;
	}
	
	public byte[] GUID() {
		return connectionGUID;
	}
	
	/**
	 * After construction this must be called until it returns true before using this connection. 
	 * @throws IOException
	 */
	public boolean isFinishConnect() throws IOException {
		return socketChannel.finishConnect();
	}
	
		
	public void handshake(Selector selector) throws IOException {

		assert(socketChannel.finishConnect());
		
		engine.beginHandshake();	
		isValid = true;
		this.key = socketChannel.register(selector, SelectionKey.OP_READ, this); 

    	
	}
	
	public boolean isDisconnecting() {
		return isDisconnecting;
	}
	

	public boolean isValid() {

		if (!socketChannel.isConnected()) {
			return false;
		}
		
		if (responsesReceived==requestsSent && System.currentTimeMillis() > closeTimeLimit) {
			log.info("stale connection closed after non use {}",this);
			
			//TODO: this work can not be done here and needs to be owned by the pipe. HTTPClientReqeust is the ideal place.
			
			beginDisconnect(); 
			
			if (true) {
				throw new UnsupportedOperationException("Can not close old connection without finishing handshake.");
			}
			
			close();
			return false;
		}
		return isValid;
	}
	
    //should only be closed by the socket writer logic or TLS handshake may be disrupted causing client to be untrusted.
	public boolean close() {
		if (isValid) {
			isValid = false;
			try {
				socketChannel.close();
				} catch (IOException e) {					
			}
			
			return true;
		} 
		
		return false;
	}

	public void beginDisconnect() {
		try {
			 isDisconnecting = true;
			 engine.closeOutbound();
		} catch (Throwable e) {
			log.warn("Error closing connection ",e);
			close();
		}
	}
		
	
	public boolean writeToSocketChannel(Pipe<ClientNetRequestSchema> source) {
		ByteBuffer[] writeHolder = PipeReader.wrappedUnstructuredLayoutBuffer(source, ClientNetRequestSchema.MSG_SIMPLEREQUEST_100_FIELD_PAYLOAD_103);
		try {
			do {
				socketChannel.write(writeHolder);
			} while (writeHolder[1].hasRemaining()); //TODO: this  is a bad spin loop to be refactored up and out.
		} catch (IOException e) {
			log.debug("unable to write to socket {}",this,e);
			close();
			return false;
		}
		return true;
		
	}
	
	public long readfromSocketChannel(ByteBuffer[] targetByteBuffers) {
		
		try {
			return socketChannel.read(targetByteBuffers);			
		} catch (IOException ex) {			
			log.warn("unable read from socket {}",this,ex);
			close();
		}
		return -1;
	}
	
	
	/**
	 * Encrypt as much as possible based on the data available from the two pipes
	 * @param source
	 * @param target
	 * @param buffer 
	 */
	
	public static void engineWrap(ClientConnectionManager ccm, Pipe<ClientNetRequestSchema> source, Pipe<ClientNetRequestSchema> target, ByteBuffer buffer, boolean isServer) {
		
		while (PipeWriter.hasRoomForWrite(target) && PipeReader.peekMsg(source, ClientNetRequestSchema.MSG_SIMPLEREQUEST_100) ) {
			
			final ClientConnection cc = ccm.get(PipeReader.peekLong(source, ClientNetRequestSchema.MSG_SIMPLEREQUEST_100_FIELD_CONNECTIONID_101));
			
			if (null==cc || !cc.isValid) {
				//do not process this message because the connection has dropped
				PipeReader.tryReadFragment(source);
				PipeReader.releaseReadLock(source);
				continue;
			}
			
			if ((!isServer) && ClientConnection.handShakeWrapIfNeeded(cc, target, buffer)) {
				return;
			}
			
			PipeReader.tryReadFragment(source);
			
			ByteBuffer[] bbHolder = PipeReader.wrappedUnstructuredLayoutBuffer(source, ClientNetRequestSchema.MSG_SIMPLEREQUEST_100_FIELD_PAYLOAD_103);
											

			try {
				final ByteBuffer[] targetBuffers = PipeWriter.wrappedUnstructuredLayoutBufferOpen(target, ClientNetRequestSchema.MSG_ENCRYPTEDREQUEST_110_FIELD_ENCRYPTED_104);
				SSLEngineResult result = cc.engine.wrap(bbHolder, targetBuffers[0]);			
				Status status = wrapResultStatusState(target, buffer, cc, bbHolder, targetBuffers, result);
											
				if (status == Status.OK) {
					
					PipeReader.copyLong(source, target, ClientNetRequestSchema.MSG_SIMPLEREQUEST_100_FIELD_CONNECTIONID_101, 
							                            ClientNetRequestSchema.MSG_ENCRYPTEDREQUEST_110_FIELD_CONNECTIONID_101);			
					PipeWriter.publishWrites(target);
					PipeReader.releaseReadLock(source);	
					
				} else if (status == Status.CLOSED) {
					
					PipeReader.releaseReadLock(source);		
					
				}
			
			} catch (SSLException sslex) {
				manageException(sslex, cc);			
				
			}
			
			
		}
	
	}

	private static Status wrapResultStatusState(Pipe<ClientNetRequestSchema> target, ByteBuffer buffer,
			final ClientConnection cc, ByteBuffer[] bbHolder, final ByteBuffer[] targetBuffers,
			SSLEngineResult result) {
		Status status = result.getStatus();
		if (status==Status.OK) {
			PipeWriter.tryWriteFragment(target, ClientNetRequestSchema.MSG_ENCRYPTEDREQUEST_110);
			PipeWriter.wrappedUnstructuredLayoutBufferClose(target, ClientNetRequestSchema.MSG_ENCRYPTEDREQUEST_110_FIELD_ENCRYPTED_104, result.bytesProduced());

		} else if (status==Status.CLOSED){
							
			PipeWriter.wrappedUnstructuredLayoutBufferCancel(target);							

			try {
				 cc.engine.closeOutbound();
				 
				 handShakeWrapIfNeeded(cc, target, buffer);
				 
				 cc.socketChannel.close();
			} catch (IOException e) {
				cc.isValid = false;
				log.warn("Error closing connection ",e);
			}				
		} else if (status==Status.BUFFER_OVERFLOW) {
			///////////
			//This is only needed becauae engine.wrap does not take multiple target ByteBuffers as it should have.
			///////////
			try {
				buffer.clear();
				SSLEngineResult result2 = cc.engine.wrap(bbHolder, buffer);
				
				status = result2.getStatus();
				if (status == Status.OK) {
					
					//write buffer to openA and openB
					buffer.flip();
					
					int finalLimit = buffer.limit();
					int room = targetBuffers[0].remaining();
					if (room<finalLimit) {
						buffer.limit(room);
					}										
					targetBuffers[0].put(buffer);
					buffer.limit(finalLimit);
					if (buffer.hasRemaining()) {
						targetBuffers[1].put(buffer);
					}
					if (buffer.hasRemaining()) {
						throw new UnsupportedOperationException("ERR1");
					}
					PipeWriter.tryWriteFragment(target, ClientNetRequestSchema.MSG_ENCRYPTEDREQUEST_110);
					PipeWriter.wrappedUnstructuredLayoutBufferClose(target, ClientNetRequestSchema.MSG_ENCRYPTEDREQUEST_110_FIELD_ENCRYPTED_104, result2.bytesProduced());
					
				} else {
					throw new UnsupportedOperationException("ERR2");
					//ERROR?
					
				}
				
			} catch (SSLException sslex) {
				manageException(sslex, cc);			
				status = null;
			}
			
			
		} else {
			throw new RuntimeException("case should not happen, we have too little data to be wrapped and sent");
		}
		return status;
	}


	public static void engineUnWrap(ClientConnectionManager ccm, Pipe<ClientNetResponseSchema> source, Pipe<ClientNetResponseSchema> target, 
			                        ByteBuffer rolling, ByteBuffer[] workspace, Pipe<ClientNetRequestSchema> handshakePipe, ByteBuffer secureBuffer) {
		
		while (PipeReader.hasContentToRead(source) ) {
			
			if (!PipeWriter.hasRoomForWrite(target)) {								
				return;//try again later when there is room in the output
			}			
				
			
			ClientConnection cc = ccm.get(PipeReader.peekLong(source, ClientNetResponseSchema.MSG_RESPONSE_200_FIELD_CONNECTIONID_201));
			
			if (null==cc || !cc.isValid) {
				//do not process this message because the connection has dropped
				PipeReader.tryReadFragment(source);
				PipeReader.releaseReadLock(source);
				continue;
			}
			
			if (ClientConnection.handShakeUnWrapIfNeeded(cc, source, rolling, workspace, handshakePipe, secureBuffer)) {
				return;
			}
			
			if (!PipeReader.tryReadFragment(source)) {
				return;//not an error because the only fragment may have been consumed by the handshake
			}
					
			if (PipeReader.getMsgIdx(source)<0) {
				//TODO: shutdown?
			}
			
			final ByteBuffer[] writeHolderUnWrap = PipeWriter.wrappedUnstructuredLayoutBufferOpen(target, ClientNetResponseSchema.MSG_SIMPLERESPONSE_210_FIELD_PAYLOAD_204);
			
			
			SSLEngineResult result = unwrappedResultStatusState(source, rolling, cc, writeHolderUnWrap);
			Status status = null==result?null:result.getStatus();			
			
			if(cc.localRunningBytesProduced>0) {
				if (!PipeWriter.tryWriteFragment(target, ClientNetResponseSchema.MSG_SIMPLERESPONSE_210)) {
					throw new RuntimeException("already checked for space should not happen.");
				}
				PipeWriter.wrappedUnstructuredLayoutBufferClose(target, ClientNetResponseSchema.MSG_SIMPLERESPONSE_210_FIELD_PAYLOAD_204, cc.localRunningBytesProduced);
				cc.localRunningBytesProduced = -1;
				
				//assert(longs match the one for cc)
				PipeWriter.writeLong(target, ClientNetResponseSchema.MSG_SIMPLERESPONSE_210_FIELD_CONNECTIONID_201, cc.id);
				//    PipeReader.copyLong(source, target, ClientNetResponseSchema.MSG_RESPONSE_200_FIELD_CONNECTIONID_201, ClientNetResponseSchema.MSG_SIMPLERESPONSE_210_FIELD_CONNECTIONID_201);
				
				PipeWriter.publishWrites(target);
				
			}			
			
			if (status==Status.OK || status==null) {						

			} else if (status==Status.CLOSED){
				if (cc.localRunningBytesProduced==0) {
					PipeWriter.wrappedUnstructuredLayoutBufferCancel(target);							
				} else {
					System.err.println("AAA Xxxxxxxxxxxxxxxxxx found some data to send ERROR: must publish this");					
					
				}
				
				try {
					 cc.engine.closeOutbound();
					 handShakeUnWrapIfNeeded(cc, source, rolling, workspace, handshakePipe, secureBuffer);
				     cc.socketChannel.close();
				} catch (IOException e) {
					cc.isValid = false;
					log.warn("Error closing connection ",e);
				}				
			} else if (status==Status.BUFFER_UNDERFLOW) {
				
				//roller already contains previous so no work but to cancel the outgoing write
				if (cc.localRunningBytesProduced==0) {
					PipeWriter.wrappedUnstructuredLayoutBufferCancel(target);							
				} else {
					if (cc.localRunningBytesProduced>0) {
						if (!PipeWriter.tryWriteFragment(target, ClientNetResponseSchema.MSG_SIMPLERESPONSE_210)) {
							throw new RuntimeException("already checked for space should not happen.");
						}
						PipeWriter.wrappedUnstructuredLayoutBufferClose(target, ClientNetResponseSchema.MSG_SIMPLERESPONSE_210_FIELD_PAYLOAD_204, cc.localRunningBytesProduced);
						cc.localRunningBytesProduced = -1;
						
						//assert(longs match the one for cc)
						PipeWriter.writeLong(target, ClientNetResponseSchema.MSG_SIMPLERESPONSE_210_FIELD_CONNECTIONID_201, cc.id);
						//    PipeReader.copyLong(source, target, ClientNetResponseSchema.MSG_RESPONSE_200_FIELD_CONNECTIONID_201, ClientNetResponseSchema.MSG_SIMPLERESPONSE_210_FIELD_CONNECTIONID_201);
						
						PipeWriter.publishWrites(target);
					}
				}
			} else {				
				assert(status == Status.BUFFER_OVERFLOW);
				
				throw new RuntimeException("server is untrustworthy? Output pipe is too small for the content to be written inside "+
			                               target.maxAvgVarLen+" reading from "+rolling.position());
				

			}	
		}
	}

	private static SSLEngineResult unwrappedResultStatusState(Pipe<ClientNetResponseSchema> source, ByteBuffer rolling,
			ClientConnection cc, final ByteBuffer[] targetBuffer) {
		
		SSLEngineResult result=null;
				
		ByteBuffer[] inputs = PipeReader.wrappedUnstructuredLayoutBuffer(source, ClientNetResponseSchema.MSG_RESPONSE_200_FIELD_PAYLOAD_203);
		
		cc.localRunningBytesProduced = 0;
		if (inputs[1].remaining()==0) {
			//if we have some rolling data from previously
			if (rolling.position()==0) {
				try {
					result = cc.engine.unwrap(inputs[0], targetBuffer);
					cc.localRunningBytesProduced += result.bytesProduced();
				
					rolling.put(inputs[0]);//keep anything left for next time.
					assert(0==inputs[0].remaining());
					
					while (result.getStatus() == Status.OK && rolling.position()>0) {					
						rolling.flip();
						result = cc.engine.unwrap(rolling, targetBuffer);
						cc.localRunningBytesProduced += result.bytesProduced();
						
		
						rolling.compact();
					}
				} catch (SSLException sslex) {
					manageException(sslex, cc);
					System.err.println("no unwrap, exception");
					result = null;
				}
				
				
			} else {
				//add this new content onto the end before use.
				rolling.put(inputs[0]);
				assert(0==inputs[0].remaining());
			
				//flip so unwrap can see it
				rolling.flip();
				try {
					//Takes single buffer in. NOTE: NONE OF THIS LOGIC WOULD HAVE BEEN REQURIED IF SSLEngine.unwrap took ByteBuffer[] instead of a single !!
					result = cc.engine.unwrap(rolling, targetBuffer);
					cc.localRunningBytesProduced += result.bytesProduced();
				
					rolling.compact();	
					
					while (result.getStatus() == Status.OK && rolling.position()>0) {
						rolling.flip();
					    result = cc.engine.unwrap(rolling, targetBuffer);
						cc.localRunningBytesProduced += result.bytesProduced();
													
						rolling.compact();
					}
				} catch (SSLException sslex) {
					manageException(sslex, cc);
					System.err.println("no unwrap, exception 3");
					result = null;
					
				}
				
			}
		} else {
			//working this is confirmed as used.
			//System.out.println("***************  A and B ");
			assert(inputs[0].hasRemaining());
			assert(inputs[1].hasRemaining());
			
			rolling.put(inputs[0]);  //slow copy
			rolling.put(inputs[1]);  //slow copy
			rolling.flip();
			try {
				result = cc.engine.unwrap(rolling, targetBuffer);
				cc.localRunningBytesProduced += result.bytesProduced();
				
				rolling.compact();
				
				while (result.getStatus() == Status.OK && rolling.position()>0) {
					rolling.flip();
					result = cc.engine.unwrap(rolling, targetBuffer);
					cc.localRunningBytesProduced += result.bytesProduced();
					
					rolling.compact();
				}
				
			} catch (SSLException sslex) {
				manageException(sslex, cc);
				System.err.println("no unwrap, exception 5");
				result = null;
				
			}
		}

		PipeReader.releaseReadLock(source);	
		
		return result;
	}

    
	
	private static void manageException(SSLException sslex, ClientConnection cc) {
		try {
			cc.close();
		} catch (Throwable t) {
			//ignore we are closing this connection
		}
		log.error("Unable to encrypt closed conection",sslex);
	}

	
	private static boolean handshakeWrapLogic(ClientConnection cc, Pipe<ClientNetRequestSchema> target, ByteBuffer buffer) {
	    
		try {
			final ByteBuffer[] targetBuffers = PipeWriter.wrappedUnstructuredLayoutBufferOpen(target, ClientNetRequestSchema.MSG_ENCRYPTEDREQUEST_110_FIELD_ENCRYPTED_104);
			SSLEngineResult result = cc.engine.wrap(cc.noDatas, targetBuffers[0]);
			Status status = wrapResultStatusState(target, buffer, cc, cc.noDatas, targetBuffers, result);
			
			if (Status.OK == status) {
				
				PipeWriter.writeLong(target, ClientNetRequestSchema.MSG_ENCRYPTEDREQUEST_110_FIELD_CONNECTIONID_101, cc.id);
				
				PipeWriter.publishWrites(target);
				HandshakeStatus handshakeStatus = result.getHandshakeStatus();
				
				return (HandshakeStatus.NOT_HANDSHAKING != handshakeStatus) && (HandshakeStatus.FINISHED != handshakeStatus);
				
			}
		} catch (SSLException e) {
			PipeWriter.wrappedUnstructuredLayoutBufferCancel(target);
		}	
		
	    return false;	
	    
	}
	
	
	static boolean handShakeWrapIfNeeded(ClientConnection cc, Pipe<ClientNetRequestSchema> target, ByteBuffer buffer) {
		 HandshakeStatus handshakeStatus = cc.engine.getHandshakeStatus();
		 
		 if (HandshakeStatus.NOT_HANDSHAKING != handshakeStatus &&
				 HandshakeStatus.FINISHED != handshakeStatus	 ) {
			 			 
			 if (HandshakeStatus.NEED_UNWRAP == handshakeStatus) {
				 return true;//done by the other stage.
			 }
			 
			 if (HandshakeStatus.NEED_WRAP == handshakeStatus) {
				 return handshakeWrapLogic(cc, target, buffer);
			 }
			 
			 if (HandshakeStatus.NEED_TASK == handshakeStatus) {
				 
	                Runnable task;
	                while ((task = cc.engine.getDelegatedTask()) != null) {
	                	task.run(); //NOTE: could be run in parallel but we only have 1 thread now
	                }
	                handshakeStatus = cc.engine.getHandshakeStatus();
	                
	                return (HandshakeStatus.NOT_HANDSHAKING != handshakeStatus) && (HandshakeStatus.FINISHED != handshakeStatus);
			 }
		 }
		 return false;
	}
	
	public static boolean handShakeUnWrapIfNeeded(ClientConnection cc, Pipe<ClientNetResponseSchema> source, ByteBuffer rolling, ByteBuffer[] workspace, 
			                                      Pipe<ClientNetRequestSchema> handshakePipe, ByteBuffer secureBuffer) {
		
		 HandshakeStatus handshakeStatus = cc.engine.getHandshakeStatus();
		 
		 if (HandshakeStatus.NOT_HANDSHAKING != handshakeStatus &&
			 HandshakeStatus.FINISHED != handshakeStatus	 ) {
		
			 if (HandshakeStatus.NEED_WRAP == handshakeStatus) {
				 if (null == handshakePipe) {
					 //Client Mode
					 return true;//done by the other stage. 
				 } else {
					 //Server Mode
					 return handshakeWrapLogic(cc, handshakePipe, secureBuffer);
				 }
			 }
			 
			 if (HandshakeStatus.NEED_UNWRAP == handshakeStatus) {
								 
				 	if (!PipeReader.tryReadFragment(source)) {
				 		throw new UnsupportedOperationException();
				 	}
				 	
				 	int msgId = PipeReader.getMsgIdx(source);
				 					 	
				    //if closed closd
	                if (msgId<0 || !cc.isValid) {	                	
	                	PipeReader.releaseReadLock(source);
	                    if (cc.engine.isInboundDone() && cc.engine.isOutboundDone()) {
	                        return true;
	                    }	                    
	                    handshakeStatus = cc.closeInboundCloseOutbound(cc.engine);
	                    return true;
	                }
	    			
	    			SSLEngineResult result = unwrappedResultStatusState(source, rolling, cc, workspace);
	    			
	    			handshakeStatus = result.getHandshakeStatus();
					Status status = null!=result?result.getStatus():null;

					if (Status.CLOSED == status) {
						 if (cc.engine.isOutboundDone()) {
							 	cc.close();
		                    	return true;	
		                    } else {
		                        cc.engine.closeOutbound();
		                        handshakeStatus = cc.engine.getHandshakeStatus();
		                    }
					}
				 
				    return (HandshakeStatus.NOT_HANDSHAKING != handshakeStatus) && (HandshakeStatus.FINISHED != handshakeStatus);
			 }
			 
			 if (HandshakeStatus.NEED_TASK == handshakeStatus) {
	                Runnable task;
	                while ((task = cc.engine.getDelegatedTask()) != null) {
	                	task.run(); //NOTE: could be run in parallel but we only have 1 thread now
	                }
	                handshakeStatus = cc.engine.getHandshakeStatus();
	                
	                return (HandshakeStatus.NOT_HANDSHAKING != handshakeStatus) && (HandshakeStatus.FINISHED != handshakeStatus);
			 }
			 
		 }
		 return false;
	}
	
	
	private final ByteBuffer noData = ByteBuffer.allocate(0);      
	private final ByteBuffer[] noDatas = new ByteBuffer[]{noData,noData};
	

//	//TODO: needs more review to convert this into a garbage free version
//    private boolean doHandshake(SocketChannel socketChannel, SSLEngine engine) throws IOException {
//
//        SSLEngineResult result;
//        HandshakeStatus handshakeStatus;
//
//        // NioSslPeer's fields myAppData and peerAppData are supposed to be large enough to hold all message data the peer
//        // will send and expects to receive from the other peer respectively. Since the messages to be exchanged will usually be less
//        // than 16KB long the capacity of these fields should also be smaller. Here we initialize these two local buffers
//        // to be used for the handshake, while keeping client's buffers at the same size.
//        int appBufferSize = engine.getSession().getApplicationBufferSize()*2;
//        
//        ByteBuffer handshakePeerAppData = ByteBuffer.allocate(appBufferSize);
//        
//        SSLSession session = engine.getSession();
//        ByteBuffer myNetData = ByteBuffer.allocate(Math.max(1<<14, session.getPacketBufferSize()));
//        
//        //These are the check sizes we read from the server.
//        ByteBuffer peerNetData = ByteBuffer.allocate(Math.max(1<<15, session.getPacketBufferSize()));
//        
//        myNetData.clear();
//        peerNetData.clear();
//        
//      //only one.  System.err.println("call to handshake");
//        
//        handshakeStatus = engine.getHandshakeStatus();
//        while (handshakeStatus != SSLEngineResult.HandshakeStatus.FINISHED && 
//        	   handshakeStatus != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
//        	
//        	//TODO: call this block based on in/out pipe status.
//        	//if (handshakeStatus!= HandshakeStatus.NEED_UNWRAP)
//        	//System.err.println("do handshake status "+handshakeStatus);
//        	
//        	/*
//        	 * for client we have
//        	 *          do handshake status NEED_WRAP
//						do handshake status NEED_TASK
//						do handshake status NEED_TASK
//						do handshake status NEED_TASK
//						do handshake status NEED_TASK
//						do handshake status NEED_WRAP
//						do handshake status NEED_WRAP
//						do handshake status NEED_WRAP
//
//                        Plus Many many NEED_UNWRAP.
//        	 * 
//        	 * 
//        	 */
//        	
//        	
//            switch (handshakeStatus) {
//	            case NEED_UNWRAP:
//	            	
//	            	int temp = 0;
//	            	do {
//	            		temp = socketChannel.read(peerNetData); //TODO: update to be non blocking. read from PIPE in.
//	            	} while (temp>0);
//	            	
//	            //	System.err.println("unwrapped:"+temp); //this is often zero!!!
//	            	
//	            	
//	            	
//	            	
//	            	
//	            	
//	                if (temp < 0) {
//	                    if (engine.isInboundDone() && engine.isOutboundDone()) {
//	                        return false;
//	                    }	                    
//	                    handshakeStatus = closeInboundCloseOutbound(engine);
//	                    break;
//	                }
//	                peerNetData.flip();
//	                try {
//	                	
//	                    result = engine.unwrap(peerNetData, handshakePeerAppData);
//	                    
//	                    //we never see the data but the unwrap process is using this space for work!
//	                    if (handshakePeerAppData.position()>0) {
//	                    	System.err.println("unwrapped dropped data"+handshakePeerAppData.position());
//	                    }
//	                    
//	                    peerNetData.compact();//bad 
//	                    handshakeStatus = result.getHandshakeStatus();
//	                    	                    
//	                } catch (SSLException sslException) {
//	                	if (!isShuttingDown) {
//	                		log.error("A problem was encountered while processing the data that caused the SSLEngine to abort. Will try to properly close connection...", sslException);
//	                	}
//	                    engine.closeOutbound();
//	                    handshakeStatus = engine.getHandshakeStatus();
//	                    break;
//	                }
//	                
//	                
//	                Status status = result.getStatus();
//	         	    if (Status.CLOSED == status) {
//	         	       if (engine.isOutboundDone()) {
//	         	    	 //  System.err.println("done");
//	                    	return false;	
//	                    } else {
//	                        engine.closeOutbound();
//	                        handshakeStatus = engine.getHandshakeStatus();
//	                        //System.err.println(handshakeStatus);
//	                    }
//	         	    	
//	         	    } else if (Status.BUFFER_OVERFLOW == status) {
//	         	    	throw new UnsupportedOperationException("Buffer overflow, the peerAppData must be larger or the server is sending responses too large");
//	         	    } else if (Status.BUFFER_UNDERFLOW == status) {
//		         	   	if (peerNetData.position() == peerNetData.limit()) {
//	                		throw new UnsupportedOperationException("Should not happen but ByteBuffer was too small upon construction");
//	                	} else {
//	                		//do nothing since the server is not yet talking to us.
//	                	}
//	         	    }
//	                             
//	                
//	                
//	                break;
//	            case NEED_WRAP:
//	                myNetData.clear();
//	                try {
//	                    result = engine.wrap(noData, myNetData);
//	                   
//	                    
//	                    handshakeStatus = result.getHandshakeStatus();
//	                } catch (SSLException sslException) {
//	                    log.error("A problem was encountered while processing the data that caused the SSLEngine to abort. Will try to properly close connection...", sslException);
//	                    engine.closeOutbound();
//	                    handshakeStatus = engine.getHandshakeStatus();
//	                    break;
//	                }
//	                
//	                
//	                status = result.getStatus();
//	                
//	                
//	                //write out if ok or closed.
//	                
//	                if (Status.OK == status || Status.CLOSED == status) {
//	                	
//	                	myNetData.flip();
//	                	
//	                	try {
//		                    while (myNetData.hasRemaining()) {
//		                        socketChannel.write(myNetData); //TODO: update to be non blocking. Write to PIPE out
//		                    }
//		                    
//		                    if (Status.CLOSED == status) {
//		                    	 peerNetData.clear();
//		                    }
//		                    
//	                	} catch (Exception e) {
//	                		 log.warn("Unable to write to socket",e);
//	                		 handshakeStatus = engine.getHandshakeStatus();
//	                	}
//	                	                                	
//	                } else if (Status.BUFFER_OVERFLOW == status) {
//	                	 throw new UnsupportedOperationException("Buffer overflow, the pipe must be larger or the server is sending responses too large");
//	                } else if (Status.BUFFER_UNDERFLOW == status) {
//	                	 throw new SSLException("Buffer underflow occured after a wrap. I don't think we should ever get here.");
//	                }
//	                               
//	                
//	                
//	                break;
//	            case NEED_TASK:
//	                Runnable task;
//	                while ((task = engine.getDelegatedTask()) != null) {
//	                	task.run(); //NOTE: could be run in parallel but we only have 1 thread now
//	                }
//	                handshakeStatus = engine.getHandshakeStatus();
//	                break;
//	            case FINISHED:
//	                break;
//	            case NOT_HANDSHAKING:
//	                break;
//	            default:
//	                throw new IllegalStateException("Invalid SSL status: " + handshakeStatus);
//	        }
//        }
//
//        return true;
//
//    }

	private HandshakeStatus closeInboundCloseOutbound(SSLEngine engine) {
		HandshakeStatus handshakeStatus;
		try {
		    engine.closeInbound();
		} catch (SSLException e) {
		    boolean debug = false;
		    if (debug) {
		    	if (!isShuttingDown) {	//if we are shutting down this is not an error.                    	
		    		log.trace("This engine was forced to close inbound, without having received the proper SSL/TLS close notification message from the peer, due to end of stream.", e);
		    	}
		    }
			
		}
		engine.closeOutbound();
		// After closeOutbound the engine will be set to WRAP state, in order to try to send a close message to the client.
		handshakeStatus = engine.getHandshakeStatus();
		return handshakeStatus;
	}



	
}
