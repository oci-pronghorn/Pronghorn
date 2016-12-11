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
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class ClientConnection extends SSLConnection {

	static final Logger logger = LoggerFactory.getLogger(ClientConnection.class);

	private static final byte[] EMPTY = new byte[0];
		
	private SelectionKey key; //only registered after handshake is complete.

	private final byte[] connectionGUID;
	
	private final int pipeIdx;
	
	private long requestsSent;
	private long responsesReceived;
	
	private final int userId;
	private final String host;
	private final int port;
	private long lastUsedTime;
	
	private static InetAddress testAddr;
	
	private long closeTimeLimit = Long.MAX_VALUE;
	private long TIME_TILL_CLOSE = 10_000;

	
	static {
		
		boolean testForExternalNetwork = false;
		
		if (testForExternalNetwork) {
			try {
				testAddr = InetAddress.getByName("www.google.com");
			} catch (UnknownHostException e) {
				logger.error("no network connection.");
				System.exit(-1);
			}		
		}
	}
	
	private boolean hasNetworkConnectivity() {
		return true;
		
//		//TODO: this detection is not yet perfected and throws NPE upon problems
//		try {
//			return testAddr.isReachable(10_000);
//		} catch (IOException e) {
//			return false;
//		}
	}
	
	public void setLastUsedTime(long time) {
		lastUsedTime = time;
	}
	
	public long getLastUsedTime() {
		return lastUsedTime;
	}
	
	public ClientConnection(String host, byte[] hostBacking, int hostPos, int hostLen, int hostMask, 
			                 int port, int userId, int pipeIdx, long conId) throws IOException {
		super(SSLEngineFactory.createSSLEngine(host, port), SocketChannel.open(), conId);
		
		assert(port<=65535);		
		
		// RFC 1035 the length of a FQDN is limited to 255 characters
		this.connectionGUID = new byte[(2*host.length())+6];
		buildGUID(connectionGUID, hostBacking, hostPos, hostLen, hostMask, port, userId);
		this.pipeIdx = pipeIdx;
		this.userId = userId;
		this.host = host;
		this.port = port;
		
		this.getEngine().setUseClientMode(true);
				
		this.getSocketChannel().configureBlocking(false);  
	//	this.getSocketChannel().setOption(StandardSocketOptions.SO_KEEPALIVE, true);
		this.getSocketChannel().setOption(StandardSocketOptions.TCP_NODELAY, true); 
		this.getSocketChannel().setOption(StandardSocketOptions.SO_RCVBUF, 1<<17); 
		
	//	logger.info("recv buffer size {} ",  getSocketChannel().getOption(StandardSocketOptions.SO_RCVBUF));
		
						
		try {
			InetSocketAddress remote = new InetSocketAddress(host, port);
			this.getSocketChannel().connect(remote);
		} catch (UnresolvedAddressException uae) {
			
			if (hasNetworkConnectivity()) {
				logger.error("unable to find {}:{}",host,port);
				throw uae;
			} else {
				logger.error("No network connection.");
				System.exit(-1);						
			}
		}
		this.getSocketChannel().finishConnect(); //call again later to confirm its done.	

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


	public int requestPipeLineIdx() {
		return pipeIdx;
	}

	public static int buildGUID(byte[] target, byte[] hostBack, int hostPos, int hostLen, int hostMask, int port, int userId) {
		//TODO: if we find a better hash for host port user we can avoid this trie lookup. TODO: performance improvement.
		//new Exception("build guid").printStackTrace();

		int pos = 0;
			
		Pipe.copyBytesFromToRing(hostBack, hostPos, hostMask, target, pos, Integer.MAX_VALUE, hostLen);
		pos+=hostLen;
		
    	target[pos++] = (byte)(port>>8);
    	target[pos++] = (byte)(port);
    	
    	target[pos++] = (byte)(userId);
    	target[pos++] = (byte)(userId>>8);
    	target[pos++] = (byte)(userId>>16);
    	target[pos++] = (byte)(userId>>24);
    	
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
		try {
			return getSocketChannel().finishConnect();
		} catch (IOException io) {
			return false;
		}
	}

	public boolean isRegistered() {
		return this.key!=null;
	}
		
	public void registerForUse(Selector selector, Pipe<NetPayloadSchema>[] handshakeBegin, boolean isTLS) throws IOException {

		assert(getSocketChannel().finishConnect());
		
		if (isTLS) {
			getEngine().beginHandshake();
			
			HandshakeStatus handshake = getEngine().getHandshakeStatus();
			if (HandshakeStatus.NEED_TASK == handshake) { 				
	             Runnable task;
	             while ((task = getEngine().getDelegatedTask()) != null) {
	                	task.run(); 
	             }
			} else if (HandshakeStatus.NEED_WRAP == handshake) {
												
				int i = handshakeBegin.length;
				while (--i>=0) {
					Pipe<NetPayloadSchema> pipe = handshakeBegin[i];

					if (PipeWriter.tryWriteFragment(pipe, NetPayloadSchema.MSG_PLAIN_210) ) {
						
						PipeWriter.writeLong(pipe, NetPayloadSchema.MSG_PLAIN_210_FIELD_CONNECTIONID_201, getId());
						PipeWriter.writeLong(pipe, NetPayloadSchema.MSG_PLAIN_210_FIELD_POSITION_206, SSLUtil.HANDSHAKE_POS); //signal that WRAP is needed 
						PipeWriter.writeBytes(pipe, NetPayloadSchema.MSG_PLAIN_210_FIELD_PAYLOAD_204, EMPTY);
						PipeWriter.publishWrites(pipe);	
						
						//we did it, hurrah
						break;							
					}						
				}
				if (i<0) {
					throw new UnsupportedOperationException("unable to wrap handshake no pipes are avilable.");
				}				
				
			}			
						
		}
		isValid = true;
		this.key = getSocketChannel().register(selector, SelectionKey.OP_READ, this); 

	}


	public boolean isValid() {

		if (!getSocketChannel().isConnected()) {
			return false;
		}
		
		//TODO: new auto shutdown logic for old unused connections, Still under development,closes connections too soon
//		if (responsesReceived==requestsSent && System.currentTimeMillis() > closeTimeLimit) {
//			log.info("stale connection closed after non use {}",this);
//			
//			//TODO: this work can not be done here and needs to be owned by the pipe. HTTPClientReqeust is the ideal place.
//			
//			beginDisconnect(); 
//			
//			if (true) {
//				throw new UnsupportedOperationException("Can not close old connection without finishing handshake.");
//			}
//			
//			close();
//			return false;
//		}
		return isValid;
	}


	public void beginDisconnect() {
		try {
			 isDisconnecting = true;
			 getEngine().closeOutbound();
		} catch (Throwable e) {
			logger.warn("Error closing connection ",e);
			close();
		}
	}
		
	

	
	
	
	

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



	
}
