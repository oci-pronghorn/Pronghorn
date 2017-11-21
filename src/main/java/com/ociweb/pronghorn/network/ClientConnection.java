package com.ociweb.pronghorn.network;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.util.Appendables;

import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.net.UnknownHostException;
import java.nio.channels.NoConnectionPendingException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;


public class ClientConnection extends SSLConnection {

	public static int resolveWithDNSTimeoutMS = 10_000; //  §6.1.3.3 of RFC 1123 suggests a value not less than 5 seconds.
	private static final long MAX_HIST_VALUE = 40_000_000_000L;
	private static final Logger logger = LoggerFactory.getLogger(ClientConnection.class);
	private static final byte[] EMPTY = new byte[0];
	private static InetAddress testAddr; //must be here to enure JIT does not delete the code
		
	private SelectionKey key; //only registered after handshake is complete.

	private final byte[] connectionGUID;
	private final int    connectionGUIDLength;
	
	private final int pipeIdx;
	
	private long requestsSent;
	private long responsesReceived;
	
	private final int sessionId;
	private final String host;
	private final int port;
	
	private long lastUsedTime;
	
	private long closeTimeLimit = Long.MAX_VALUE;
	private long TIME_TILL_CLOSE = 10_000;
	private Histogram histRoundTrip = new Histogram(MAX_HIST_VALUE,0);

	private final int maxInFlightBits;
	public  final int maxInFlight;
	private final int maxInFlightMask;

	private int inFlightTimeSentPos;
	private int inFlightTimeRespPos;	
	private long[] inFlightTimes;
	
	private int inFlightRoutesSentPos;
	private int inFlightRoutesRespPos;
	private long[] inFlightRoutes;

	
	private boolean isTLS;
	boolean isFinishedConnection = false;
	
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
	
	public ClientConnection(SSLEngine engine, CharSequence host, int port, int sessionId, int pipeIdx,
			                 long conId, boolean isTLS, int inFlightBits, int maxRecBuf) throws IOException {

		super(engine, SocketChannel.open(), conId);
		
		this.maxInFlightBits = inFlightBits;
		this.maxInFlight = 1<<maxInFlightBits;
		this.maxInFlightMask = maxInFlight-1;
		this.inFlightTimes = new long[maxInFlight];
		this.inFlightRoutes = new long[maxInFlight];
		
		this.isTLS = isTLS;
		
		if (isTLS) {
			getEngine().setUseClientMode(true);
		}
		
		assert(port<=65535);		
		// RFC 1035 the length of a FQDN is limited to 255 characters
		this.connectionGUID = new byte[(2*host.length())+6];
		this.connectionGUIDLength = buildGUID(connectionGUID, host, port, sessionId);
		this.pipeIdx = pipeIdx;
		this.sessionId = sessionId;
		this.host = host instanceof String ? (String)host : host.toString();
		this.port = port;
					
		this.getSocketChannel().configureBlocking(false);  
		this.getSocketChannel().setOption(StandardSocketOptions.SO_KEEPALIVE, true);
	
		//TCP_NODELAY is required for HTTP/2 get used to to being on.
		this.getSocketChannel().setOption(StandardSocketOptions.TCP_NODELAY, true);
	
		this.getSocketChannel().setOption(StandardSocketOptions.SO_RCVBUF, 1<<16); 
		
		//TODO: we know the pipe size but the socket takes this as a suggestion...
		
		
		this.getSocketChannel().setOption(StandardSocketOptions.SO_SNDBUF, 1<<16); 
						
		//logger.info("client recv buffer size {} ",  getSocketChannel().getOption(StandardSocketOptions.SO_RCVBUF)); //default 43690
		//logger.info("client send buffer size {} ",  getSocketChannel().getOption(StandardSocketOptions.SO_SNDBUF)); //default  8192
	
		resolveAddressAndConnect(port);		
	}


	private void resolveAddressAndConnect(int port) throws IOException {

				
		InetAddress[] ipAddresses = null;
		boolean failureDetected = false;
		long resolveTimeout = System.currentTimeMillis()+resolveWithDNSTimeoutMS;
		long msSleep = 1;
		do { //NOTE: warning this a blocking loop looking up DNS, should replace with non blocking... See Netty
			try {
				if (failureDetected) {
					//we have done this before so slow down a little
					try {
						Thread.sleep(msSleep); //for each attempt double ms waiting for next call
						msSleep <<= 1;
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						return;
					} //2 ms					
				}
				
				long s = System.nanoTime();
				ipAddresses = InetAddress.getAllByName(host);
				long d = System.nanoTime()-s;
				
				if (d>1_000_000_000) {
					logger.info("warning slow DNS took {} sec to resolve {} to {} ",d/1_000_000_000,host,ipAddresses);
				}
			} catch (UnknownHostException unknwnHostEx) {
				failureDetected = true;
			}
		} while (null == ipAddresses && System.currentTimeMillis()<resolveTimeout);
		
		
		
		
		if (null==ipAddresses || ipAddresses.length==0) {
			//unresolved
			logger.error("unable to resolve address for {}:{}",host,port);
			new Exception("we have host: "+host+" and port: "+port).printStackTrace();
			return;
		} else {
			this.getSocketChannel().connect(new InetSocketAddress(ipAddresses[0], port));
		}

		this.getSocketChannel().finishConnect(); //call again later to confirm its done.

	}

	public String getHost() {
		return host;
	}
	public int getPort() {
		return port;
	}
	
	public String toString() {
		return host+":"+port;
	}
	
	@Deprecated
	public int getUserId() {
		return getSessionId();
	}
	
	public int getSessionId() {
		return sessionId;
	}
	
	public void incRequestsSent() {
		closeTimeLimit = Long.MAX_VALUE;
		requestsSent++;		
	}
	
//	public void waitForMatch() {
//		while (responsesReceived<requestsSent) {
//			Thread.yield();
//		}
//	}
	
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

	public static int buildGUID(byte[] target, CharSequence host, int port, int userId) {
		//TODO: if we find a better hash for host port user we can avoid this trie lookup. TODO: performance improvement.
        //      RABIN hash may be just the right thing.
		
		int pos = Pipe.copyUTF8ToByte(host, 0, target, Integer.MAX_VALUE, 0, host.length());
				
		return finishBuildGUID(target, port, userId, pos);
	}
	
	public static int buildGUID(byte[] target, byte[] hostBack, int hostPos, int hostLen, int hostMask, int port, int userId) {
		//TODO: if we find a better hash for host port user we can avoid this trie lookup. TODO: performance improvement.
        //      RABIN hash may be just the right thing.
		
		Pipe.copyBytesFromToRing(hostBack, hostPos, hostMask, 
				                 target, 0, Integer.MAX_VALUE, 
				                 hostLen);

		int pos = hostLen;
				
		return finishBuildGUID(target, port, userId, pos);
	
	}

	private static int finishBuildGUID(byte[] target, int port, int userId, int pos) {
		
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
	public int GUIDLength() {
		return connectionGUIDLength;
	}
	
	/**
	 * After construction this must be called until it returns true before using this connection. 
	 */
	public boolean isFinishConnect() {
		if (isFinishedConnection) {
			return true;
		} else {
			try {
				
				boolean finishConnect = getSocketChannel().finishConnect();
			    isFinishedConnection |= finishConnect;
				return finishConnect;
				
			} catch (IOException io) {
				return false;
			} catch (NoConnectionPendingException ncpe) {
				close();
				return false;
			}
		}
	}

	public boolean isRegistered() {
		return this.key!=null;
	}
		
	public void registerForUse(Selector selector, Pipe<NetPayloadSchema>[] handshakeBegin, boolean isTLS) throws IOException {

		assert(getSocketChannel().finishConnect());
		
		//logger.trace("now finished connection to : {} ",getSocketChannel().getRemoteAddress().toString());
		
		if (isTLS) {

			getEngine().beginHandshake();
			
			HandshakeStatus handshake = getEngine().getHandshakeStatus();
			if (HandshakeStatus.NEED_TASK == handshake) { 				
	             Runnable task;
	             while ((task = getEngine().getDelegatedTask()) != null) {
	                	task.run(); 
	             }
			} else if (HandshakeStatus.NEED_WRAP == handshake) {
								
				
				int c= (int)getId()%handshakeBegin.length;				
				
				int j = handshakeBegin.length;
				while (--j>=0) {
						
					
					Pipe<NetPayloadSchema> pipe = handshakeBegin[c];
					assert(null!=pipe);
					
					if (Pipe.hasRoomForWrite(pipe)) {
					
						logger.trace("ClientConnection request wrap for id {} to pipe {}",getId(), pipe);
						
						int size = Pipe.addMsgIdx(pipe, NetPayloadSchema.MSG_PLAIN_210);
						Pipe.addLongValue(getId(), pipe);
						Pipe.addLongValue(System.currentTimeMillis(), pipe);
						Pipe.addLongValue(SSLUtil.HANDSHAKE_POS, pipe);
						Pipe.addByteArray(EMPTY, 0, 0, pipe);
						Pipe.confirmLowLevelWrite(pipe, size);
						Pipe.publishWrites(pipe);
						
						//we did it, hurrah
						break;
					} else {
						logger.info("ERROR SCKIPPED THE REQUEST TO WRAP WRITE!!!!!!!!");
					}
					
					if (--c<0) {
						c = handshakeBegin.length-1;
					}
					
				}				
				
				
				
				if (j<0) {
					throw new UnsupportedOperationException("unable to wrap handshake no pipes are avilable.");
				}				
				
			}			
						
		}
		isValid = true;
		//logger.info("is now valid");
		this.key = getSocketChannel().register(selector, SelectionKey.OP_READ, this); 

	}


	public boolean isValid() {

		if (!getSocketChannel().isConnected()) {
			return false;
		}
		return isValid;
	}


	public void beginDisconnect() {

		try {
			 isDisconnecting = true;
			 if (isTLS) {
				 SSLEngine eng = getEngine();
				 if (null!=eng) {
					 eng.closeOutbound();
				 }
			 }
		} catch (Throwable e) {
			logger.warn("Error closing connection ",e);
			close();
		}
	}


	
	public Histogram histogram() {
		return histRoundTrip;
	}

	public void recordSentTime(long time) {
		inFlightTimes[++inFlightTimeSentPos & maxInFlightMask] = time;		
	}

	public void recordArrivalTime(long time) {
		long value = time - inFlightTimes[++inFlightTimeRespPos & maxInFlightMask];
			
		if (value>=0 && value<MAX_HIST_VALUE) {
		
			boolean showAllTimes = false;
			if (showAllTimes) {
				Appendables.appendNearestTimeUnit(System.err, value, " client latency\n");
			}
			histRoundTrip.recordValue(value);
		}
	}
		
	public boolean isBusy() {
		//turn back on after this is everywhere.
		return  ((maxInFlightMask&inFlightRoutesSentPos) != (maxInFlightMask&inFlightRoutesRespPos)) &&
				((maxInFlightMask&inFlightRoutesSentPos) == (maxInFlightMask&(maxInFlight+inFlightRoutesRespPos)));
	}
		
	public void recordDestinationRouteId(long id) {
		inFlightRoutes[++inFlightRoutesSentPos & maxInFlightMask] = id;
	}
	
	public long consumeDestinationRouteId() {
		return inFlightRoutes[++inFlightRoutesRespPos & maxInFlightMask];
	}
	
	public long readDestinationRouteId() {
		return inFlightRoutes[(1+inFlightRoutesRespPos) & maxInFlightMask];
	}

	
	/////////////////////////
	//This is for asserting of thread safety
	/////////////////////////
	
	private int usingStage = -1;
	
	public boolean singleUsage(int stageId) {
		if (-1 == usingStage) {
			usingStage = stageId;
			return true;
		} else {
			return usingStage == stageId;
		}
	}
	
}
