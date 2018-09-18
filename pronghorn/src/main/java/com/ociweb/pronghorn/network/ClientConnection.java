package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.net.UnknownHostException;
import java.nio.channels.NoConnectionPendingException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.scheduling.ElapsedTimeRecorder;
import com.ociweb.pronghorn.util.Appendables;


public class ClientConnection extends BaseConnection implements SelectionKeyHashMappable {

	//TODO: limitations with client side calls
	//      only supports 1 struct or 1 JSON parser or 1 set of headers per ClientSession
	//      this may force developers to use interleaving in some cases to solve this 
	//      paths will need to be pre-defined and given identifications.  (future feature)
	
	public static int resolveWithDNSTimeoutMS = 10_000; //  §6.1.3.3 of RFC 1123 suggests a value not less than 5 seconds.
	private static final long MAX_HIST_VALUE = 40_000_000_000L;
	private static final Logger logger = LoggerFactory.getLogger(ClientConnection.class);
	private static final byte[] EMPTY = new byte[0];

	public static boolean logDisconnects = false;
	public static boolean logLatencyData = false;
	
	private SelectionKey key; //only registered after handshake is complete.

	private final int pipeIdx;
	
	private long requestsSent;
	private long responsesReceived;
	
	///////////////////////
	//TODO: Store the JSON Extractor here so we can apply it when the results come in??
	///////////////////////
	
	public final int sessionId;
	public final String host;
	public final int port;
	public final int hostId;
		  
	private long closeTimeLimit = Long.MAX_VALUE;
	private long TIME_TILL_CLOSE = 10_000;
	private ElapsedTimeRecorder histRoundTrip = new ElapsedTimeRecorder();

	private final static int maxInFlightBits  = 14;
	public  final static int maxInFlight      = 1<<maxInFlightBits;
	private final static int maxInFlightMask  = maxInFlight-1;
	
	//TODO: if memory needs to be saved this flight time feature could be "turned off" 
	private int inFlightTimeSentPos;
	private int inFlightTimeRespPos;	
	private final long[] inFlightTimes;
	
	private int inFlightRoutesSentPos;
	private int inFlightRoutesRespPos;
	private final int[] inFlightRoutes;

	private final long creationTimeNS;
	
	public final boolean isTLS;
	boolean isFinishedConnection = false;
	
	//also holds structureId for use as needed.  This can be any protocols payload for indexing
	protected final int structureId;
	
	private final int payloadSize;
	private final long timeoutNS;

	public int getStructureId() {
		return structureId;
	}
	
	public boolean close() {
		boolean result = super.close();
		SelectionKey localKey = key;		
		if (result && null!=localKey) {
			localKey.cancel();
		}		
		return result;
	}
	
	public ClientConnection(SSLEngine engine, int hostId, int port, int sessionId,
			                int pipeIdx, long conId, long timeoutNS, int structureId		                 
			 			  ) throws IOException {

		super(engine, SocketChannel.open(), conId);

		
		//TODO: add support to hold data to be returned to client responder.
		this.connectionDataWriter = null;
		
		this.structureId = structureId;
		this.timeoutNS =  timeoutNS;
		this.creationTimeNS = System.nanoTime();
		
		this.isTLS = (engine!=null);
		if (isTLS) {
			getEngine().setUseClientMode(true);
		}
		
		assert(port<=65535);		

		this.pipeIdx = pipeIdx;

		this.sessionId = sessionId;
		this.host = ClientCoordinator.registeredDomain(hostId);
		this.port = port;
		this.hostId = hostId;
		
		if (logDisconnects) {
			logger.info("new client socket connection to {}:{} session {}",host,port,sessionId);
		}
				
		initSocket(this.getSocketChannel());
						
		this.recBufferSize = this.getSocketChannel().getOption(StandardSocketOptions.SO_RCVBUF);

		this.payloadSize = isTLS ?
				Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_ENCRYPTED_200) :
				Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_PLAIN_210);
				
		resolveAddressAndConnect(port);		
		
		System.gc();
		inFlightTimes = new long[maxInFlight];
		inFlightRoutes = new int[maxInFlight];
		Arrays.fill(inFlightRoutes, -1);
	}

	public static void initSocket(SocketChannel socket) throws IOException {
		socket.configureBlocking(false);  
		socket.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
	
		//TCP_NODELAY is required for HTTP/2 get used to to being on.
		socket.setOption(StandardSocketOptions.TCP_NODELAY, true);
	}

	public final int recBufferSize; //cache
	
	public int recvBufferSize() {
		return recBufferSize;
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
			
			InetAddress inetAddress = ipAddresses[0];			
			this.getSocketChannel().connect(new InetSocketAddress(inetAddress, port));
			
			//if you see BindException: Can't assign requested address then something is
			//up with your local network and this machine no longer has access to that address and port.
			
		}
				
		/////////////////////////

		boolean done = false;
		do {		
			try {
				done = this.getSocketChannel().finishConnect(); //call again later to confirm its done.
			} catch(ConnectException ce) {
				if (ce.getMessage().equals("Connection refused")) {
					break;
				} else {
					throw ce;
				}
			}
			if (!done) {
				Thread.yield();
			}
		} while (!done && (System.currentTimeMillis()<resolveTimeout));
		
	}
	
	public String toString() {
		return "ConId:"+id+" "+host+":"+port;
	}
	
	public void incRequestsSent() {
		closeTimeLimit = Long.MAX_VALUE;
		requestsSent++;		
	}
	
//   public boolean close() {
//	   new Exception().printStackTrace();
//	   return super.close();
//   }
	
	
	
	public boolean incResponsesReceived() {
		assert(1+responsesReceived<=requestsSent) : "received more responses than requests were sent";
		boolean result = (++responsesReceived)==requestsSent;
		if (result) {

				closeTimeLimit = System.currentTimeMillis()+TIME_TILL_CLOSE;
			
		}
		return result;
	}
	
	public SelectionKey getSelectionKey() {
		return key;
	}


	public int requestPipeLineIdx() {
		return pipeIdx;
	}

	//new GUID value 16 port 48/2  24 bits for sessions/domains  16 million.
	//16ports 
	
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
	
	public static boolean showConectionDetails = false;
	/**
	 * After construction this must be called until it returns true before using this connection. 
	 */
	public boolean isFinishConnect() {
		if (isFinishedConnection) {
			return true;
		} else {
			try {
								
				SocketChannel socketChannel = getSocketChannel();
				if (socketChannel!=null) {
					boolean finishConnect = socketChannel.finishConnect();			    				
				    if (!finishConnect ) {
	
				    	if (System.nanoTime() > creationTimeNS+(resolveWithDNSTimeoutMS*1000000L)) {
				    		logger.info("connection timeout {} {}ms",this,resolveWithDNSTimeoutMS);
				    		beginDisconnect();
				    	}
				    	
				    } else {
				    	isFinishedConnection = true;			    	
				    	clearWaitingForNetwork();
				    	if (showConectionDetails) {
				    		logger.info("new connection completed to {}:{}",host,port);
				    	}
				    }
					return finishConnect;
				} else {
					close();
					isFinishedConnection = true;	
					return true;
				}
				
			} catch (IOException io) {
				close();
				if (showConectionDetails) {
					logger.info("finish connection exception ",io);
				}
				return false;
			} catch (NoConnectionPendingException ncpe) {
				close();
				if (showConectionDetails) {
					logger.info("no pending connection ",ncpe);
				}
				return false;
			}
		}
	}

	public boolean isRegistered() {
		return this.key!=null;
	}
		
	public void registerForUse(Selector selector, Pipe<NetPayloadSchema>[] handshakeBegin, boolean isTLS) throws IOException {

		SocketChannel socket = getSocketChannel();		
		assert(null==socket || socket.finishConnect());
		
		if (null==socket) {
			return;//not valid;
		}
		//logger.trace("now finished connection to : {} ",getSocketChannel().getRemoteAddress().toString());
		
		//logger.info("is now valid connection {} ",this.id);
		SelectionKey tempkey = getSocketChannel().register(selector, SelectionKey.OP_READ, this); 

		if (isTLS) {
			beginHandshakeNow(handshakeBegin);		
		}
		isValid = true;
		//must be last, this connection can not be used until this key is not null;
		this.key = tempkey;
	}

	private void beginHandshakeNow(Pipe<NetPayloadSchema>[] handshakeBegin) throws SSLException {

		getEngine().beginHandshake();
		logger.trace("\nbegin handshake for : {} pipes:{} ",getId(),handshakeBegin.length);
		//////////////////////////////
		HandshakeStatus handshake = getEngine().getHandshakeStatus();
		while (HandshakeStatus.NEED_TASK == handshake) { 				
			Runnable task;
			while ((task = getEngine().getDelegatedTask()) != null) {
				task.run(); 
			}
			handshake = getEngine().getHandshakeStatus();
		} 
        /////////////
		if (HandshakeStatus.NEED_WRAP == handshake) {							
				int c= (int)getId()%handshakeBegin.length;				
				boolean sent = false;
				do {
					int j = handshakeBegin.length;
					while (--j>=0) {//find first available pipe to send handshake
							
						final Pipe<NetPayloadSchema> pipe = handshakeBegin[c];
						assert(null!=pipe);
						
						if (Pipe.hasRoomForWrite(pipe)) {
						
							//Warning the follow on calls should be low level...
							//logger.warn("Low-Level ClientConnection request wrap for id {} to pipe {}",getId(), pipe, new Exception());
							
							////////////////////////////////////
							//NOTE: must repeat this until the handshake is finished.
							///////////////////////////////////
							final int size = Pipe.addMsgIdx(pipe, NetPayloadSchema.MSG_PLAIN_210);
							Pipe.addLongValue(getId(), pipe);
							Pipe.addLongValue(System.currentTimeMillis(), pipe);
							Pipe.addLongValue(SSLUtil.HANDSHAKE_POS, pipe);
							Pipe.addByteArray(EMPTY, 0, 0, pipe);
							
							Pipe.confirmLowLevelWrite(pipe, size);
							Pipe.publishWrites(pipe);
													
							sent = true;
							break;
						} else {
							//Why so much data??
					//		System.out.println("pipe is not long enough to begin handshake "+pipe);
							if (--c<0) {
								c = handshakeBegin.length-1;
							}
						}
					}
					if (j<0) {
						//begin handshake pipe should be longer to avoid this issue...
						logger.warn("Not enough open pipes available to begin handshake. Total pipes: "+handshakeBegin.length);
						Thread.yield();
					}
				} while (!sent);

		
		} else {
			logger.warn("need {} but this will not start the handshake.",handshake);
		}
	}

	public boolean isValid() {

		SocketChannel socketChannel = getSocketChannel();
		if (null==socketChannel || (!socketChannel.isConnected())) {
			if (logDisconnects) {
				logger.info("{}:{} session {} is no longer connected. It was opened {} ago.",
						host,port,sessionId,
						Appendables.appendNearestTimeUnit(new StringBuilder(), System.nanoTime()-creationTimeNS).toString()
					);
			}
			return false;
		}
		return isValid;
	}


	public boolean isDisconnecting() {
		return isDisconnecting;
	}
	
	public void beginDisconnect() {

		if (logLatencyData) {
			logger.info("closing connection, latencies for {}:{}\n{}",this.host,this.port,histRoundTrip);
		}
		
		try {
			 isDisconnecting = true;
			 if (isTLS) {
				 SSLEngine eng = getEngine(); ////TODO: is this needed and is it called too early??
				 if (null!=eng) {
					 eng.closeOutbound();
				 }
			 }
		} catch (Throwable e) {
			logger.warn("Error closing connection ",e);
			close();
		}
	}


	
	public ElapsedTimeRecorder histogram() {
		return histRoundTrip;
	}

	public void recordSentTime(long timeNS) {
		inFlightTimes[++inFlightTimeSentPos & maxInFlightMask] = timeNS;		
	}

	//important method to determine if the network was dropped while call was outstanding
	public long outstandingCallTime(long timeNS) {
		if (inFlightRoutesRespPos == inFlightTimeSentPos) {
			return -1;
		} else {			
			long sentTime = inFlightTimes[1+inFlightTimeRespPos & maxInFlightMask];
			if (sentTime > 0) {		
				return timeNS - sentTime;
			} else {
				return -1;//we read the value while it was being sent so discard
			}
		}
	}
	
	public void touchSentTime(long nowNS) {
		inFlightTimes[1+inFlightTimeRespPos & maxInFlightMask] = nowNS;
	}
	
	public void recordArrivalTime(long time) {
				
		long sentTime = inFlightTimes[++inFlightTimeRespPos & maxInFlightMask];
		if (sentTime>0) {
			long value = time - sentTime;			
			if (value>=0 && value<MAX_HIST_VALUE) {
				//only record the good values
				ElapsedTimeRecorder.record(histRoundTrip, value);
			}
		}
	}
		
	
	public boolean isBusy() {
		boolean ok = ((maxInFlightMask&inFlightRoutesSentPos) != (maxInFlightMask&inFlightRoutesRespPos)) &&
				     ((maxInFlightMask&inFlightRoutesSentPos) == (maxInFlightMask&(maxInFlight+inFlightRoutesRespPos)));
	
		long busyCounter = ClientCoordinator.busyCounter;
		if (ok) {
			if (Long.numberOfLeadingZeros(busyCounter) != Long.numberOfLeadingZeros(++busyCounter)) {
				logger.warn("client connection to {}:{} session {} has not received any responses, waiting for {} messages",host,port,sessionId,maxInFlight);
			}
			ClientCoordinator.busyCounter = busyCounter;
		}
		
		return ok;
	}
		
	public void recordDestinationRouteId(int id) {
	//	assert(-1 == inFlightRoutes[(1+inFlightRoutesSentPos) & maxInFlightMask]);
		inFlightRoutes[++inFlightRoutesSentPos & maxInFlightMask] = id;
	}
	
	public int consumeDestinationRouteId() {
		int idx = ++inFlightRoutesRespPos & maxInFlightMask;
		int value = inFlightRoutes[idx];
		inFlightRoutes[idx] = -1;
		return value;
	}
	
	public int readDestinationRouteId() {
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

	public int spaceReq(int maxVarLen) {
		return payloadSize*(1 + (recBufferSize / (maxVarLen+2)));
	}

	private int skPos = -1;
	
	@Override
	public void skPosition(int position) {
		skPos=position;
	}

	@Override
	public int skPosition() {
		return skPos;
	}
	
	private boolean clientClosedNotificationSent; //only done once per connection

	public boolean isClientClosedNotificationSent() {
		return clientClosedNotificationSent;
	}

	public void clientClosedNotificationSent() {
		clientClosedNotificationSent= true;
	}

	public long getTimeoutNS() {
		return timeoutNS;
	}

	
}
