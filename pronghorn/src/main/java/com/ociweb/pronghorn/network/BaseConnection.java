package com.ociweb.pronghorn.network;

import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.ConnectionStateSchema;
import com.ociweb.pronghorn.pipe.ChannelReader;
import com.ociweb.pronghorn.pipe.ChannelWriter;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;

public abstract class BaseConnection {
	
	static final Logger logger = LoggerFactory.getLogger(BaseConnection.class);

	private SSLEngine engine;
	private SocketChannel socketChannel;
	public final long id;//MUST be final and never change.
	protected boolean isValid = true;

	protected int localRunningBytesProduced;
	private long lastNetworkBeginWait = 0;
	private int sequenceNo;

	protected boolean isDisconnecting = false;

    private long lastUsedTimeNS = System.nanoTime();

	protected ConnDataWriterController connectionDataWriter;
	protected ConnDataReaderController connectionDataReader;	
	
	protected BaseConnection(SSLEngine engine, 
			                 SocketChannel socketChannel,
			                 long id
				) {
		
		this.engine = engine;
		this.socketChannel = socketChannel;
		this.id = id;
		
	}
	
	/**
	 * Only called upon release. 
	 * This helps GC faster and eliminates the loop caused by this Connection held by 
	 * the Selector.
	 */
	public void decompose() {
		//new Exception("decompose connection").printStackTrace();
		//socketChannel = null;
		//engine = null;
		//connectionDataWriter = null;
		//connectionDataReader = null;
	}
		
	public ConnDataWriterController connectionDataWriter() {		
		return connectionDataWriter;
	}
	
	public ConnDataReaderController connectionDataReader() {		
		return connectionDataReader;
	}
	
	public void setLastUsedTime(long timeNS) {
		lastUsedTimeNS = timeNS;
	}
	
	public long getLastUsedTime() {
		return lastUsedTimeNS;
	}
	
	public SSLEngine getEngine() {
			return engine;		
	}
	
	public String toString() {
		if (null==getEngine()) {
			return null==socketChannel? "Connection" : socketChannel.toString()+" id:"+id;
		} else {		
			return getEngine().getSession().toString()+" "
					+ ((null==socketChannel) ? "Connection" : (socketChannel.toString()+" id:"+id));
		}
	}
	
    //should only be closed by the socket writer logic or TLS handshake may be disrupted causing client to be untrusted.
	public boolean close() {
		//logger.info("closed connection {}",id);
		if (isValid) {
			isValid = false;
			try {
				 //this call to close will also de-register the selector key
				 socketChannel.close();
			} catch (Throwable e) {					
			}			
			return true;
		} 		
		return false;
	}

	public boolean isValid() {
		return isValid;
	}
	
	protected HandshakeStatus closeInboundCloseOutbound(SSLEngine engine) {
		
		try {
		    engine.closeInbound();
		} catch (SSLException e) {
		    boolean debug = false;
		    if (debug) {
		          	
		    		logger.trace("This engine was forced to close inbound, without having received the proper SSL/TLS close notification message from the peer, due to end of stream.", e);
		    
		    }
			
		}
		engine.closeOutbound();
		// After closeOutbound the engine will be set to WRAP state, in order to try to send a close message to the client.
		return engine.getHandshakeStatus();
	}

	
	public boolean isDisconnecting() {
		return isDisconnecting;
	}
	
	
	public long getId() {
		return this.id;
	}

	public SocketChannel getSocketChannel() {
		return socketChannel;
	}


	public void clearWaitingForNetwork() {
		lastNetworkBeginWait=0;
	}


	public long durationWaitingForNetwork() {
		long now = System.nanoTime();
		if (0 == lastNetworkBeginWait) {
			lastNetworkBeginWait=now;
			return 0;
		} else {
			return now - lastNetworkBeginWait;
		}
	}

	public void setSequenceNo(int seq) {
		if (seq<sequenceNo) {
			logger.info("FORCE EXIT value rolled back {}for chanel{} ",seq,id);
			System.exit(-1);
		}
		//	log.info("setSequenceNo {} for chanel {}",seq,id);
		sequenceNo = seq;
	}
	
	public int getSequenceNo() {
		//  log.info("getSequenceNo {} for chanel {}",sequenceNo,id);
		return sequenceNo;
	}

	private int poolReservation=-1;
	
	public void setPoolReservation(int value) {
		assert(-1==poolReservation);
		poolReservation = value;
		assert(value>=0);

	}
	
	public int getPoolReservation() {
		return poolReservation;
	}
	
	public void clearPoolReservation() {
		poolReservation = -1;
	}

	@Override
	protected void finalize() throws Throwable {
		close();
	}
	
	public class ConnDataWriterController {

		
		private final int SIZE_OF = Pipe.sizeOf(ConnectionStateSchema.instance,ConnectionStateSchema.MSG_STATE_1);
		protected final Pipe<ConnectionStateSchema> pipe;
		private boolean isWriting = false;		
		
		public ConnDataWriterController(Pipe<ConnectionStateSchema> pipe) {
			this.pipe = pipe;
		}
		
		/**
		 * check if connection data can take another message
		 * @return true if there is room for another write
		 */
		public boolean hasRoomForWrite() {
			return Pipe.hasRoomForWrite(pipe);
		}
		
		/**
		 * Open the message for writing
		 * @return returns the ChannelWriter or null if there is no room to write.
		 */
		public ChannelWriter beginWrite(int route, long arrivalTime) {
			if (Pipe.hasRoomForWrite(pipe)) {
				Pipe.markHead(pipe);
				Pipe.addMsgIdx(pipe, ConnectionStateSchema.MSG_STATE_1);
				Pipe.addIntValue(route, pipe);
				Pipe.addLongValue(arrivalTime, pipe);
				isWriting = true;
				return Pipe.openOutputStream(pipe);
			}
			return null;
		}
		
		/**
		 * Dispose of everything written and restore to the way it was before
		 * beginWrite() was called.
		 */
		public void abandonWrite() {
			if (isWriting) {
				DataOutputBlobWriter.closeLowLevelField(Pipe.outputStream(pipe));
				Pipe.resetHead(pipe);
			} else {
				isWriting=false;
			}
		}
		
		/**
		 * Store the message and move the pointers forward so the data can be
		 * consumed later.
		 */
		public void commitWrite(int context, long businessTime) {
			if (isWriting) {
				DataOutputBlobWriter.closeLowLevelField(Pipe.outputStream(pipe));		
				
				Pipe.confirmLowLevelWrite(pipe, SIZE_OF);
				Pipe.addIntValue(context, pipe);
				Pipe.addLongValue(businessTime, pipe);
				Pipe.publishWrites(pipe);
			} else {
				isWriting = false;
			}
		}

	}
	
	public class ConnDataReaderController {

		private final int SIZE_OF = Pipe.sizeOf(ConnectionStateSchema.instance,ConnectionStateSchema.MSG_STATE_1);
		
		protected final Pipe<ConnectionStateSchema> pipe;
		protected AtomicBoolean isReading = new AtomicBoolean();
				
	    private int activeRoute;
	    private long activeArrivalTime;
	    private int activeContext;
	    private long activeBusinessTime;
	    
		
		public ConnDataReaderController(Pipe<ConnectionStateSchema> pipe) {
			this.pipe = pipe;
		}
			
		/**
		 * checks if there is data on the channel
		 * @return true if there is a full message to read
		 */
		public boolean hasContentToRead() {
			return Pipe.hasContentToRead(pipe);
		}

		
		/**
		 * Opens the channel reader for reading from beginning of message
		 * @return ChannelReader opened for reading or null if channel has no data
		 */
		public ChannelReader beginRead() {
			if (Pipe.hasContentToRead(pipe)) {
				Pipe.markTail(pipe);
				int msg = Pipe.takeMsgIdx(pipe);
				
				if (msg >= 0) {
					isReading .set(true);
					
					activeRoute = Pipe.takeInt(pipe);
					activeArrivalTime = Pipe.takeLong(pipe);					
					DataInputBlobReader<ConnectionStateSchema> result = Pipe.openInputStream(pipe);
					activeContext = Pipe.takeInt(pipe);
					activeBusinessTime = Pipe.takeLong(pipe);
					return result;
				} else {
					//eof
				}
			}
			return null;
		}
		
		public int readRoute() {
			assert(isReading.get());
			return activeRoute;
		}
		
		public long readArrivalTime() {
			assert(isReading.get());
			return activeArrivalTime;
		}
		
		public int readContext() {
			assert(isReading.get());
			return activeContext;
		}
		
		public long readBusinessTime() {
			assert(isReading.get());
			return activeBusinessTime;
		}
		
		/**
		 * Restore position to the beginning, the ChannelReader is invalidated
		 * beginRead() must be called again for another read.
		 */
		public void rollback() {
			if (isReading.get()) {
				Pipe.resetTail(pipe);
			}
			isReading .set(false);
		}
		
		/**
		 * Move position forward.  ChanelReader is invalid and beginRead() must be called again.
		 */
		public void commitRead() {
			if (isReading.get()) {
				Pipe.confirmLowLevelRead(pipe, SIZE_OF);
				Pipe.releaseReadLock(pipe);
			}
			isReading.set(false);
		}

	}

	
}
