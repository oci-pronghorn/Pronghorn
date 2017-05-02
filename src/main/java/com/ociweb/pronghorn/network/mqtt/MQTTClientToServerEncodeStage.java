package com.ociweb.pronghorn.network.mqtt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.ClientConnection;
import com.ociweb.pronghorn.network.ClientCoordinator;
import com.ociweb.pronghorn.network.schema.MQTTClientToServerSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobLoadSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobStoreSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class MQTTClientToServerEncodeStage extends PronghornStage {

	private final Logger logger = LoggerFactory.getLogger(MQTTClientToServerEncodeStage.class);
	
	private final Pipe<MQTTClientToServerSchema> input;
	private final Pipe<PersistedBlobStoreSchema> persistBlobStore;
	private final Pipe<PersistedBlobLoadSchema> persistBlobLoad;
	
	private final Pipe<NetPayloadSchema>[] toBroker; //array is the size of supported "in-flight" messages.
			
	
	private final int uniqueConnectionId;
	private final ClientCoordinator ccm;
	
	private ClientConnection activeConnection;
	private byte[] hostBack;
	private int hostPos;
	private int hostLen;
	private int hostMask;
	private int hostPort;
	private int keepAliveMS;
	private int maxInFlight; 	
	
	private boolean isInReloadPersisted;
	private boolean isInPersistWrite;
	
		
	private long lastActvityTime;
	boolean isPersistantSession;
	
	//The File server module also copies data from the outgoing pipe however...
	//In the file server its used as a cache and used if it has not been written over
	//Here for the encoder we must ensure the data is not written over so it can be
	//re-sent until an ack is received.  The server must ack each in order and
	//the client must reply in order
	

	private int[]  packetIdRing;
	private long[] slabPositionsRing;
	private int[]  blobPositionsRing;
		
	private int ringTail;
	private int ringHead;
	private final int ringSize;
	private final int ringMask;
	
	public MQTTClientToServerEncodeStage(GraphManager gm, ClientCoordinator ccm, int maxInFlight, int uniqueId, 
			                             Pipe<MQTTClientToServerSchema> input,
			                             Pipe<PersistedBlobStoreSchema> persistBlobStore,
			                             Pipe<PersistedBlobLoadSchema> persistBlobLoad,
			                             Pipe<NetPayloadSchema>[] toBroker) {
		super(gm, join(input,persistBlobLoad), join(toBroker,persistBlobStore));
		this.input = input;
		
		this.persistBlobStore = persistBlobStore;
		this.persistBlobLoad = persistBlobLoad;
		
		this.toBroker = toBroker;
		assert(toBroker.length>0);
		
		this.maxInFlight = maxInFlight;
		
		int ringSizeBits = (int)Math.ceil(Math.log(maxInFlight+1)/Math.log(2));
		this.ringSize = 1 << ringSizeBits;
		this.ringMask = ringSize - 1;
		
		this.packetIdRing = new int[ringSize];
		this.slabPositionsRing = new long[ringSize];
		this.blobPositionsRing = new int[ringSize];
		
		this.ccm = ccm;
		
		this.uniqueConnectionId = uniqueId;
	}

	//////////////////	////////
	//////////////////////////
	
	public boolean roomForMoreInFlight(Pipe<NetPayloadSchema> pipe) {
	
		if (ringTail!=ringHead) {
			if (Pipe.getSlabHeadPosition(pipe) >=	(slabPositionsRing[ringMask & ringTail] + pipe.sizeOfSlabRing)) {
				return false;//no room becuase it will write over the old data waiting for ack.
			}
		}	
		
		return (ringHead-ringTail) < maxInFlight;
	}
	
	public boolean hasUnackPublished() {
		return ringTail!=ringHead;
	}
	
	public void clearAckPublishedCollection() {
		ringTail = ringHead = 0;
	}
	
	private void storePublishedPosPersisted(int blobPosition, int blobConsumed, byte[] blob, final int packetId) {
		
		PipeWriter.presumeWriteFragment(persistBlobStore, PersistedBlobStoreSchema.MSG_BLOCK_1);
		PipeWriter.writeLong(persistBlobStore,PersistedBlobStoreSchema.MSG_BLOCK_1_FIELD_BLOCKID_3, (long) packetId);
		PipeWriter.writeBytes(persistBlobStore,PersistedBlobStoreSchema.MSG_BLOCK_1_FIELD_BYTEARRAY_2, blob, blobPosition, blobConsumed);
		PipeWriter.publishWrites(persistBlobStore);
	}

	private void storePublishedPosLocal(long slabPosition, int blobPosition, final int packetId) {
		packetIdRing[ringMask & ringHead] = packetId;
		slabPositionsRing[ringMask & ringHead] = slabPosition;
		blobPositionsRing[ringMask & ringHead] = blobPosition;
		ringHead++;
	}
	
	public void ackPublishedPos(int packetId) {		
		if (isPersistantSession) {
		
			PipeWriter.presumeWriteFragment(persistBlobStore, PersistedBlobStoreSchema.MSG_RELEASE_7);		
			PipeWriter.writeLong(persistBlobStore,PersistedBlobStoreSchema.MSG_RELEASE_7_FIELD_BLOCKID_3, (long) packetId);
		    PipeWriter.publishWrites(persistBlobStore);
			 
		    //will clear locally upon ack

		} else {
			ackPublishedPosLocal(packetId);
		}
	}

	private void ackPublishedPosLocal(int packetId) {
		if ((packetIdRing[ringMask & ringTail] == packetId) && hasUnackPublished() ) {
			//this is the normal case since if everyone behaves these values will arrive in order
			ringTail++;			
			while (hasUnackPublished() && packetIdRing[ringMask & ringTail] == Integer.MAX_VALUE) {
				ringTail++; //skip over any values that showed up early.
			}
		} else {
			//this is the odd case
			int i = ringTail;
			int stop = ringMask&ringHead;
			while ((i&ringMask) != stop) {
				if (packetIdRing[ringMask & ringHead] == packetId) {			
					packetIdRing[ringMask & ringHead] = Integer.MAX_VALUE;//set as bad value to skip
					break;
				}
				i++;
			}
		}
	}
	
	public void rePublish(Pipe<NetPayloadSchema> pipe) {
		int i = ringTail;
		int stop = ringMask&ringHead;
		while ((i&ringMask) != stop) {
			
			//skip bad value already acknowledged
			if (packetIdRing[ringMask & ringHead] != Integer.MAX_VALUE) {	
				
				if (!PipeWriter.tryReplication(pipe, slabPositionsRing[ringMask & i], blobPositionsRing[ringMask & i])) {
					logger.warn("output pipe must be large enough for all in-flight publishes");					
					while (!PipeWriter.tryReplication(pipe, slabPositionsRing[ringMask & i], blobPositionsRing[ringMask & i])) {
						Thread.yield();
						if (Thread.interrupted() || Pipe.isShutdown(pipe)) {
							Thread.currentThread().interrupt();
							activeConnection = null;
							return;
						}
					}
				}
			}
			i++;
		}		
	}
	
	
	///////////////////////////////
	///////////////////////////////
	
	@Override
	public void startup() {
		hostBack = new byte[input.maxVarLen];
	}
	
	public long connectionId() {
		if (hostLen==0) {
			return -1;
		}
		if (null==activeConnection || !activeConnection.isValid() ) {
			
			activeConnection = ClientCoordinator.openConnection(ccm, 
					                         hostBack, hostPos, hostLen, hostMask, hostPort, 
					                         uniqueConnectionId, 
					                         toBroker, -1);
			
			if (null!=activeConnection) {				
				//When a Client reconnects with CleanSession set to 0, both the Client and Server MUST re-send any 
				//unacknowledged PUBLISH Packets (where QoS > 0) and PUBREL Packets using their original Packet Identifiers [MQTT-4.4.0-1].
				//This is the only circumstance where a Client or Server is REQUIRED to re-deliver messages.
				rePublish(toBroker[activeConnection.requestPipeLineIdx()]);								
			}
		}
		
		return null!=activeConnection ? activeConnection.id : -1;
	}
	
	public static void encodeVarLength(DataOutputBlobWriter<NetPayloadSchema> output, int x) {	
		
	    //little endian
		//high bit is on until the end
		
		byte encodedByte = (byte)(x & 0x7F);
		x = x >> 7;
		while (x>0) {
			output.writeByte(0x80 | encodedByte);			
			encodedByte = (byte)(x & 0x7F);
			 x = x >> 7;
		}
		output.writeByte(0xFF & encodedByte);	
	}
	
	private static void appendFixedProtoName(DataOutputBlobWriter<NetPayloadSchema> output) {
		//NOTE: this is hardcoded from 3.1.1 spec and may not be compatible with 3.1
		output.writeByte(0); //MSB
		output.writeByte(4); //LSB
		output.writeByte('M');
		output.writeByte('Q');
		output.writeByte('T');
		output.writeByte('T');		
	}
	
	@Override
	public void run() {		
		if (!processPersistLoad()) {
			processInput(processPingAndReplay());
		}
	}

	private boolean processPersistLoad() {
		long connectionId = -1;
		
		while ( (connectionId = connectionId())>=0
				&& PipeReader.tryReadFragment(persistBlobLoad)) {
		    int msgIdx = PipeReader.getMsgIdx(persistBlobLoad);
		    switch(msgIdx) {
		    	case PersistedBlobLoadSchema.MSG_BEGINREPLAY_8:
		    		clearAckPublishedCollection();
		    		isInReloadPersisted = true;
		    	break;
		        case PersistedBlobLoadSchema.MSG_BLOCK_1:
		        {
		        	int fieldBlockId = (int)PipeReader.readLong(persistBlobLoad,PersistedBlobLoadSchema.MSG_BLOCK_1_FIELD_BLOCKID_3);
		            
		        	Pipe<NetPayloadSchema> server = toBroker[activeConnection.requestPipeLineIdx()];
		        	
		    		long slabPos = Pipe.getSlabHeadPosition(server);
		    		final int blobPos = Pipe.getBlobHeadPosition(server);
		        	
		    		boolean ok = PipeWriter.tryWriteFragment(server, NetPayloadSchema.MSG_PLAIN_210);
		    		assert(ok) : "checked above and should not happen";		    				    		
		    		DataOutputBlobWriter<NetPayloadSchema> output = PipeWriter.outputStream(server);
		    		DataOutputBlobWriter.openField(output);
		    		
		    		PipeReader.readBytes(persistBlobLoad,
		    				             PersistedBlobLoadSchema.MSG_BLOCK_1_FIELD_BYTEARRAY_2,
		    				             output);
					finishEndOfBrokerMessage(connectionId, server, System.currentTimeMillis(), output);					
					//NOTE: no need to re-store persistently since we are loading..
					storePublishedPosLocal(slabPos, blobPos, fieldBlockId);
					PipeWriter.publishWrites(server);					
		        }
		        break;
		        case PersistedBlobLoadSchema.MSG_FINISHREPLAY_9:
		        	isInReloadPersisted = false;
				break;
				
		        case PersistedBlobLoadSchema.MSG_ACKRELEASE_10:
		        	int fieldBlockId1 = (int)PipeReader.readLong(persistBlobLoad,PersistedBlobLoadSchema.MSG_ACKRELEASE_10_FIELD_BLOCKID_3);
		        	ackPublishedPosLocal(fieldBlockId1);
		        break;
		        
		        case PersistedBlobLoadSchema.MSG_ACKWRITE_11:
		        	PipeWriter.publishWrites(toBroker[activeConnection.requestPipeLineIdx()]);
		        	PipeReader.releaseReadLock(input);
		        	isInPersistWrite = false;//can now continue with next write
		        			        	
		        	//long comittedBlockId = PipeReader.readLong(persistBlobLoad,PersistedBlobLoadSchema.MSG_ACKWRITE_11_FIELD_BLOCKID_3);	        	
		        	//NOTE: if desired we could send this id back to MQTTClient, but I see no need at this time.
		        			        	
		        break;
		        
		        case -1:
		           requestShutdown();
		        break;
		    }
		    PipeReader.releaseReadLock(persistBlobLoad);
		}
		return isInReloadPersisted || isInPersistWrite;
	}

	private long processPingAndReplay() {
		long now = System.currentTimeMillis();
		long quiet = now-lastActvityTime;
		long connectionId = 0;		 
		
		if ((connectionId = connectionId())>=0 ) {
			Pipe<NetPayloadSchema> server = toBroker[activeConnection.requestPipeLineIdx()];
			if (quiet > (keepAliveMS>>1)) {
				//long gap so send unpublished or ping
				if (hasUnackPublished()) {
					rePublish(server);					
					lastActvityTime = now;
				} else {
					requestPing(now, connectionId, server);					
					lastActvityTime = now;
				}				
			} else {
				//not a long quiet but we may need to re-send publish
				if (quiet > 2000) { //2 seconds
					if (hasUnackPublished()) {
						rePublish(server);	
						lastActvityTime = System.currentTimeMillis();
					}
				}
			}
		}
		return connectionId;
	}

	private void processInput(long connectionId) {

		while ( (PipeReader.peekMsg(input, MQTTClientToServerSchema.MSG_BROKERHOST_100) 
				|| PipeReader.peekMsg(input, MQTTClientToServerSchema.MSG_STOPREPUBLISH_99) 				
				|| (((connectionId = connectionId())>=0)
				   && roomForMoreInFlight(toBroker[activeConnection.requestPipeLineIdx()])
				   && PipeWriter.hasRoomForWrite(persistBlobStore)
				   && PipeWriter.hasRoomForWrite(toBroker[activeConnection.requestPipeLineIdx()]) 	)
				)
				&& PipeReader.tryReadFragment(input)) {

			//NOTE: warning, if this gets disconnected it may pick a new pipe and the old data may be abandoned?
			
			int msgIdx = PipeReader.getMsgIdx(input);

			if (MQTTClientToServerSchema.MSG_BROKERHOST_100 == msgIdx) {
				
				this.hostLen = PipeReader.readBytesLength(input, MQTTClientToServerSchema.MSG_BROKERHOST_100_FIELD_HOST_26);
				this.hostPos = 0;
				this.hostMask = Integer.MAX_VALUE;
				PipeReader.readBytes(input, MQTTClientToServerSchema.MSG_BROKERHOST_100_FIELD_HOST_26, hostBack, 0);
				this.hostPort = PipeReader.readInt(input, MQTTClientToServerSchema.MSG_BROKERHOST_100_FIELD_PORT_27);
				//must establish new connection
				ccm.releaseResponsePipeLineIdx(connectionId);
				if (null!=activeConnection) {
					activeConnection.close();
				}
				PipeReader.releaseReadLock(input);
				
				continue;
			} else if (MQTTClientToServerSchema.MSG_STOPREPUBLISH_99 == msgIdx){
				
				ackPublishedPos(PipeReader.readInt(input, MQTTClientToServerSchema.MSG_STOPREPUBLISH_99_FIELD_PACKETID_20));				
				PipeReader.releaseReadLock(input);
				
				continue;
			}			
						
			if (writeToBroker(connectionId, toBroker[activeConnection.requestPipeLineIdx()], msgIdx)) {
				PipeReader.releaseReadLock(input);
			} else {
				isInPersistWrite = true;
				return;//must wait until this is completed
			}
			
		}
	}

	private boolean writeToBroker(long connectionId, Pipe<NetPayloadSchema> server, int msgIdx) {

		long arrivalTime = 0;
		
		//must capture these values now in case we are doing a publish of QOS 1 or 2
		long slabPos = Pipe.getSlabHeadPosition(server);
		int blobPos = Pipe.getBlobHeadPosition(server);
		
		
		//////
		
		boolean ok = PipeWriter.tryWriteFragment(server, NetPayloadSchema.MSG_PLAIN_210);
		assert(ok) : "checked above and should not happen";
		
		
		DataOutputBlobWriter<NetPayloadSchema> output = PipeWriter.outputStream(server);
		DataOutputBlobWriter.openField(output);

		lastActvityTime = System.currentTimeMillis();
		
		switch (msgIdx) {
				case MQTTClientToServerSchema.MSG_CONNECT_1:
					
					arrivalTime = PipeReader.readLong(input, MQTTClientToServerSchema.MSG_CONNECT_1_FIELD_TIME_37);											
					int conFlags = PipeReader.readInt(input, MQTTClientToServerSchema.MSG_CONNECT_1_FIELD_FLAGS_29);					
					int clientIdLen = PipeReader.readBytesLength(input, MQTTClientToServerSchema.MSG_CONNECT_1_FIELD_CLIENTID_30);

					int willTopicLen =  PipeReader.readBytesLength(input, MQTTClientToServerSchema.MSG_CONNECT_1_FIELD_WILLTOPIC_31);
					int willMessageLen =  PipeReader.readBytesLength(input, MQTTClientToServerSchema.MSG_CONNECT_1_FIELD_WILLPAYLOAD_32);					
					
					int userLen =  PipeReader.readBytesLength(input, MQTTClientToServerSchema.MSG_CONNECT_1_FIELD_USER_33);						
					int passLen =  PipeReader.readBytesLength(input, MQTTClientToServerSchema.MSG_CONNECT_1_FIELD_PASS_34);
					
					int keepAliveSec = PipeReader.readInt(input, MQTTClientToServerSchema.MSG_CONNECT_1_FIELD_KEEPALIVESEC_28);
					keepAliveMS = keepAliveSec*1000;

					isPersistantSession = (conFlags&MQTTEncoder.CONNECT_FLAG_CLEAN_SESSION_1) == 0; 
					if (!isPersistantSession) {
						//if this is a new clean session then we must clear out our repeats
						clearAckPublishedCollection();
						clearPersistedCollection();
					} else {						
						requestReplayOfPersistedCollection();
					}
					
					int length = computeConectionOpenLength(conFlags, clientIdLen, willTopicLen, willMessageLen, userLen, passLen);
					output.writeByte((0xFF&0x10));
					encodeVarLength(output, length); //const and remaining length, 2  bytes
					
					//variable header
					appendFixedProtoName(output); //const 6 bytes
					output.writeByte(4); //const 1 byte for version		
					output.writeByte(conFlags); //8 bits or togehter, if clientId zero length must set clear
					output.writeShort(keepAliveSec); //seconds < 16 bits
											
					//payload
		        	output.writeShort(clientIdLen); 
			        PipeReader.readBytes(input, MQTTClientToServerSchema.MSG_CONNECT_1_FIELD_CLIENTID_30, output);
											
					if (0!=(MQTTEncoder.CONNECT_FLAG_WILL_FLAG_2&conFlags)) {
													
						output.writeShort(willTopicLen);
						PipeReader.readBytes(input, MQTTClientToServerSchema.MSG_CONNECT_1_FIELD_WILLTOPIC_31, output);
						output.writeShort(willMessageLen);
						PipeReader.readBytes(input, MQTTClientToServerSchema.MSG_CONNECT_1_FIELD_WILLPAYLOAD_32, output);
						
					}
					
					if (0!=(MQTTEncoder.CONNECT_FLAG_USERNAME_7&conFlags)) {						
						output.writeShort(userLen);
						PipeReader.readBytes(input, MQTTClientToServerSchema.MSG_CONNECT_1_FIELD_USER_33, output);
					}
					
					if (0!=(MQTTEncoder.CONNECT_FLAG_PASSWORD_6&conFlags)) {								
						output.writeShort(passLen);
						PipeReader.readBytes(input, MQTTClientToServerSchema.MSG_CONNECT_1_FIELD_PASS_34, output);
					}
					finishEndOfBrokerMessage(connectionId, server, arrivalTime, output);							
					PipeWriter.publishWrites(server);					
				break;
				case MQTTClientToServerSchema.MSG_DISCONNECT_14:
					
					arrivalTime = PipeReader.readLong(input, MQTTClientToServerSchema.MSG_DISCONNECT_14_FIELD_TIME_37);
					
					output.writeByte(0xE0);
					output.writeByte(0x00);
					finishEndOfBrokerMessage(connectionId, server, arrivalTime, output);					
					PipeWriter.publishWrites(server);
				break;					
				case MQTTClientToServerSchema.MSG_PUBACK_4:
					
					arrivalTime = PipeReader.readLong(input, MQTTClientToServerSchema.MSG_PUBACK_4_FIELD_TIME_37);
					
			        //0x40  type/reserved   0100 0000
			        //0x02  remaining length
			        //MSB PacketID high
			        //LSB PacketID low
					output.writeByte(0x40);
					output.writeByte(0x02);
					
					int serverPacketId4 = PipeReader.readInt(input, MQTTClientToServerSchema.MSG_PUBACK_4_FIELD_PACKETID_20);
					output.writeShort(0xFFFF & serverPacketId4);
					finishEndOfBrokerMessage(connectionId, server, arrivalTime, output);
					PipeWriter.publishWrites(server);
				break;					
				case MQTTClientToServerSchema.MSG_PUBCOMP_7:
					
					arrivalTime = PipeReader.readLong(input, MQTTClientToServerSchema.MSG_PUBCOMP_7_FIELD_TIME_37);
					
			        //0x70  type/reserved   0111 0000
			        //0x02  remaining length
			        //MSB PacketID high
			        //LSB PacketID low
					output.writeByte(0x70);
					output.writeByte(0x02);
					
					int serverPacketId =  PipeReader.readInt(input, MQTTClientToServerSchema.MSG_PUBCOMP_7_FIELD_PACKETID_20);
					output.writeShort(0xFFFF & serverPacketId);
					finishEndOfBrokerMessage(connectionId, server, arrivalTime, output);
					
					PipeWriter.publishWrites(server);						
					///////////////
					//release the pubrec
					//note this packetId is from the server side.
					//////////////					
					ackPublishedPos(serverPacketId);					
				break;					
				case MQTTClientToServerSchema.MSG_PUBLISH_3:
					
					arrivalTime  = PipeReader.readLong(input, MQTTClientToServerSchema.MSG_PUBLISH_3_FIELD_TIME_37);
					int qos      = PipeReader.readInt(input, MQTTClientToServerSchema.MSG_PUBLISH_3_FIELD_QOS_21);
					int packetId = PipeReader.readInt(input, MQTTClientToServerSchema.MSG_PUBLISH_3_FIELD_PACKETID_20);
			
					buildPublishMessage(output, qos, packetId);
					finishEndOfBrokerMessage(connectionId, server, arrivalTime, output);


					if (qos != 0) {
						storePublishedPosLocal(slabPos, blobPos, packetId);
						//hold for re-send until we get an ack for this packetId.
						if (isPersistantSession) {	
							int consumedBytes = Pipe.computeCountOfBytesConsumed(server);
							storePublishedPosPersisted(blobPos, consumedBytes, Pipe.blob(server), packetId);
							return false;
						}
					}
					PipeWriter.publishWrites(server);
					
				break;					
				case MQTTClientToServerSchema.MSG_PUBREC_5:	
					
					arrivalTime = PipeReader.readLong(input, MQTTClientToServerSchema.MSG_PUBREC_5_FIELD_TIME_37);
					
			        //0x50  type/reserved   0101 0000
			        //0x02  remaining length
			        //MSB PacketID high
			        //LSB PacketID low
					output.writeByte(0x50);
					output.writeByte(0x02);
					int packetId5 = PipeReader.readInt(input, MQTTClientToServerSchema.MSG_PUBREC_5_FIELD_PACKETID_20);
					output.writeShort(0xFFFF & packetId5);
					finishEndOfBrokerMessage(connectionId, server, arrivalTime, output);
					
					storePublishedPosLocal(slabPos, blobPos, packetId5); //warning this packetId came from the server..
					if (isPersistantSession) {	
						int consumedBytes5 = Pipe.computeCountOfBytesConsumed(server);
						storePublishedPosPersisted(blobPos, consumedBytes5, Pipe.blob(server), packetId5);	
						return false;
					} else {
						PipeWriter.publishWrites(server);
					}
				break;
				case MQTTClientToServerSchema.MSG_PUBREL_6:	
					
					arrivalTime = PipeReader.readLong(input, MQTTClientToServerSchema.MSG_PUBREL_6_FIELD_TIME_37);
					
			        //0x62  type/reserved   0110 0010
			        //0x02  remaining length
			        //MSB PacketID high
			        //LSB PacketID low
					output.writeByte(0x62);
					output.writeByte(0x02);
					
					int packetId6 = PipeReader.readInt(input, MQTTClientToServerSchema.MSG_PUBREL_6_FIELD_PACKETID_20);
					output.writeShort(0xFFFF & packetId6);

					//clear the message we were sending
					ackPublishedPos(packetId6);
					finishEndOfBrokerMessage(connectionId, server, arrivalTime, output);
					storePublishedPosLocal(slabPos, blobPos, packetId6);
					//hold this publrel re-send until we get an ack for this packetId.
					if (isPersistantSession) {	
						int consumedBytes6 = Pipe.computeCountOfBytesConsumed(server);
						storePublishedPosPersisted(blobPos, consumedBytes6, Pipe.blob(server), packetId6);	
						return false;
					} else {
						PipeWriter.publishWrites(server);
					}
				break;
				case MQTTClientToServerSchema.MSG_SUBSCRIBE_8:
					
					arrivalTime = PipeReader.readLong(input, MQTTClientToServerSchema.MSG_SUBSCRIBE_8_FIELD_TIME_37);
					
			        int topicIdLen = PipeReader.readBytesLength(input, MQTTClientToServerSchema.MSG_SUBSCRIBE_8_FIELD_TOPIC_23);			
			        output.writeByte((0xFF&0x82));
					encodeVarLength(output, 2 + topicIdLen + 2 + 1); //const and remaining length, 2  bytes
									
					int packetId8 = PipeReader.readInt(input, MQTTClientToServerSchema. MSG_SUBSCRIBE_8_FIELD_PACKETID_20);
					output.writeShort(packetId8);						
					//variable header
					output.writeShort(topicIdLen);
					
					PipeReader.readBytes(input, MQTTClientToServerSchema.MSG_SUBSCRIBE_8_FIELD_TOPIC_23, output);
					
					output.writeByte(PipeReader.readInt(input, MQTTClientToServerSchema.MSG_SUBSCRIBE_8_FIELD_QOS_21));
					finishEndOfBrokerMessage(connectionId, server, arrivalTime, output);		
					storePublishedPosLocal(slabPos, blobPos, packetId8);
					//hold this until we have our subscription ack
					if (isPersistantSession) {	
						int consumedBytes8 = Pipe.computeCountOfBytesConsumed(server);
						storePublishedPosPersisted(blobPos, consumedBytes8, Pipe.blob(server), packetId8);	
						return false;
					} else {
						PipeWriter.publishWrites(server);
					}
				break;
				case MQTTClientToServerSchema.MSG_UNSUBSCRIBE_10:
									
					arrivalTime = PipeReader.readLong(input, MQTTClientToServerSchema.MSG_UNSUBSCRIBE_10_FIELD_TIME_37);
					
					int topicIdLen10 = PipeReader.readBytesLength(input, MQTTClientToServerSchema.MSG_UNSUBSCRIBE_10_FIELD_TOPIC_23);
			        output.writeByte((0xFF&0x82));
					encodeVarLength(output, 2 + topicIdLen10 + 2); //const and remaining length, 2  bytes
									
					int packetId10 = PipeReader.readInt(input, MQTTClientToServerSchema.MSG_UNSUBSCRIBE_10_FIELD_PACKETID_20);
					output.writeShort(packetId10);
					
					//variable header
					output.writeShort(topicIdLen10);
					
					PipeReader.readBytes(input, MQTTClientToServerSchema.MSG_UNSUBSCRIBE_10_FIELD_TOPIC_23, output);
					finishEndOfBrokerMessage(connectionId, server, arrivalTime, output);
					//hold this until we have our un-subscription ack
					
					storePublishedPosLocal(slabPos, blobPos, packetId10);
					if (isPersistantSession) {	
						int consumedBytes10 = Pipe.computeCountOfBytesConsumed(server);
						storePublishedPosPersisted(blobPos, consumedBytes10, Pipe.blob(server), packetId10);	
						return false;
					} else {
						PipeWriter.publishWrites(server);
					}
				break;
		}	
		return true;
	}

	private void requestReplayOfPersistedCollection() {
		PipeWriter.presumeWriteFragment(persistBlobStore, PersistedBlobStoreSchema.MSG_REQUESTREPLAY_6);
		PipeWriter.publishWrites(persistBlobStore);
	}

	private void clearPersistedCollection() {
		PipeWriter.presumeWriteFragment(persistBlobStore, PersistedBlobStoreSchema.MSG_CLEAR_12);
		PipeWriter.publishWrites(persistBlobStore);
	}

	private void finishEndOfBrokerMessage(long connectionId, Pipe<NetPayloadSchema> server, long arrivalTime,
			DataOutputBlobWriter<NetPayloadSchema> output) {
		DataOutputBlobWriter.closeHighLevelField(output, NetPayloadSchema.MSG_PLAIN_210_FIELD_PAYLOAD_204);
		
		PipeWriter.writeLong(server, NetPayloadSchema.MSG_PLAIN_210_FIELD_CONNECTIONID_201, connectionId);
		PipeWriter.writeLong(server, NetPayloadSchema.MSG_PLAIN_210_FIELD_ARRIVALTIME_210, arrivalTime);
		PipeWriter.writeLong(server, NetPayloadSchema.MSG_PLAIN_210_FIELD_POSITION_206, 0); //always use zero for client requests
	}

	private void requestPing(long now, long connectionId, Pipe<NetPayloadSchema> server) {
		PipeWriter.presumeWriteFragment(server, NetPayloadSchema.MSG_PLAIN_210);
		DataOutputBlobWriter<NetPayloadSchema> output = PipeWriter.outputStream(server);
		DataOutputBlobWriter.openField(output);
		output.writeByte(0xC0);
		output.writeByte(0x00);		
		DataOutputBlobWriter.closeHighLevelField(output, NetPayloadSchema.MSG_PLAIN_210_FIELD_PAYLOAD_204);
		
		PipeWriter.writeLong(server, NetPayloadSchema.MSG_PLAIN_210_FIELD_CONNECTIONID_201, connectionId);
		PipeWriter.writeLong(server, NetPayloadSchema.MSG_PLAIN_210_FIELD_POSITION_206, 0); //always use zero for client requests
		PipeWriter.writeLong(server, NetPayloadSchema.MSG_PLAIN_210_FIELD_ARRIVALTIME_210, now);
		PipeWriter.publishWrites(server);
	}

	public void buildPublishMessage(DataOutputBlobWriter<NetPayloadSchema> output, int qos, int packetId) {
		int retain = PipeReader.readInt(input, MQTTClientToServerSchema.MSG_PUBLISH_3_FIELD_RETAIN_22);

		int topicLength = PipeReader.readBytesLength(input, MQTTClientToServerSchema.MSG_PUBLISH_3_FIELD_TOPIC_23);
		int payloadLength = PipeReader.readBytesLength(input, MQTTClientToServerSchema.MSG_PUBLISH_3_FIELD_PAYLOAD_25);
					
		final int pubHead = 0x30 | (0x6&(qos<<1)) | 1&retain; //bit 3 dup is zero which is modified later
		output.writeByte((0xFF&pubHead));
		encodeVarLength(output, topicLength + 2 + payloadLength + (packetId>=0 ? 2 : 0)); //const and remaining length, 2  bytes

		//variable header
		output.writeShort(topicLength);
		
		PipeReader.readBytes(input, MQTTClientToServerSchema.MSG_PUBLISH_3_FIELD_TOPIC_23, output);
		
		if (packetId>=0) {
			output.writeShort(packetId);
		}						
		
		//payload - note it does not record the length first, its just the remaining space
		PipeReader.readBytes(input, MQTTClientToServerSchema.MSG_PUBLISH_3_FIELD_PAYLOAD_25, output);
	}

	public int computeConectionOpenLength(int conFlags, int clientIdLen, int willTopicLen, int willMessageLen,
			int userLen, int passLen) {
		//The Remaining Length is the number of bytes remaining within the current packet, including data in the
		//variable header and the payload. The Remaining Length does not include the bytes used to encode the
		//Remaining Length.
		int length = 6+1+1+2;//fixed portion from protoName level flags and keep alive
		
		int length2 = clientIdLen;
		length += (2+length2);//encoded clientId
								
		
		if (0!=(MQTTEncoder.CONNECT_FLAG_WILL_FLAG_2&conFlags)) {
			if (willTopicLen>0) {
				length += (2+willTopicLen);
			}
			
			if (willMessageLen>=0) {
				length += (2+willMessageLen);
			}
		}
		
		if (0!=(MQTTEncoder.CONNECT_FLAG_USERNAME_7&conFlags) && userLen>0) {
			length += (2+userLen);
		}
		
		if (0!=(MQTTEncoder.CONNECT_FLAG_PASSWORD_6&conFlags) && passLen>0) {
			length += (2+passLen);
		}
		assert(length > 0) : "Code error above this point, length must always be positive";
		assert(length < (1<<28)) : "Error length is too large, "+length;

		return length;
	}

}
