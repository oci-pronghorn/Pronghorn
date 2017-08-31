package com.ociweb.pronghorn.network.mqtt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.ClientConnection;
import com.ociweb.pronghorn.network.ClientCoordinator;
import com.ociweb.pronghorn.network.schema.MQTTClientToServerSchema;
import com.ociweb.pronghorn.network.schema.MQTTClientToServerSchemaAck;
import com.ociweb.pronghorn.network.schema.MQTTIdRangeControllerSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.MessageSchema;
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
	private final Pipe<MQTTClientToServerSchemaAck> inputAck;
	
	private final Pipe<PersistedBlobStoreSchema> persistBlobStore;
	private final Pipe<PersistedBlobLoadSchema> persistBlobLoad;
	
	private final Pipe<NetPayloadSchema>[] toBroker; //array is the size of supported "in-flight" messages.
			
	private final Pipe<MQTTIdRangeControllerSchema> idRangeControl;
	
	
	private final int uniqueConnectionId;
	private final ClientCoordinator ccm;
	private boolean brokerAcknowledgedConnection;
	
	private ClientConnection activeConnection;
	private byte[] hostBack;
	private int hostPos;
	private int hostLen;
	private int hostMask;
	private int hostPort;
	private int keepAliveMS;	
	
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
	
    int remainingInFlight;
	
	public MQTTClientToServerEncodeStage(GraphManager gm, ClientCoordinator ccm, int maxInFlight, int uniqueId, 
			                             Pipe<MQTTClientToServerSchema> input,
			                             Pipe<MQTTClientToServerSchemaAck> inputAck,
			                             Pipe<PersistedBlobStoreSchema> persistBlobStore,
			                             Pipe<PersistedBlobLoadSchema> persistBlobLoad,
			                             Pipe<MQTTIdRangeControllerSchema> idRangeControl,
			                             Pipe<NetPayloadSchema>[] toBroker) {
		super(gm, join(input,inputAck,persistBlobLoad), join(toBroker,persistBlobStore,idRangeControl));
		this.input = input;
		this.inputAck = inputAck;
		this.idRangeControl = idRangeControl;
		this.persistBlobStore = persistBlobStore;
		this.persistBlobLoad = persistBlobLoad;
		
		this.toBroker = toBroker;
		assert(ofSchema(toBroker, NetPayloadSchema.instance));
		assert(toBroker.length>0);
		
		this.remainingInFlight = maxInFlight;
		
		int ringSizeBits = (int)Math.ceil(Math.log(maxInFlight)/Math.log(2));
		this.ringSize = 1 << ringSizeBits;
		this.ringMask = ringSize - 1;
		assert(ringSize >= maxInFlight);
		
		this.packetIdRing = new int[ringSize];
		this.slabPositionsRing = new long[ringSize];
		this.blobPositionsRing = new int[ringSize];
	
		this.ccm = ccm;
				
		this.uniqueConnectionId = uniqueId;
	}

	private boolean ofSchema(Pipe<NetPayloadSchema>[] toBroker, MessageSchema instance) {
		int i = toBroker.length;
		while (--i>=0) {
			if (!Pipe.isForSchema(toBroker[i], instance)) {
				logger.info("expected {} but found {}",instance.getClass(), Pipe.schemaName(toBroker[i]));				
				return false;
			}
			
		}
		
		
		return true;
		
	}

	@Override
	public void run() {		
		if (!processPersistLoad()) {

			long connectionId = processPingAndReplay();
			
			processInputAcks(connectionId);
									
			processInput(connectionId);
			
		}
	}
	
	//////////////////	////////
	//////////////////////////
	
	public boolean hasUnackPublished() {
		return ringTail!=ringHead;
	}
	
	public void clearAckPublishedCollection() {
		ringTail = ringHead = 0;
	}
	
	private void storePublishedPosPersisted(int blobPosition, int blobConsumed, byte[] blob, final int packetId) {
		//logger.trace("AAAA  store disk need ack for {} ",packetId);
		
		PipeWriter.presumeWriteFragment(persistBlobStore, PersistedBlobStoreSchema.MSG_BLOCK_1);
		PipeWriter.writeLong(persistBlobStore,PersistedBlobStoreSchema.MSG_BLOCK_1_FIELD_BLOCKID_3, (long) packetId);
		PipeWriter.writeBytes(persistBlobStore,PersistedBlobStoreSchema.MSG_BLOCK_1_FIELD_BYTEARRAY_2, blob, blobPosition, blobConsumed);
		PipeWriter.publishWrites(persistBlobStore);
	}

	private void storePublishedPosLocal(long slabPosition, int blobPosition, final int packetId) {
		//logger.trace("AAAA  store local need ack for {} ",packetId);

		packetIdRing[ringMask & ringHead] = packetId;
		slabPositionsRing[ringMask & ringHead] = slabPosition;
		blobPositionsRing[ringMask & ringHead] = blobPosition;
		ringHead++;
	}
	
	public void ackPublishedPos(int packetId) {	
		
		
		if (isPersistantSession) {
			//logger.trace("BBB must clear top level ack from drive {} ",packetId);
			
			PipeWriter.presumeWriteFragment(persistBlobStore, PersistedBlobStoreSchema.MSG_RELEASE_7);		
			PipeWriter.writeLong(persistBlobStore,PersistedBlobStoreSchema.MSG_RELEASE_7_FIELD_BLOCKID_3, (long) packetId);
		    PipeWriter.publishWrites(persistBlobStore);
			 
		    //will clear locally upon ack

		} else {
			
			//logger.trace("BBB must clear top level ack form memory {} ",packetId);
			
			ackPublishedPosLocal(packetId);
		}
	}

	private void ackPublishedPosLocal(int packetId) {
		if ((packetIdRing[ringMask & ringTail] == packetId) && hasUnackPublished() ) {
			//logger.info("got the expected next packetId {} ",packetId);
			//this is the normal case since if everyone behaves these values will arrive in order
			ringTail++;			
			while (hasUnackPublished() && packetIdRing[ringMask & ringTail] == Integer.MAX_VALUE) {
				ringTail++; //skip over any values that showed up early.
			}
		} else {
			//logger.info("got out of ourder tail id of  {} ",packetId);
			//Normal case if there is a poor network and packets get dropped.
			int i = ringTail;
			int stop = ringMask&ringHead;
			while ((i&ringMask) != stop) {
				if (packetIdRing[ringMask & i] == packetId) {			
					packetIdRing[ringMask & i] = Integer.MAX_VALUE;//set as bad value to skip
					//logger.trace("found an cleared old packetId {}",packetId);
					break;
				}
				i++;
			}
			//move tail up as needed
			while (hasUnackPublished() && packetIdRing[ringMask & ringTail] == Integer.MAX_VALUE) {
				ringTail++; //skip over any values that showed up early.
			}
		}
		//System.err.println("restore one");
		remainingInFlight++;
	}
	
	private final boolean rePublish(Pipe<NetPayloadSchema> pipe) {

		int stop = ringMask&ringHead;
		for(int i = (ringTail&ringMask); (i&ringMask)!=stop; i++ ) {

			//skip bad value already acknowledged
			if (packetIdRing[ringMask & i] != Integer.MAX_VALUE) {	
				
				//logger.info("republish to broker packet {} ",packetIdRing[ringMask & i]);
				
	    		final long slabPos = Pipe.getSlabHeadPosition(pipe); 
	    		final int blobPos = Pipe.getBlobHeadPosition(pipe);
					    			    	
	    		//////////////////////////////////////////////////////////////////
	    		int firstPos = pipe.blobMask & blobPositionsRing[ringMask & i];
				byte[] blob = Pipe.blob(pipe);
				int firstByte = blob[firstPos];
	    		if ((0x30&firstByte)==0x30) {//publish flag	    				
	    			//Set the DUP flag which is the 3rd bit now that we are sending this again.
	    			blob[firstPos] = (byte)(firstByte | 8); //
	    		}
	    		/////////////////////////////////////////////////////////////////
	    		
				if (!PipeWriter.tryReplication(pipe, slabPositionsRing[ringMask & i], blobPositionsRing[ringMask & i])) {
					return false;
				}
				lastActvityTime = System.currentTimeMillis(); //no need to ping if we keep reSending these.	
				
				//logger.info("re-sent message with id {}  {}",packetIdRing[ringMask & i],System.currentTimeMillis());
				
				storePublishedPosLocal(slabPos, blobPos, packetIdRing[ringMask & i]);
				packetIdRing[ringMask & i] = Integer.MAX_VALUE;//clear this one since we have added a new one.

				ringTail = i+1;//move up ring tail since everything up till here is done
				
				PipeWriter.publishWrites(pipe);
							
			}
		}	
		return true;
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
		
		if (null==activeConnection || ((!activeConnection.isValid()) && activeConnection.isFinishConnect() ) ) {
			//only do reOpen if the previous one is finished connecting and its now invalid.
			reOpenConnection();
		}
		
		long result = (null!=activeConnection) ? activeConnection.id : -1;
		return result;
		
	}

	private void reOpenConnection() {
		//logger.info("opening connection to broker {}:{} ",
		//		     Appendables.appendUTF8(new StringBuilder(), hostBack, hostPos, hostLen, hostMask), hostPort);

		activeConnection = ClientCoordinator.openConnection(ccm, 
				                         hostBack, hostPos, hostLen, hostMask, hostPort, 
				                         uniqueConnectionId, 
				                         toBroker,
				                         ccm.lookup(hostBack,hostPos,hostLen,hostMask, hostPort, uniqueConnectionId)); 

		if (null!=activeConnection) {		
			//When a Client reconnects with CleanSession set to 0, both the Client and Server MUST re-send any 
			//unacknowledged PUBLISH Packets (where QoS > 0) and PUBREL Packets using their original Packet Identifiers [MQTT-4.4.0-1].
			//This is the only circumstance where a Client or Server is REQUIRED to re-deliver messages.
			while (!rePublish(toBroker[activeConnection.requestPipeLineIdx()])) {
				Thread.yield();//have no choice in this corner case.
			}
		}
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


	private boolean processPersistLoad() {
		long connectionId = -1;
		
		while ( (connectionId = connectionId())>=0
				&& PipeReader.tryReadFragment(persistBlobLoad)) {
			
		    int msgIdx = PipeReader.getMsgIdx(persistBlobLoad);
		    switch(msgIdx) {
		    	case PersistedBlobLoadSchema.MSG_BEGINREPLAY_8:
		    		clearAckPublishedCollection();
		    		MQTTIdRangeControllerSchema.publishClearAll(idRangeControl);
		    		isInReloadPersisted = true;
		    	break;
		        case PersistedBlobLoadSchema.MSG_BLOCK_1:
		        {		        	
		        	int fieldBlockId = (int)PipeReader.readLong(persistBlobLoad,PersistedBlobLoadSchema.MSG_BLOCK_1_FIELD_BLOCKID_3);

		        	//Tell the IdGen that this value is already consumed
		        	MQTTIdRangeControllerSchema.publishIdRange(idRangeControl, IdGenStage.buildRange(fieldBlockId, fieldBlockId+1));
		        			        	
		        	Pipe<NetPayloadSchema> server = toBroker[activeConnection.requestPipeLineIdx()];
		        	
		        	//logger.info("read positions and publish block");
		    		final long slabPos = Pipe.getSlabHeadPosition(server);
		    		final int blobPos = Pipe.getBlobHeadPosition(server);
		        	
		    		PipeWriter.presumeWriteFragment(server, NetPayloadSchema.MSG_PLAIN_210);		    		
		    		
		    		DataOutputBlobWriter<NetPayloadSchema> output = PipeWriter.outputStream(server);
		    		DataOutputBlobWriter.openField(output);
		    		
		    		PipeReader.readBytes(persistBlobLoad,
		    				             PersistedBlobLoadSchema.MSG_BLOCK_1_FIELD_BYTEARRAY_2,
		    				             output);
					finishEndOfBrokerMessage(connectionId, server, System.currentTimeMillis());					
					//NOTE: no need to re-store persistently since we are loading..
					storePublishedPosLocal(slabPos, blobPos, fieldBlockId);
					PipeWriter.publishWrites(server);					
		        }
		        break;
		        case PersistedBlobLoadSchema.MSG_FINISHREPLAY_9:
		        	MQTTIdRangeControllerSchema.publishReady(idRangeControl);		        	
		        	isInReloadPersisted = false;
				break;
				
		        case PersistedBlobLoadSchema.MSG_ACKRELEASE_10:
		        	ackPublishedPosLocal((int)PipeReader.readLong(persistBlobLoad,PersistedBlobLoadSchema.MSG_ACKRELEASE_10_FIELD_BLOCKID_3));
		        break;
		        
		        case PersistedBlobLoadSchema.MSG_ACKWRITE_11:
		        	PipeWriter.publishWrites(toBroker[activeConnection.requestPipeLineIdx()]);
		        	PipeReader.releaseReadLock(input);
		        	isInPersistWrite = false;//can now continue with next write
		        			        	
		        	long comittedBlockId = PipeReader.readLong(persistBlobLoad,PersistedBlobLoadSchema.MSG_ACKWRITE_11_FIELD_BLOCKID_3);	        	
		        	//logger.trace("publish ack write is now on disk for id {} ",comittedBlockId);
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
		long connectionId = -1;
		if ((connectionId = connectionId())>=0 && brokerAcknowledgedConnection ) {
			final long now = System.currentTimeMillis();
			final long quiet = now-lastActvityTime;
			if (quiet > (keepAliveMS>>3)) { //every 1/8 of the keep alive re-publish if needed.
				
				if (hasUnackPublished()) {
					rePublish(toBroker[activeConnection.requestPipeLineIdx()]);					
					//if rePublish does something then lastActivityTime will have been set
				} else {
					//we have nothing to re-publish and half the keep alive has gone by so ping to be safe.
					if (quiet > (keepAliveMS>>1)) {
						//logger.trace("note quiet {} trigger {} ",quiet, keepAliveMS);
						//logger.info("request ping, must ensure open first??, if not close?");
						
						requestPing(now, connectionId, toBroker[activeConnection.requestPipeLineIdx()]);					
						lastActvityTime = now;
					}
				}				
			}
		}
		return connectionId;
	}

	private void processInput(long connectionId) {


		
		while ( (PipeReader.peekMsg(input, MQTTClientToServerSchema.MSG_BROKERHOST_100)  				
				|| (
				    ((connectionId = connectionId())>=0)
				   && hasInFlightCapacity()
				   && hasRoomToPersist()
				   && hasRoomToSocketWrite() 	)
				)
				&& PipeReader.tryReadFragment(input)) {

			int msgIdx = PipeReader.getMsgIdx(input);

			if (MQTTClientToServerSchema.MSG_BROKERHOST_100 == msgIdx) {
				
				//logger.info("open new broker socket connection");
				
				this.hostLen = PipeReader.readBytesLength(input, MQTTClientToServerSchema.MSG_BROKERHOST_100_FIELD_HOST_26);
				this.hostPos = 0;
				this.hostMask = Integer.MAX_VALUE;
				PipeReader.readBytes(input, MQTTClientToServerSchema.MSG_BROKERHOST_100_FIELD_HOST_26, hostBack, 0);
				this.hostPort = PipeReader.readInt(input, MQTTClientToServerSchema.MSG_BROKERHOST_100_FIELD_PORT_27);
				//must establish new connection
				ccm.releaseResponsePipeLineIdx(connectionId);
				if (null!=activeConnection) {
					logger.info("close old connection to make new one");
					activeConnection.close();
				}
				PipeReader.releaseReadLock(input);
				break;
			}		
						
			ClientConnection clientConnection = (ClientConnection) ccm.get(connectionId);
			if (null==clientConnection || (! clientConnection.isFinishConnect())) {
			//TODO: may need to optimize later.
		    //TODO: also - inflight should be smaller and the  re-send should happen a few times?
				break;
			}
			
			if (writeToBroker(connectionId, toBroker[activeConnection.requestPipeLineIdx()], msgIdx)) {
				PipeReader.releaseReadLock(input);
			} else {
				isInPersistWrite = true;
				return;//must wait until this is completed
			}
			
		}
	}

	private boolean hasInFlightCapacity() {
			
		//Required space for repeating calls:
		//MQTTClientToServerSchema.MSG_PUBLISH_3    int qos      = PipeReader.readInt(input, MQTTClientToServerSchema.MSG_PUBLISH_3_FIELD_QOS_21);
		//MQTTClientToServerSchema.MSG_SUBSCRIBE_8  1
		//MQTTClientToServerSchema.MSG_UNSUBSCRIBE_10  1
			
		boolean result;
		if (PipeReader.peekMsg(input, MQTTClientToServerSchema.MSG_PUBLISH_3)) {
			result = (PipeReader.peekInt(input, MQTTClientToServerSchema.MSG_PUBLISH_3_FIELD_QOS_21)<=remainingInFlight);
			
			if (!result) {
				logger.info("does not have in flight capacity, B remaining limit {} ",remainingInFlight);
			}
			
		} else if (PipeReader.peekMsg(input, MQTTClientToServerSchema.MSG_SUBSCRIBE_8, 
				                             MQTTClientToServerSchema.MSG_UNSUBSCRIBE_10,
				                             MQTTClientToServerSchema.MSG_PUBREC_5 )) {
			result = 1<=remainingInFlight;
			
			if (!result) {
				logger.info("does not have in flight capacity, A remaining limit {} ",remainingInFlight);
			}
			
		} else {
			result = true;
		}
		
		return result;
		
	}

	private void processInputAcks(long connectionId) {

		while ( (PipeReader.peekMsg(inputAck, MQTTClientToServerSchemaAck.MSG_STOPREPUBLISH_99) 				
				|| (
				   ((connectionId = connectionId())>=0)
				   && hasRoomToPersist()
				   && hasRoomToSocketWrite() 	)
				)
				&& PipeReader.tryReadFragment(inputAck)) {

			//NOTE: warning, if this gets disconnected it may pick a new pipe and the old data may be abandoned?
			
			int msgIdx = PipeReader.getMsgIdx(inputAck);

			if (MQTTClientToServerSchemaAck.MSG_STOPREPUBLISH_99 == msgIdx){				
				ackPublishedPos(PipeReader.readInt(inputAck, MQTTClientToServerSchemaAck.MSG_STOPREPUBLISH_99_FIELD_PACKETID_20));//remaining flight up can only be changed later
				PipeReader.releaseReadLock(inputAck);
				continue;
			}			
			
			if (MQTTClientToServerSchemaAck.MSG_BROKERACKNOWLEDGEDCONNECTION_98 == msgIdx) {
				brokerAcknowledgedConnection = true;
				PipeReader.releaseReadLock(inputAck);
				continue;
			}
			
						
			if (writeAcksToBroker(connectionId, toBroker[activeConnection.requestPipeLineIdx()], msgIdx)) {
				PipeReader.releaseReadLock(inputAck);
			} else {
				isInPersistWrite = true;
				return;//must wait until this is completed
			}
			
		}
	}
	
	private boolean hasRoomToSocketWrite() {
		Pipe<NetPayloadSchema> pipe = toBroker[activeConnection.requestPipeLineIdx()];
		boolean result = PipeWriter.hasRoomForWrite(pipe);
		if (!result) {
			logger.info("no room to write socket "+pipe);
		}
		return result;
	}

	private boolean hasRoomToPersist() {
		boolean result = PipeWriter.hasRoomForWrite(persistBlobStore);
		if (!result) {
			logger.info("no room to persist at this time");
		}
		return result;
	}

	private boolean writeAcksToBroker(long connectionId, Pipe<NetPayloadSchema> server, int msgIdx) {

		long arrivalTime = 0;
		
		//must capture these values now in case we are doing a publish of QOS 1 or 2
		final long slabPos = Pipe.getSlabHeadPosition(server);
		final int blobPos = Pipe.getBlobHeadPosition(server);
		
		
		//////
		PipeWriter.presumeWriteFragment(server, NetPayloadSchema.MSG_PLAIN_210);

		
		DataOutputBlobWriter<NetPayloadSchema> output = PipeWriter.outputStream(server);
		DataOutputBlobWriter.openField(output);

		lastActvityTime = System.currentTimeMillis();
		
		switch (msgIdx) {
					
				case MQTTClientToServerSchemaAck.MSG_PUBACK_4: //Pub from broker to client, QoS1 response to broker
					
					//  From broker to client
					//  broker will keep sending publish to client until we send pub ack
					//
					//  Publish -> client
					//                      PubAck -> broker
					//
					
					arrivalTime = PipeReader.readLong(inputAck, MQTTClientToServerSchemaAck.MSG_PUBACK_4_FIELD_TIME_37);
					
			        //0x40  type/reserved   0100 0000
			        //0x02  remaining length
			        //MSB PacketID high
			        //LSB PacketID low
					output.writeByte(0x40);
					output.writeByte(0x02);
					
					int serverPacketId4 = PipeReader.readInt(inputAck, MQTTClientToServerSchemaAck.MSG_PUBACK_4_FIELD_PACKETID_20);
					output.writeShort(0xFFFF & serverPacketId4);
					finishEndOfBrokerMessage(connectionId, server, arrivalTime);
					PipeWriter.publishWrites(server);
					
				break;					
				case MQTTClientToServerSchemaAck.MSG_PUBCOMP_7: 
					
					//  From broker to client
					//  broker will keep sending PubRel to client until we send pub comp back
					//  client has been repeating PubRec but now must stop and clear it.
					//
					//  Publish -> client
					//                    PubRec -> broker  (client is repeating this and it must be cleared)
					//  PubRel -> client
					//                    PubComp -> broker
					//
					
					arrivalTime = PipeReader.readLong(inputAck, MQTTClientToServerSchemaAck.MSG_PUBCOMP_7_FIELD_TIME_37);
					
			        //0x70  type/reserved   0111 0000
			        //0x02  remaining length
			        //MSB PacketID high
			        //LSB PacketID low
					output.writeByte(0x70);
					output.writeByte(0x02);
					
					int serverPacketId =  PipeReader.readInt(inputAck, MQTTClientToServerSchemaAck.MSG_PUBCOMP_7_FIELD_PACKETID_20);
					output.writeShort(0xFFFF & serverPacketId);
					finishEndOfBrokerMessage(connectionId, server, arrivalTime);
					
					PipeWriter.publishWrites(server);						
					///////////////
					//release the pubrec
					//note this packetId is from the server side.
					//////////////					
					ackPublishedPos(serverPacketId);	
				
				break;									
				
				case MQTTClientToServerSchemaAck.MSG_PUBREL_6: //requires ack room
					//NOTE: upon the client doing QoS2 publish we have already
					//      subtracted 2 from the in-flight counts to ensure this works
					//
					//  From client to broker (QoS2)  Needs 2
					//
					//  Publish -> client (we were repeating this and it must be cleared)
					//                    PubRec -> broker 
					//  PubRel -> client (client keeps sending this until pub comp returns)
					//                    PubComp -> broker
					//
					
					//This message will both release and store for a net of zero change to pending ack requests 
					arrivalTime = PipeReader.readLong(inputAck, MQTTClientToServerSchemaAck.MSG_PUBREL_6_FIELD_TIME_37);
					
			        //0x62  type/reserved   0110 0010
			        //0x02  remaining length
			        //MSB PacketID high
			        //LSB PacketID low
					output.writeByte(0x62);
					output.writeByte(0x02);
					
					int packetId6 = PipeReader.readInt(inputAck, MQTTClientToServerSchemaAck.MSG_PUBREL_6_FIELD_PACKETID_20);
					output.writeShort(0xFFFF & packetId6);

					//this may clear async after persistence. Not this will be taken from
					//tail and the following new storePublish will be a head. Since these
					//are at two different positions this will not be a problem.
					ackPublishedPos(packetId6);
					
					finishEndOfBrokerMessage(connectionId, server, arrivalTime);
					
					//NOTE: this is our publish so we have already accounted for this extra room by subtracting 2 from in flight count
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

				default:
					logger.info("oops, unknown message type {} ",msgIdx);
		}	
		return true;
	}
	
	
	
	private boolean writeToBroker(long connectionId, Pipe<NetPayloadSchema> server, int msgIdx) {

		long arrivalTime = 0;
		
		//logger.trace("write to broker after reading positions");
		//must capture these values now in case we are doing a publish of QOS 1 or 2
		final long slabPos = Pipe.getSlabHeadPosition(server);
		final int blobPos = Pipe.getBlobHeadPosition(server);
		
		
		//////
		PipeWriter.presumeWriteFragment(server, NetPayloadSchema.MSG_PLAIN_210);

		
		DataOutputBlobWriter<NetPayloadSchema> output = PipeWriter.outputStream(server);
		DataOutputBlobWriter.openField(output);

		lastActvityTime = System.currentTimeMillis();

		switch (msgIdx) {
				case MQTTClientToServerSchema.MSG_CONNECT_1:
					
					//block ping until this is complete.
					brokerAcknowledgedConnection = false;

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
						
						MQTTIdRangeControllerSchema.publishReady(idRangeControl);
						
					} else {		
						//open new connect we must first reload the old persisted messages.
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
					finishEndOfBrokerMessage(connectionId, server, arrivalTime);							
					PipeWriter.publishWrites(server);					
				break;
				case MQTTClientToServerSchema.MSG_DISCONNECT_14:
					
					arrivalTime = PipeReader.readLong(input, MQTTClientToServerSchema.MSG_DISCONNECT_14_FIELD_TIME_37);
					
					output.writeByte(0xE0);
					output.writeByte(0x00);
					finishEndOfBrokerMessage(connectionId, server, arrivalTime);					
					PipeWriter.publishWrites(server);
				break;
				case MQTTClientToServerSchema.MSG_PUBLISH_3:
					
					arrivalTime  = PipeReader.readLong(input, MQTTClientToServerSchema.MSG_PUBLISH_3_FIELD_TIME_37);
					int qos      = PipeReader.readInt(input, MQTTClientToServerSchema.MSG_PUBLISH_3_FIELD_QOS_21);
					int packetId = PipeReader.readInt(input, MQTTClientToServerSchema.MSG_PUBLISH_3_FIELD_PACKETID_20);
					remainingInFlight -= qos;
								
					buildPublishMessage(output, qos, packetId);
					finishEndOfBrokerMessage(connectionId, server, arrivalTime);

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
				case MQTTClientToServerSchema.MSG_PUBREC_5: //requires ack room
					remainingInFlight -= 1; 
					//System.err.println("consume 1");
					//  From broker to client (Qos2) Needs 1
					//  broker will keep sending publish until we send pub rec
					//
					//  Publish -> client
					//                    PubRec -> broker  (client must send this until we get pubRel)
					//  PubRel -> client
					//                    PubComp -> broker
					//
					
					//TODO: must subtract 1 for available.
										
					arrivalTime = PipeReader.readLong(input, MQTTClientToServerSchema.MSG_PUBREC_5_FIELD_TIME_37);
					
			        //0x50  type/reserved   0101 0000
			        //0x02  remaining length
			        //MSB PacketID high
			        //LSB PacketID low
					output.writeByte(0x50);
					output.writeByte(0x02);
					int packetId5 = PipeReader.readInt(input, MQTTClientToServerSchema.MSG_PUBREC_5_FIELD_PACKETID_20);
					output.writeShort(0xFFFF & packetId5);
					
					//no need to call ackPublishedPos because this is not held by this end of the communication
					
					finishEndOfBrokerMessage(connectionId, server, arrivalTime);
					
					storePublishedPosLocal(slabPos, blobPos, packetId5); //warning this packetId came from the server..
					if (isPersistantSession) {	
						int consumedBytes5 = Pipe.computeCountOfBytesConsumed(server);
						storePublishedPosPersisted(blobPos, consumedBytes5, Pipe.blob(server), packetId5);	
						return false;
					} else {
						PipeWriter.publishWrites(server);
					}

				break;
				case MQTTClientToServerSchema.MSG_SUBSCRIBE_8:
					remainingInFlight -= 1;

					arrivalTime = PipeReader.readLong(input, MQTTClientToServerSchema.MSG_SUBSCRIBE_8_FIELD_TIME_37);
					
			        int topicIdLen = PipeReader.readBytesLength(input, MQTTClientToServerSchema.MSG_SUBSCRIBE_8_FIELD_TOPIC_23);			
			        output.writeByte((0xFF&0x82));
					encodeVarLength(output, 2 + topicIdLen + 2 + 1); //const and remaining length, 2  bytes
									
					int packetId8 = PipeReader.readInt(input, MQTTClientToServerSchema. MSG_SUBSCRIBE_8_FIELD_PACKETID_20);
					output.writeShort(packetId8);
					
					//logger.trace("requesting new subscription using id :{}",packetId8);
									
					//variable header
					output.writeShort(topicIdLen);
					
					PipeReader.readBytes(input, MQTTClientToServerSchema.MSG_SUBSCRIBE_8_FIELD_TOPIC_23, output);
					
					int subscriptionQoS = PipeReader.readInt(input, MQTTClientToServerSchema.MSG_SUBSCRIBE_8_FIELD_QOS_21);
					
					output.writeByte(subscriptionQoS);
					finishEndOfBrokerMessage(connectionId, server, arrivalTime);
	
					storePublishedPosLocal(slabPos, blobPos, packetId8);
					//hold this until we have our subscription ack
					if (isPersistantSession) {	
						
						logger.info("store to disk the packet for subscription {} ",packetId8);
						
						int consumedBytes8 = Pipe.computeCountOfBytesConsumed(server);
						storePublishedPosPersisted(blobPos, consumedBytes8, Pipe.blob(server), packetId8);	
						return false;
					}				
					
					PipeWriter.publishWrites(server);
					
				break;
				case MQTTClientToServerSchema.MSG_UNSUBSCRIBE_10:
					remainingInFlight -= 1;
					//System.err.println("consume 1");
					
					arrivalTime = PipeReader.readLong(input, MQTTClientToServerSchema.MSG_UNSUBSCRIBE_10_FIELD_TIME_37);
					
					int topicIdLen10 = PipeReader.readBytesLength(input, MQTTClientToServerSchema.MSG_UNSUBSCRIBE_10_FIELD_TOPIC_23);
			        output.writeByte((0xFF&0x82));
					encodeVarLength(output, 2 + topicIdLen10 + 2); //const and remaining length, 2  bytes
									
					int packetId10 = PipeReader.readInt(input, MQTTClientToServerSchema.MSG_UNSUBSCRIBE_10_FIELD_PACKETID_20);
					output.writeShort(packetId10);
					
					//variable header
					output.writeShort(topicIdLen10);
					
					PipeReader.readBytes(input, MQTTClientToServerSchema.MSG_UNSUBSCRIBE_10_FIELD_TOPIC_23, output);
					finishEndOfBrokerMessage(connectionId, server, arrivalTime);
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
				default:
					logger.info("oops, unknown message type {} ",msgIdx);
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

	private void finishEndOfBrokerMessage(long connectionId, Pipe<NetPayloadSchema> server, long arrivalTime) {
		DataOutputBlobWriter.closeHighLevelField(PipeWriter.outputStream(server), NetPayloadSchema.MSG_PLAIN_210_FIELD_PAYLOAD_204);
		
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
		PipeWriter.writeLong(server, NetPayloadSchema.MSG_PLAIN_210_FIELD_ARRIVALTIME_210, now);
		PipeWriter.writeLong(server, NetPayloadSchema.MSG_PLAIN_210_FIELD_POSITION_206, 0); //always use zero for client requests
		PipeWriter.publishWrites(server);
	}

	public void buildPublishMessage(DataOutputBlobWriter<NetPayloadSchema> output, int qos, int packetId) {
		int retain = PipeReader.readInt(input, MQTTClientToServerSchema.MSG_PUBLISH_3_FIELD_RETAIN_22);

		int topicLength = PipeReader.readBytesLength(input, MQTTClientToServerSchema.MSG_PUBLISH_3_FIELD_TOPIC_23);
		int payloadLength = PipeReader.readBytesLength(input, MQTTClientToServerSchema.MSG_PUBLISH_3_FIELD_PAYLOAD_25);
					
		final int pubHead = 0x30 | (0x06&(qos<<1)) | 1&retain; //bit 3 dup is zero which is modified later
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
