package com.ociweb.pronghorn.network.mqtt;

import static com.ociweb.pronghorn.pipe.Pipe.bytePosition;

import javax.net.ssl.SSLEngineResult.HandshakeStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.BasicClientConnectionFactory;
import com.ociweb.pronghorn.network.ClientConnection;
import com.ociweb.pronghorn.network.ClientCoordinator;
import com.ociweb.pronghorn.network.OrderSupervisorStage;
import com.ociweb.pronghorn.network.SSLUtil;
import com.ociweb.pronghorn.network.schema.MQTTClientToServerSchema;
import com.ociweb.pronghorn.network.schema.MQTTClientToServerSchemaAck;
import com.ociweb.pronghorn.network.schema.MQTTIdRangeControllerSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.FragmentWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobLoadConsumerSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobLoadProducerSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobLoadReleaseSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobStoreConsumerSchema;
import com.ociweb.pronghorn.stage.file.schema.PersistedBlobStoreProducerSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.TrieParserReader;

public class MQTTClientToServerEncodeStage extends PronghornStage {

	private final TrieParserReader READER = new TrieParserReader(true);

	private final Logger logger = LoggerFactory.getLogger(MQTTClientToServerEncodeStage.class);
	
	private final Pipe<MQTTClientToServerSchema> input;
	private final Pipe<MQTTClientToServerSchemaAck> inputAck;
	
	private final Pipe<PersistedBlobStoreConsumerSchema> persistBlobStoreConsumer;
	private final Pipe<PersistedBlobStoreProducerSchema> persistBlobStoreProducer;
	
	private final Pipe<PersistedBlobLoadReleaseSchema> persistBlobLoadRelease;
	private final Pipe<PersistedBlobLoadConsumerSchema> persistBlobLoadConsumer;
	private final Pipe<PersistedBlobLoadProducerSchema> persistBlobLoadProducer;
	
	private final Pipe<NetPayloadSchema>[] toBroker; //array is the size of supported "in-flight" messages.
			
	private final Pipe<MQTTIdRangeControllerSchema> idRangeControl;
	
	
	private final int uniqueConnectionId;
	private final ClientCoordinator ccm;
	private boolean brokerAcknowledgedConnection;
	
	private ClientConnection activeConnection;
	private StringBuilder host;
	
	private int hostPort;
	
	
	private int keepAliveMS;
	
	private final int quietRepublish = 20_000;//ms
	private final int quiteDivisorBits = 5;
	
	private boolean isInReloadPersisted;
	private boolean isInPersistWrite;
	private int inPersistSize;
	
	private long replayFromPosition = -1;//reset every time we consume this.
		
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
			                             Pipe<PersistedBlobStoreConsumerSchema> persistBlobStoreConsumer,
			                             Pipe<PersistedBlobStoreProducerSchema> persistBlobStoreProducer,			                             
			                             Pipe<PersistedBlobLoadReleaseSchema> persistBlobLoadRelease,
			                             Pipe<PersistedBlobLoadConsumerSchema> persistBlobLoadConsumer,
			                             Pipe<PersistedBlobLoadProducerSchema> persistBlobLoadProducer,
			                             Pipe<MQTTIdRangeControllerSchema> idRangeControl,
			                             Pipe<NetPayloadSchema>[] toBroker) {
		super(gm, join(input,inputAck,persistBlobLoadConsumer,persistBlobLoadProducer ), join(toBroker,persistBlobStoreConsumer, persistBlobStoreProducer,idRangeControl));
		this.input = input;
		this.inputAck = inputAck;
		this.idRangeControl = idRangeControl;
		this.persistBlobStoreConsumer = persistBlobStoreConsumer;
		this.persistBlobStoreProducer = persistBlobStoreProducer;		
				
		this.persistBlobLoadRelease = persistBlobLoadRelease;
		this.persistBlobLoadConsumer = persistBlobLoadConsumer;
		this.persistBlobLoadProducer = persistBlobLoadProducer;
		
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

	private boolean ofSchema(Pipe<NetPayloadSchema>[] toBroker, NetPayloadSchema instance) {
		int i = toBroker.length;
		while (--i>=0) {
			if (!Pipe.isForSchema(toBroker[i], instance)) {
				logger.info("expected {} but found {}",instance.getClass(), Pipe.schemaName(toBroker[i]));				
				return false;
			}
		}
		return true;
		
	}

	private int countOfBlocksWaitingForPersistLoad = 0;
	
	@Override
	public void run() {		
		if (!processPersistLoad()) {

			long connectionId = processPingAndReplay();
			
			processInputAcks(connectionId);
									
			processInput(connectionId);
				
			
		} else {
			if (Integer.numberOfLeadingZeros(countOfBlocksWaitingForPersistLoad) != Integer.numberOfLeadingZeros(++countOfBlocksWaitingForPersistLoad)) {
				logger.info("NOTE: too much volume, encoding has been blocked by waiting for persistance {} times",countOfBlocksWaitingForPersistLoad);
			}
		}
	}
	
	//////////////////	////////
	//////////////////////////
	
	public boolean hasUnackPublished() {
		
		//logger.info("check values "+(ringMask&ringTail)+" - "+(ringMask&ringHead));
		return (ringMask&ringTail)!=(ringMask&ringHead);
	}
	
	public void clearAckPublishedCollection() {
		ringTail = ringHead = 0;
	}
	
	public int countUnackPublished() {
		int result = (ringMask &ringHead)-(ringMask & ringTail);						
		return result>0? result : ringSize+result;
	}
	
	private void storePublishedPosPersisted(int blobPosition, int blobConsumed, byte[] blob, final int packetId) {
		Pipe.presumeRoomForWrite(persistBlobStoreProducer);
		FragmentWriter.writeLV(persistBlobStoreProducer, PersistedBlobStoreProducerSchema.MSG_BLOCK_1,
				packetId, //persist store supports long but we only have a packetId.
				blob, blobPosition, blobConsumed
				);
	}
	
	private void storePublishedPosLocal(long slabPosition, int blobPosition, final int packetId) {
		if (-1==replayFromPosition) {
			replayFromPosition = slabPosition;
		} else {
			if (slabPosition<replayFromPosition) {
				replayFromPosition = slabPosition;
			}
		}
		
		packetIdRing[ringMask & ringHead] = packetId;
		slabPositionsRing[ringMask & ringHead] = slabPosition;
		blobPositionsRing[ringMask & ringHead] = blobPosition;
		ringHead++;
	}
	
	public void ackPublishedPos(int packetId) {	
		
		
		if (isPersistantSession) {
			//logger.trace("BBB must clear top level ack from drive {} ",packetId);
			
			Pipe.presumeRoomForWrite(persistBlobStoreConsumer);
			FragmentWriter.writeL(persistBlobStoreConsumer, PersistedBlobStoreConsumerSchema.MSG_RELEASE_7,
					packetId
					);
			
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

		replayFromPosition = -1; //get next lowest value.
		int stop = ringMask&ringHead;

		for(int i = (ringTail&ringMask); (i&ringMask)!=stop; i++ ) {

			//skip bad value already acknowledged
			int idx = ringMask&i;
			if (packetIdRing[idx] != Integer.MAX_VALUE) {	
				
				//logger.info("republish to broker packet {} ",packetIdRing[idx]);
				
	    		final long slabPos = Pipe.getSlabHeadPosition(pipe); 
	    		final int blobPos = Pipe.getBlobHeadPosition(pipe);
					    			    	
	    		//////////////////////////////////////////////////////////////////
	    		int firstPos = pipe.blobMask & blobPositionsRing[idx];
				byte[] blob = Pipe.blob(pipe);
				int firstByte = blob[firstPos];
	    		if ((0x30&firstByte)==0x30) {//publish flag	    				
	    			//Set the DUP flag which is the 3rd bit now that we are sending this again.
	    			blob[firstPos] = (byte)(firstByte | 8); //
	    		}
	    		/////////////////////////////////////////////////////////////////
	    		
				if (!Pipe.tryReplication(pipe, slabPositionsRing[idx], blobPositionsRing[idx])) {
					return false;
				}
				lastActvityTime = System.currentTimeMillis(); //no need to ping if we keep reSending these.	
				
				//logger.info("re-sent message with id {}  {}",packetIdRing[ringMask & i],System.currentTimeMillis());
				
				storePublishedPosLocal(slabPos, blobPos, packetIdRing[idx]);
				packetIdRing[idx] = Integer.MAX_VALUE;//clear this one since we have added a new one.
				ringTail = i+1;//move up ring tail since everything up till here is done
			
			} 
			
		}	
		return true;
	}
	
	
	///////////////////////////////
	///////////////////////////////
	
	@Override
	public void startup() {
		host = new StringBuilder();
	}
	
	public long connectionId() {
		
		if (host.length()==0) {
			return -1;
		}
		
		if (null==activeConnection || ((!activeConnection.isValid()) && activeConnection.isFinishConnect() ) ) {
			//only do reOpen if the previous one is finished connecting and its now invalid.
			reOpenConnection(activeConnection);
		}
		
		long result = (null!=activeConnection) ? activeConnection.id : -1;
		return result;
		
	}

	private void reOpenConnection(ClientConnection old) {
		//logger.info("opening connection to broker {}:{} ",
		//		     Appendables.appendUTF8(new StringBuilder(), hostBack, hostPos, hostLen, hostMask), hostPort);

		//this.payloadToken = schema.growStruct(structId, BStructTypes.Blob, 0, "payload".getBytes());
		//only add header support for http calls..
		//int structureId = 
		
		
		int hostId = null!=old? old.hostId : ClientCoordinator.lookupHostId(host, READER);
		long lookup = null!=old? old.id : ccm.lookup(hostId, hostPort, uniqueConnectionId);
		activeConnection = ClientCoordinator.openConnection(ccm, host, hostPort, 
				                         uniqueConnectionId, 
				                         toBroker, lookup, READER,
				                         BasicClientConnectionFactory.instance); 

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
				&& Pipe.hasContentToRead(persistBlobLoadRelease)) {
			
		    int msgIdx = Pipe.takeMsgIdx(persistBlobLoadRelease);
		    switch(msgIdx) {
		        case PersistedBlobLoadReleaseSchema.MSG_ACKRELEASE_10:
		        	ackPublishedPosLocal((int)Pipe.takeLong(persistBlobLoadRelease));
		        break;		       		        
		        case -1:
		           requestShutdown();
		        break;
		    }
		    PipeReader.releaseReadLock(persistBlobLoadRelease);
		}
				
		while ( (connectionId = connectionId())>=0
				&& Pipe.hasContentToRead(persistBlobLoadConsumer)) {
			
		    int msgIdx = Pipe.takeMsgIdx(persistBlobLoadConsumer);
		    switch(msgIdx) {
		    	case PersistedBlobLoadConsumerSchema.MSG_BEGINREPLAY_8:
		    		clearAckPublishedCollection();
		    		
		    		Pipe.presumeRoomForWrite(idRangeControl);
		    		FragmentWriter.write(idRangeControl, MQTTIdRangeControllerSchema.MSG_CLEARALL_2);
		    		
					isInReloadPersisted = true;
		    	break;
		        case PersistedBlobLoadConsumerSchema.MSG_BLOCK_1:
		        {		        	
		        	int fieldBlockId = (int)Pipe.takeLong(persistBlobLoadConsumer);

		        	//Tell the IdGen that this value is already consumed
		        	Pipe.presumeRoomForWrite(idRangeControl);
		        	FragmentWriter.writeI(idRangeControl, MQTTIdRangeControllerSchema.MSG_IDRANGE_1, 
		        			                  IdGenStage.buildRange(fieldBlockId, fieldBlockId+1));
		 		        			        	
		        	Pipe<NetPayloadSchema> server = toBroker[activeConnection.requestPipeLineIdx()];
		        	
		        	//logger.info("read positions and publish block");
		    		final long slabPos = Pipe.getSlabHeadPosition(server);
		    		final int blobPos = Pipe.getBlobHeadPosition(server);
		        	
		    		Pipe.presumeRoomForWrite(server);
		    		Pipe.addMsgIdx(server, NetPayloadSchema.MSG_PLAIN_210);
		    		
		    		Pipe.addLongValue(connectionId, server); //connectionId
		    		Pipe.addLongValue(System.currentTimeMillis(), server); //arrival time
		    		Pipe.addLongValue(0, server); //position
		    				    			    		
		    		//payload
		    		DataOutputBlobWriter<NetPayloadSchema> output = Pipe.openOutputStream(server);
		    		Pipe.readBytes(persistBlobLoadConsumer, output);
		    		output.closeLowLevelField();
		    		
					//NOTE: no need to re-store persistently since we are loading..
					storePublishedPosLocal(slabPos, blobPos, fieldBlockId);
					
					Pipe.confirmLowLevelWrite(server, Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_PLAIN_210));
					Pipe.publishWrites(server);
					
					//logger.info("wrote block of {}",len2);					
		        }
		        break;
		        case PersistedBlobLoadConsumerSchema.MSG_FINISHREPLAY_9:				
		        	Pipe.presumeRoomForWrite(idRangeControl);
		        	FragmentWriter.write(idRangeControl, MQTTIdRangeControllerSchema.MSG_READY_3);
		        	isInReloadPersisted = false;
				break;      		        
		        case -1:
		           requestShutdown();
		        break;
		    }
		    PipeReader.releaseReadLock(persistBlobLoadConsumer);
		}
		
		while ( (connectionId = connectionId())>=0
				&& Pipe.hasContentToRead(persistBlobLoadProducer)) {
			
		    int msgIdx = Pipe.takeMsgIdx(persistBlobLoadProducer);
		    switch(msgIdx) {
		        case PersistedBlobLoadProducerSchema.MSG_ACKWRITE_11:
		        	
		        	Pipe<NetPayloadSchema> pipe = toBroker[activeConnection.requestPipeLineIdx()];
					Pipe.confirmLowLevelWrite(pipe, Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_PLAIN_210));
					Pipe.publishWrites(pipe);
							        	
					assert(inPersistSize>=0);
		        	Pipe.confirmLowLevelRead(input, inPersistSize);
		        	inPersistSize = -1;
		        	Pipe.releaseReadLock(input);
		        	
		        	isInPersistWrite = false;//can now continue with next write
		        			        	
		        	long comittedBlockId = Pipe.takeLong(persistBlobLoadProducer);	
		        	assert(comittedBlockId>=0);
		        	//logger.trace("publish ack write is now on disk for id {} ",comittedBlockId);
		        	//NOTE: if desired we could send this id back to MQTTClient, but I see no need at this time.
		        			        	
		        break;		        
		        case -1:
		           requestShutdown();
		        break;
		    }
		    PipeReader.releaseReadLock(persistBlobLoadProducer);
		}		
		
		return isInReloadPersisted || isInPersistWrite;
	}

	
	private long processPingAndReplay() {
		long connectionId = -1;
		if ((connectionId = connectionId())>=0 
			&& brokerAcknowledgedConnection) {  
			final long now = System.currentTimeMillis();
			final long quiet = now-lastActvityTime;
			if (quiet > (keepAliveMS>>quiteDivisorBits)) { //every 1/32 of the keep alive re-publish if needed.
				
				if (hasUnackPublished()) {	
					if (keepAliveMS>0 || quiet>quietRepublish) {
						
						Pipe<NetPayloadSchema> pipe = toBroker[activeConnection.requestPipeLineIdx()];
						if (0 == Pipe.contentRemaining(pipe))  {
							rePublish(pipe);
						}
						//else when there is conten remaining the system is busy and doing
						//a replublish will not help plus it may cause repeats of the 
						//republish on the same pipes at the same time.
					}
					//if rePublish does something then lastActivityTime will have been set
				} else {
					//we have nothing to re-publish and half the keep alive has gone by so ping to be safe.
					//when keepAliveMS is zero do not ping
					if ((keepAliveMS>0) && (quiet > (keepAliveMS>>1))) {
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


		
		while ( (Pipe.peekMsg(input, MQTTClientToServerSchema.MSG_BROKERHOST_100)  				
				|| (
				    ((connectionId = connectionId())>=0)
				   && hasInFlightCapacity()
				   && hasRoomToPersist()
				   && hasRoomToSocketWriteOrDropQoSZeros()
				   && hasRoomForNewMessagePlusReplay()
						)
				)
				&& Pipe.hasContentToRead(input)) {

			if (Pipe.peekMsg(input, MQTTClientToServerSchema.MSG_BROKERHOST_100)) {
				
					final int msgIdx = Pipe.takeMsgIdx(input);
					assert(msgIdx == MQTTClientToServerSchema.MSG_BROKERHOST_100);
					//logger.info("open new broker socket connection");
					
					this.host.setLength(0);
										
					int hostMeta = Pipe.takeRingByteMetaData(input);					
					int hostLen = Pipe.takeRingByteLen(input);
					Pipe.readUTF8(input, this.host, hostMeta, hostLen);
														
					this.hostPort = Pipe.takeInt(input);
					//must establish new connection
					ccm.releaseResponsePipeLineIdx(connectionId);
					if (null!=activeConnection) {
						logger.info("close old connection to make new one");
						activeConnection.close();
					}
					
					Pipe.confirmLowLevelRead(input, Pipe.sizeOf(MQTTClientToServerSchema.instance, MQTTClientToServerSchema.MSG_BROKERHOST_100 ));
					Pipe.releaseReadLock(input);
				
				break;
			}		
						
			ClientConnection clientConnection = (ClientConnection) ccm.connectionForSessionId(connectionId);
			if (null==clientConnection || (! clientConnection.isFinishConnect())) {
				break;
			}
					
			Pipe<NetPayloadSchema> server = toBroker[activeConnection.requestPipeLineIdx()];
			
//			long count1 = Pipe.totalWrittenFragments(server);
//			logger.info("total messages so far {}",count1);
//		
	
	        if (ccm.isTLS) {	
	        	//NOTE: this will continue triggering handshake work until complete.
				if (!SSLUtil.handshakeProcessing(server, clientConnection)) {
					///logger.trace("during handshake later {} ", Pipe.totalWrittenFragments(server));
					//we must wait until later...
					return;
				}
				
				if (HandshakeStatus.NEED_UNWRAP == clientConnection.getEngine().getHandshakeStatus()) {
					return;//wait until this is cleared before continuing
				}
				//logger.info("after handshake done {} status {} "
				//		, Pipe.totalWrittenFragments(server)
				//		, clientConnection.getEngine().getHandshakeStatus());
				
				//send empt block??
				if (Pipe.hasRoomForWrite(server)) {
		    		//logger.trace("write handshake plain to trigger wrap");
		    		int size = Pipe.addMsgIdx(server, NetPayloadSchema.MSG_PLAIN_210);
		    		Pipe.addLongValue(connectionId, server);//connection
		    		Pipe.addLongValue(System.currentTimeMillis(), server);
		    		Pipe.addLongValue(SSLUtil.HANDSHAKE_POS, server); //signal that WRAP is needed 
		    		
		    		Pipe.addByteArray(OrderSupervisorStage.EMPTY, 0, 0, server);
		    		
		    		Pipe.confirmLowLevelWrite(server, size);
		    		Pipe.publishWrites(server);
		    		//wait for this to be consumed		    		
		    	}
				
				
			}

	        if (!Pipe.hasRoomForWrite(server)) {
	        	break;//try again later, no room to write out to server.
	        }
	        			   
			final int msgIdx = Pipe.takeMsgIdx(input);
			assert(Pipe.isForSchema(toBroker[activeConnection.requestPipeLineIdx()], NetPayloadSchema.instance)) : "found unexpected "+Pipe.schemaName(toBroker[activeConnection.requestPipeLineIdx()]);
			
			if (writeToBrokerServer(connectionId, server, msgIdx)) {
				
				//logger.trace("after write to broker done {} ", Pipe.totalWrittenFragments(server));
				
				Pipe.confirmLowLevelRead(input, Pipe.sizeOf(MQTTClientToServerSchema.instance,msgIdx));
				Pipe.releaseReadLock(input);
				
			} else {
				
				//logger.trace("did not write to broker {} ", Pipe.totalWrittenFragments(server));
				
				inPersistSize = Pipe.sizeOf(MQTTClientToServerSchema.instance,msgIdx);						
				isInPersistWrite = true;
				return;//must wait until this is completed
			}
			
		}
	}

	private long MAX_FRAG_SIZE_REPLAY = FieldReferenceOffsetManager.maxFragmentSize(NetPayloadSchema.FROM); //max we will write
	
	int countOfReplayBlocks = 0;
	private boolean hasRoomForNewMessagePlusReplay() {
		
		Pipe<NetPayloadSchema> server = toBroker[activeConnection.requestPipeLineIdx()];
		
		//can not write if we have replay values we may write over.		
		if (-1==replayFromPosition) {			
			return true;
		} else {
			//NOTE: this rePublish may be happening far too often if the pipe is too short...
			
			rePublish(server);
	
			if (-1==replayFromPosition) {			
				return true;
			}
		}
		final long slabPos = Pipe.getSlabHeadPosition(server); //where we will write..
		long maxReplaySpaceNeeded = countUnackPublished()*MAX_FRAG_SIZE_REPLAY;
		long wrappedOnceLimit = replayFromPosition+server.sizeOfSlabRing;
		boolean result =  (maxReplaySpaceNeeded + slabPos + MAX_FRAG_SIZE_REPLAY) < wrappedOnceLimit;				          
		if (!result) {
			if (Integer.numberOfLeadingZeros(countOfReplayBlocks) != 
				Integer.numberOfLeadingZeros(++countOfReplayBlocks)) {
				
				logger.info("max write {} max position {} slab write to {} ",
						 (maxReplaySpaceNeeded + slabPos + MAX_FRAG_SIZE_REPLAY), 
						 replayFromPosition, slabPos);
				
				logger.info("Warning: required replay messages have blocked new content {} times",countOfReplayBlocks);
			}
		}
		return result;
	}

	private boolean hasInFlightCapacity() {
			
		//Required space for repeating calls:
		//MQTTClientToServerSchema.MSG_PUBLISH_3    int qos      = PipeReader.readInt(input, MQTTClientToServerSchema.MSG_PUBLISH_3_FIELD_QOS_21);
		//MQTTClientToServerSchema.MSG_SUBSCRIBE_8  1
		//MQTTClientToServerSchema.MSG_UNSUBSCRIBE_10  1
			
		boolean result;
		if (Pipe.peekMsg(input, MQTTClientToServerSchema.MSG_PUBLISH_3)) {
			result = (Pipe.peekInt(input, MQTTClientToServerSchema.MSG_PUBLISH_3_FIELD_QOS_21)<=remainingInFlight);
			
			if (!result) {
				logger.info("in flight capacity saturated, remaining limit {} ",remainingInFlight);
			}
			
		} else if (Pipe.peekMsg(input, MQTTClientToServerSchema.MSG_SUBSCRIBE_8, 
				                             MQTTClientToServerSchema.MSG_UNSUBSCRIBE_10,
				                             MQTTClientToServerSchema.MSG_PUBREC_5 )) {
			result = 1<=remainingInFlight;
			
			if (!result) {
				logger.info("does not have in flight capacity, remaining limit {} ",remainingInFlight);
			}
			
		} else {
			result = true;
		}
		
		return result;
		
	}

	private void processInputAcks(long connectionId) {

		while ( (Pipe.peekMsg(inputAck, MQTTClientToServerSchemaAck.MSG_STOPREPUBLISH_99) 				
				|| (
				   ((connectionId = connectionId())>=0)
				  
				   && hasRoomToSocketWriteOrDropQoSZeros() 	)
				)
				&& Pipe.hasContentToRead(inputAck)
				&& (!isPersistantSession || hasRoomToPersist()) ) {

			//NOTE: warning, if this gets disconnected it may pick a new pipe and the old data may be abandoned?
			
			int msgIdx = Pipe.takeMsgIdx(inputAck);

			if (MQTTClientToServerSchemaAck.MSG_STOPREPUBLISH_99 == msgIdx){				
				ackPublishedPos(Pipe.takeInt(inputAck));
				Pipe.confirmLowLevelRead(inputAck, Pipe.sizeOf(MQTTClientToServerSchemaAck.instance, MQTTClientToServerSchemaAck.MSG_STOPREPUBLISH_99));
				Pipe.releaseReadLock(inputAck);
				continue;
			} else			
			
			if (MQTTClientToServerSchemaAck.MSG_BROKERACKNOWLEDGEDCONNECTION_98 == msgIdx) {
				brokerAcknowledgedConnection = true;
				Pipe.confirmLowLevelRead(inputAck, Pipe.sizeOf(MQTTClientToServerSchemaAck.instance, MQTTClientToServerSchemaAck.MSG_BROKERACKNOWLEDGEDCONNECTION_98));
				Pipe.releaseReadLock(inputAck);
				
				continue;
			} else			
						
			if (writeAcksToBroker(connectionId, toBroker[activeConnection.requestPipeLineIdx()], msgIdx)) {
				Pipe.confirmLowLevelRead(inputAck, Pipe.sizeOf(MQTTClientToServerSchemaAck.instance, msgIdx));
				Pipe.releaseReadLock(inputAck);
			} else {
				isInPersistWrite = true;
				return;//must wait until this is completed
			}
			
		}
	}
	
	public boolean reportDroppedMessages = true;
	public long totalDroppedMessages = 0;
	public StringBuilder topicDropped = new StringBuilder(); 
	
	private boolean hasRoomToSocketWriteOrDropQoSZeros() {
		boolean hasRoom = Pipe.hasRoomForWrite(toBroker[activeConnection.requestPipeLineIdx()]);
		if (!hasRoom && Pipe.peekMsg(input, 0xF & MQTTClientToServerSchema.MSG_PUBLISH_3)) {
			//if there is no room and the next item is a QoS 0 drop it.
			int qos = Pipe.peekInt(input, 0xF & MQTTClientToServerSchema.MSG_PUBLISH_3_FIELD_QOS_21);
			if (qos==0) {
				if (reportDroppedMessages) {
					if (Long.numberOfLeadingZeros(totalDroppedMessages) != Long.numberOfLeadingZeros(++totalDroppedMessages)) {
						
						topicDropped.setLength(0);
						topicDropped.append("Most recently dropped topic: ");
						
						Pipe.peekUTF8(input, 0xF & MQTTClientToServerSchema.MSG_PUBLISH_3_FIELD_TOPIC_23, topicDropped);
						
						logger.info("Network is not consuming message fast enough. {} accumulative QoS 0 messages have now been dropped."
								+ "\nYou may want to check your network or lower the rate of messages sent. \n{}"
								,totalDroppedMessages, topicDropped);
						
					}
				}
				
				//toss this QoS 0 message the pipes are backed up.
				Pipe.skipNextFragment(input);
			}
		}
		return hasRoom;
	}

	int countOfPersistStoreBlocks = 0;
	private boolean hasRoomToPersist() {
		boolean result = Pipe.hasRoomForWrite(persistBlobStoreConsumer)&&Pipe.hasRoomForWrite(persistBlobStoreProducer);
		if ((!result) &
			(Integer.numberOfLeadingZeros(countOfPersistStoreBlocks) != Integer.numberOfLeadingZeros(++countOfPersistStoreBlocks))) {
			logger.info("Warning: encoding blocked {} times waiting to persist store ",countOfPersistStoreBlocks);
		}
		
		return result;
	}

	private boolean writeAcksToBroker(long connectionId, Pipe<NetPayloadSchema> server, int msgIdx) {

		long arrivalTime = 0;
		
		//must capture these values now in case we are doing a publish of QOS 1 or 2
		final long slabPos = Pipe.getSlabHeadPosition(server);
		final int blobPos = Pipe.getBlobHeadPosition(server);
		
		Pipe.presumeRoomForWrite(server);
				
		lastActvityTime = System.currentTimeMillis();
		
		switch (msgIdx) {
					
				case MQTTClientToServerSchemaAck.MSG_PUBACK_4: //Pub from broker to client, QoS1 response to broker
				{
					Pipe.addMsgIdx(server, NetPayloadSchema.MSG_PLAIN_210);
					DataOutputBlobWriter<NetPayloadSchema> output = Pipe.openOutputStream(server);
					//  From broker to client
					//  broker will keep sending publish to client until we send pub ack
					//
					//  Publish -> client
					//                      PubAck -> broker
					//
					
					arrivalTime = Pipe.takeLong(inputAck);
					
					Pipe.addLongValue(connectionId, server);
					Pipe.addLongValue(arrivalTime, server);
					Pipe.addLongValue(0, server);//always use zero for client requests
					
			        //0x40  type/reserved   0100 0000
			        //0x02  remaining length
			        //MSB PacketID high
			        //LSB PacketID low
					output.writeByte(0x40);
					output.writeByte(0x02);
					
					int serverPacketId4 = Pipe.takeInt(inputAck);
					output.writeShort(0xFFFF & serverPacketId4);
					output.closeLowLevelField();

					
					Pipe.confirmLowLevelWrite(server,
							Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_PLAIN_210));
					Pipe.publishWrites(server);
				}			
				break;					
				case MQTTClientToServerSchemaAck.MSG_PUBCOMP_7: 
				{
					Pipe.addMsgIdx(server, NetPayloadSchema.MSG_PLAIN_210);
					DataOutputBlobWriter<NetPayloadSchema> output = Pipe.openOutputStream(server);
					//  From broker to client
					//  broker will keep sending PubRel to client until we send pub comp back
					//  client has been repeating PubRec but now must stop and clear it.
					//
					//  Publish -> client
					//                    PubRec -> broker  (client is repeating this and it must be cleared)
					//  PubRel -> client
					//                    PubComp -> broker
					//
					
					arrivalTime = Pipe.takeLong(inputAck);
					
					Pipe.addLongValue(connectionId, server);
					Pipe.addLongValue(arrivalTime, server);
					Pipe.addLongValue(0, server);//always use zero for client requests
					
			        //0x70  type/reserved   0111 0000
			        //0x02  remaining length
			        //MSB PacketID high
			        //LSB PacketID low
					output.writeByte(0x70);
					output.writeByte(0x02);
					
					int serverPacketId = Pipe.takeInt(inputAck);
					output.writeShort(0xFFFF & serverPacketId);
					output.closeLowLevelField();

					Pipe.confirmLowLevelWrite(server, Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_PLAIN_210));
					Pipe.publishWrites(server);					
					///////////////
					//release the pubrec
					//note this packetId is from the server side.
					//////////////					
					ackPublishedPos(serverPacketId);	
				}
				break;									
				
				case MQTTClientToServerSchemaAck.MSG_PUBREL_6: //requires ack room
				{
					Pipe.addMsgIdx(server, NetPayloadSchema.MSG_PLAIN_210);
					DataOutputBlobWriter<NetPayloadSchema> output = Pipe.openOutputStream(server);
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
					arrivalTime = Pipe.takeLong(inputAck);
					
					Pipe.addLongValue(connectionId, server);
					Pipe.addLongValue(arrivalTime, server);
					Pipe.addLongValue(0, server);//always use zero for client requests
					
			        //0x62  type/reserved   0110 0010
			        //0x02  remaining length
			        //MSB PacketID high
			        //LSB PacketID low
					output.writeByte(0x62);
					output.writeByte(0x02);
					
					int packetId6 = Pipe.takeInt(inputAck);
					output.writeShort(0xFFFF & packetId6);

					//this may clear async after persistence. Not this will be taken from
					//tail and the following new storePublish will be a head. Since these
					//are at two different positions this will not be a problem.
					ackPublishedPos(packetId6);
					output.closeLowLevelField();
					
					//NOTE: this is our publish so we have already accounted for this extra room by subtracting 2 from in flight count
					storePublishedPosLocal(slabPos, blobPos, packetId6);
					//hold this publrel re-send until we get an ack for this packetId.
					if (isPersistantSession) {	
						int consumedBytes6 = Pipe.computeCountOfBytesConsumed(server);
						storePublishedPosPersisted(blobPos, consumedBytes6, Pipe.blob(server), packetId6);	
						return false;
					} else {
						Pipe.confirmLowLevelWrite(server, Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_PLAIN_210));
						Pipe.publishWrites(server);
					}
				}
				break;
				case -1:
					//TODO: need to pass along the shutdown message
					
				break;
				default:
					logger.info("oops, unknown message type {} ",msgIdx);
		}	
		return true;
	}
	
	
	
	private boolean writeToBrokerServer(long connectionId, Pipe<NetPayloadSchema> server, int msgIdx) {

		
		assert(FieldReferenceOffsetManager.isGroupTemplate(Pipe.from(server),NetPayloadSchema.MSG_PLAIN_210));
	
		long arrivalTime = 0;
		
		//logger.trace("write to broker after reading positions");
		//must capture these values now in case we are doing a publish of QOS 1 or 2
		final long slabPos = Pipe.getSlabHeadPosition(server);
		final int blobPos = Pipe.getBlobHeadPosition(server);
		
		//logger.info("write plain message at position {} ",Pipe.workingHeadPosition(server));

		lastActvityTime = System.currentTimeMillis();

		//logger.info("send message value {}",msgIdx);
		
		switch (msgIdx) {
				case MQTTClientToServerSchema.MSG_CONNECT_1:
					{
			
						int plainSize = Pipe.addMsgIdx(server, NetPayloadSchema.MSG_PLAIN_210);
						DataOutputBlobWriter<NetPayloadSchema> output = Pipe.openOutputStream(server);
						
						//block ping until this is complete.
						brokerAcknowledgedConnection = false;
	
						arrivalTime = Pipe.takeLong(input);										
						int keepAliveSec = Pipe.takeInt(input);
						int conFlags = Pipe.takeInt(input);
												
						Pipe.addLongValue(connectionId, server);
						Pipe.addLongValue(arrivalTime, server);
						Pipe.addLongValue(0, server);
												
						int clientIdMeta2 = Pipe.takeRingByteMetaData(input);						
						int clientIdLen = Pipe.takeRingByteLen(input);
						int clientIdPos = bytePosition(clientIdMeta2, input, clientIdLen);  
	
						int willTopicMeta2 = Pipe.takeRingByteMetaData(input);						
						int willTopicLen = Pipe.takeRingByteLen(input);
						int willTopicPos = bytePosition(willTopicMeta2, input, willTopicLen); 
						
						int willMessageMeta2 = Pipe.takeRingByteMetaData(input);						
						int willMessageLen = Pipe.takeRingByteLen(input);
						int willMessagePos = bytePosition(willMessageMeta2, input, willMessageLen);
						
						int userMeta2 = Pipe.takeRingByteMetaData(input);						
						int userLen = Pipe.takeRingByteLen(input);
						int userPos = bytePosition(userMeta2, input, userLen);
						
						int passMeta2 = Pipe.takeRingByteMetaData(input);						
						int passLen = Pipe.takeRingByteLen(input);
						int passPos = bytePosition(passMeta2, input, passLen);
	
						keepAliveMS = keepAliveSec*1000;
	
						isPersistantSession = (conFlags&MQTTEncoder.CONNECT_FLAG_CLEAN_SESSION_1) == 0; 
						if (!isPersistantSession) {
							//if this is a new clean session then we must clear out our repeats
							clearAckPublishedCollection();
							clearPersistedCollection();
							
							Pipe.presumeRoomForWrite(idRangeControl);
							FragmentWriter.write(idRangeControl, MQTTIdRangeControllerSchema.MSG_READY_3);
														
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
			        	output.write(input.blobRing, clientIdPos, clientIdLen, input.blobMask);
			        													
						if (0!=(MQTTEncoder.CONNECT_FLAG_WILL_FLAG_2&conFlags)) {
														
							output.writeShort(willTopicLen);
							output.write(input.blobRing, willTopicPos, willTopicLen, input.blobMask);
							output.writeShort(willMessageLen);
							output.write(input.blobRing, willMessagePos, willMessageLen, input.blobMask);

						}
						
						if (0!=(MQTTEncoder.CONNECT_FLAG_USERNAME_7&conFlags)) {						
							output.writeShort(userLen);
							output.write(input.blobRing, userPos, userLen, input.blobMask);							
						}
						
						if (0!=(MQTTEncoder.CONNECT_FLAG_PASSWORD_6&conFlags)) {								
							output.writeShort(passLen);
							output.write(input.blobRing, passPos, passLen, input.blobMask);
						}
						output.closeLowLevelField();
									
						Pipe.confirmLowLevelWrite(server, plainSize);
						Pipe.publishWrites(server);
						//logger.trace("wrote connect block ");
					}
				break;
				case MQTTClientToServerSchema.MSG_DISCONNECT_14:
				{
					
					int plainSize = Pipe.addMsgIdx(server, NetPayloadSchema.MSG_PLAIN_210);
					DataOutputBlobWriter<NetPayloadSchema> output = Pipe.openOutputStream(server);
					
					arrivalTime = Pipe.takeLong(input);
					
					Pipe.addLongValue(connectionId, server);
					Pipe.addLongValue(arrivalTime, server);
					Pipe.addLongValue(0, server);//always use zero for client requests
					
					output.writeByte(0xE0);
					output.writeByte(0x00);
					output.closeLowLevelField();
			
					Pipe.confirmLowLevelWrite(server, plainSize);
					Pipe.publishWrites(server);
					//logger.info("wrote block of {}",len2);
				}
				break;
				case MQTTClientToServerSchema.MSG_PUBLISH_3:
				{
					int plainSize = Pipe.addMsgIdx(server, NetPayloadSchema.MSG_PLAIN_210);
					DataOutputBlobWriter<NetPayloadSchema> output = Pipe.openOutputStream(server);
					
					arrivalTime  = Pipe.takeLong(input);
					int packetId = Pipe.takeInt(input);
					int qos      = Pipe.takeInt(input);
										
					Pipe.addLongValue(connectionId, server);
					Pipe.addLongValue(arrivalTime, server);
					Pipe.addLongValue(0, server);//always use zero for client requests
					
					remainingInFlight -= qos;
								
					buildPublishMessage(output, qos, packetId);
					output.closeLowLevelField();

					//logger.info("publish with qos {} ", qos);
					
					if (qos != 0) {
						storePublishedPosLocal(slabPos, blobPos, packetId);
						//hold for re-send until we get an ack for this packetId.
						if (isPersistantSession) {	
							int consumedBytes = Pipe.computeCountOfBytesConsumed(server);
							storePublishedPosPersisted(blobPos, consumedBytes, Pipe.blob(server), packetId);
							//NOTE: caller takes msgIdx and keeps the right closing size
							return false;
						}
					}
					Pipe.confirmLowLevelWrite(server, plainSize);
					Pipe.publishWrites(server);
					//logger.info("full length of publish write {} Pipe {} {} {}",len3, server, server.slabMask, server.blobMask);
				}
				break;
				case MQTTClientToServerSchema.MSG_PUBREC_5: //requires ack room
					{
						int plainSize = Pipe.addMsgIdx(server, NetPayloadSchema.MSG_PLAIN_210);
						DataOutputBlobWriter<NetPayloadSchema> output = Pipe.openOutputStream(server);
						
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
					
						arrivalTime = Pipe.takeLong(input);
						
						Pipe.addLongValue(connectionId, server);
						Pipe.addLongValue(arrivalTime, server);
						Pipe.addLongValue(0, server);//always use zero for client requests
						
				        //0x50  type/reserved   0101 0000
				        //0x02  remaining length
				        //MSB PacketID high
				        //LSB PacketID low
						output.writeByte(0x50);
						output.writeByte(0x02);
						int packetId5 = Pipe.takeInt(input);
						output.writeShort(0xFFFF & packetId5);
						output.closeLowLevelField();
						//no need to call ackPublishedPos because this is not held by this end of the communication
		
						storePublishedPosLocal(slabPos, blobPos, packetId5); //warning this packetId came from the server..
						if (isPersistantSession) {	
							int consumedBytes5 = Pipe.computeCountOfBytesConsumed(server);
							storePublishedPosPersisted(blobPos, consumedBytes5, Pipe.blob(server), packetId5);	
							//NOTE: caller takes msgIdx and keeps the right closing size
							return false;
						} else {
							Pipe.confirmLowLevelWrite(server, plainSize);
							Pipe.publishWrites(server);
							//logger.info("wrote block of {}",len4);
						}
					}
				break;
				case MQTTClientToServerSchema.MSG_SUBSCRIBE_8:
					{
						int plainSize = Pipe.addMsgIdx(server, NetPayloadSchema.MSG_PLAIN_210);
						DataOutputBlobWriter<NetPayloadSchema> output = Pipe.openOutputStream(server);
						
						remainingInFlight -= 1;
	
						arrivalTime = Pipe.takeLong(input); 
						int packetId8 = Pipe.takeInt(input); 
						int subscriptionQoS = Pipe.takeInt(input);
						int topicMeta = Pipe.takeRingByteMetaData(input);
						int topicLen  = Pipe.takeRingByteLen(input);
	
						Pipe.addLongValue(connectionId, server);
						Pipe.addLongValue(arrivalTime, server);
						Pipe.addLongValue(0, server);//always use zero for client requests
						
						output.writeByte((0xFF&0x82));
						encodeVarLength(output, 2 + topicLen + 2 + 1); //const and remaining length, 2  bytes
										
						output.writeShort(packetId8);
						
						//logger.trace("requesting new subscription using id :{}",packetId8);
										
						//variable header
						output.writeShort(topicLen);
						
						Pipe.readBytes(input, output, topicMeta, topicLen);
						
						
						output.writeByte(subscriptionQoS);
						output.closeLowLevelField();
		
						storePublishedPosLocal(slabPos, blobPos, packetId8);
						//hold this until we have our subscription ack
						if (isPersistantSession) {	
							
							logger.info("store to disk the packet for subscription {} ",packetId8);
							
							int consumedBytes8 = Pipe.computeCountOfBytesConsumed(server);
							storePublishedPosPersisted(blobPos, consumedBytes8, Pipe.blob(server), packetId8);	
							//NOTE: caller takes msgIdx and keeps the right closing size
							return false;
						}				
						
						Pipe.confirmLowLevelWrite(server, plainSize);
						Pipe.publishWrites(server);
						//logger.info("wrote block of {}",len5);
					}
				break;
				case MQTTClientToServerSchema.MSG_UNSUBSCRIBE_10:
					{
						int plainSize = Pipe.addMsgIdx(server, NetPayloadSchema.MSG_PLAIN_210);
						DataOutputBlobWriter<NetPayloadSchema> output = Pipe.openOutputStream(server);
						
						remainingInFlight -= 1;
						//System.err.println("consume 1");
						
						arrivalTime = Pipe.takeLong(input);					
						int packetId10 = Pipe.takeInt(input);					
						int topicMeta = Pipe.takeRingByteMetaData(input);
						int topicIdLen10 = Pipe.takeRingByteLen(input);
								
						Pipe.addLongValue(connectionId, server);
						Pipe.addLongValue(arrivalTime, server);
						Pipe.addLongValue(0, server);//always use zero for client requests
						
				        output.writeByte((0xFF&0x82));
						encodeVarLength(output, 2 + topicIdLen10 + 2); //const and remaining length, 2  bytes
										
						output.writeShort(packetId10);
						
						//variable header
						output.writeShort(topicIdLen10);
						
						Pipe.readBytes(input, output, topicMeta, topicIdLen10);
						output.closeLowLevelField();

						//hold this until we have our un-subscription ack
						
						storePublishedPosLocal(slabPos, blobPos, packetId10);
						if (isPersistantSession) {	
							int consumedBytes10 = Pipe.computeCountOfBytesConsumed(server);
							storePublishedPosPersisted(blobPos, consumedBytes10, Pipe.blob(server), packetId10);
							//NOTE: caller takes msgIdx and keeps the right closing size
							return false;
						} else {
							
							Pipe.confirmLowLevelWrite(server, plainSize);
							Pipe.publishWrites(server);
							//logger.info("wrote block of {}",len6);
						}
					}
				break;
				case -1:
					//TODO: need to pass along the shutdown message
					
				break;	
				default:
					logger.info("oops, unknown message type {} ",msgIdx);
		}	
		return true;
	}

	private void requestReplayOfPersistedCollection() {
		Pipe.presumeRoomForWrite(persistBlobStoreConsumer);
		FragmentWriter.write(persistBlobStoreConsumer, PersistedBlobStoreConsumerSchema.MSG_REQUESTREPLAY_6);
	}

	private void clearPersistedCollection() {
		Pipe.presumeRoomForWrite(persistBlobStoreConsumer);
		FragmentWriter.write(persistBlobStoreConsumer, PersistedBlobStoreConsumerSchema.MSG_CLEAR_12);
	}

	private void requestPing(long now, long connectionId, Pipe<NetPayloadSchema> server) {
		
		Pipe.presumeRoomForWrite(server);
		int size = Pipe.addMsgIdx(server, NetPayloadSchema.MSG_PLAIN_210);
		Pipe.addLongValue(connectionId, server);
		Pipe.addLongValue(now, server);
		Pipe.addLongValue(0, server); //always use zero for client requests
	
		DataOutputBlobWriter<NetPayloadSchema> output = Pipe.openOutputStream(server);
		output.writeByte(0xC0);
		output.writeByte(0x00);	
		output.closeLowLevelField();
		
		Pipe.confirmLowLevelWrite(server, size);
		Pipe.publishWrites(server);
		//logger.info("wrote block of {}",len2);
	}

	private void buildPublishMessage(DataOutputBlobWriter<NetPayloadSchema> output, int qos, int packetId) {
				
		buildPublishMessage(output, qos, packetId, 
				Pipe.takeInt(input), 
				Pipe.takeRingByteMetaData(input), 
				Pipe.takeRingByteLen(input), 
				Pipe.takeRingByteMetaData(input), 
				Pipe.takeRingByteLen(input));
	}

	private void buildPublishMessage(DataOutputBlobWriter<NetPayloadSchema> output, int qos, int packetId, int retain,
			int topicMeta, int topicLength, int payloadMeta, int payloadLength) {
		
		final int pubHead = 0x30 | (0x06&(qos<<1)) | 1&retain; //bit 3 dup is zero which is modified later
		output.writeByte((0xFF&pubHead));
		encodeVarLength(output, topicLength + 2 + payloadLength + (packetId>=0 ? 2 : 0)); //const and remaining length, 2  bytes

		//variable header
		output.writeShort(topicLength);
		Pipe.readBytes(input, output, topicMeta, topicLength);
		
		if (packetId>=0) {
			output.writeShort(packetId);
		}						
		
		Pipe.readBytes(input, output, payloadMeta, payloadLength);
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
