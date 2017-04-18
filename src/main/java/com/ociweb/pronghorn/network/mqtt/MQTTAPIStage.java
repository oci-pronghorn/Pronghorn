package com.ociweb.pronghorn.network.mqtt;

import com.ociweb.pronghorn.network.schema.MQTTConnectionInSchema;
import com.ociweb.pronghorn.network.schema.MQTTConnectionOutSchema;
import com.ociweb.pronghorn.network.schema.MQTTIdRangeSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class MQTTAPIStage extends PronghornStage {

	private final Pipe<MQTTConnectionOutSchema> fromBroker; //directly populated by external method calls, eg unknown thread
	private final Pipe<MQTTIdRangeSchema> idGenIn; //used by same external thread
	private final Pipe<MQTTConnectionInSchema> toBroker;
	 	
	private int nextFreePacketId = -1;
	private int nextFreePacketIdLimit = -1;
	private final int ttlSec;
	
	public final byte[] clientId = "somename for this client in UTF8".getBytes(); //same for entire run

	/**
  	 * This first implementation is kept simple until we get a working project.
  	 * 
  	 * The API may need some adjustments based on use cases.
  	 * 
  	 */
	
	private final int sizeOfPacketIdFragment;
	private final int sizeOfPubRel;
	private static final int theOneMsg = 0;// there is only 1 message supported by this stage
	
	
	protected MQTTAPIStage(GraphManager gm, Pipe<MQTTIdRangeSchema> idGenIn, Pipe<MQTTConnectionOutSchema> fromBroker, Pipe<MQTTConnectionInSchema> toBroker, int ttlSec) {
		super(gm, new Pipe[]{idGenIn,fromBroker}, toBroker);
	
		this.idGenIn = idGenIn;
		this.fromBroker = fromBroker;
		this.toBroker = toBroker;
		
		this.ttlSec = ttlSec;
			  	
        this.sizeOfPacketIdFragment = Pipe.from(idGenIn).fragDataSize[theOneMsg];
        this.sizeOfPubRel = Pipe.from(toBroker).fragDataSize[MQTTConnectionInSchema.MSG_PUBREL_9];
        
        //add one more ring buffer so apps can write directly to it since this stage needs to copy from something.
        //this makes testing much easier, it makes integration tighter
        //it may add a copy?
        
        //must be set so this stage will get shut down and ignore the fact that is has un-consumed messages coming in 
        GraphManager.addNota(gm,GraphManager.PRODUCER, GraphManager.PRODUCER, this);
        
	}
	
	

	@Override
	public void run() {
		 
	   
		while ( PipeWriter.hasRoomForFragmentOfSize(toBroker, sizeOfPubRel) && PipeReader.tryReadFragment(fromBroker)) {	
			int msgIdx = PipeReader.getMsgIdx(fromBroker);
			
			System.out.println("got message: "+msgIdx);
			
			int packetId;
			switch(msgIdx) {
				case MQTTConnectionOutSchema.MSG_CONNACKOK_20:
					newConnection();
				break;	
				case MQTTConnectionOutSchema.MSG_CONNACKID_22:
				case MQTTConnectionOutSchema.MSG_CONNACKAUTH_25:
				case MQTTConnectionOutSchema.MSG_CONNACKPROTO_21:
				case MQTTConnectionOutSchema.MSG_CONNACKSERVER_23:
				case MQTTConnectionOutSchema.MSG_CONNACKUSER_24:
					newConnectionError(msgIdx);
				break;	
				case MQTTConnectionOutSchema.MSG_PUBREL_9:
				    packetId = PipeReader.readInt(fromBroker, MQTTConnectionOutSchema.MSG_PUBREL_9_FIELD_PACKETID_200);
				    //subscriber logic
				    //TODO: now send pubcomp message to be sent 
				    
				    
				break;
				case MQTTConnectionOutSchema.MSG_PUBACK_6:
				    packetId = PipeReader.readInt(fromBroker, MQTTConnectionOutSchema.MSG_PUBACK_6_FIELD_PACKETID_200);
				    //System.out.println("ack packet "+packetId+" "+fromCon);
				    ackReceived1(packetId);
				    
				break;    
				case MQTTConnectionOutSchema.MSG_PUBREC_7:
				    System.out.println("ZZZZZZZZZZZZZZZZZZZZZZZZZZZzz  Got PubRec fromserver ");
				    
				    packetId = PipeReader.readInt(fromBroker, MQTTConnectionOutSchema.MSG_PUBREC_7_FIELD_PACKETID_200);
				    System.out.println("for packet:"+packetId);
				    
				    ackReceived2(packetId);
				    
				    if (!PipeWriter.tryWriteFragment(toBroker, MQTTConnectionInSchema.MSG_PUBREL_9)) {
				       throw new UnsupportedOperationException("Expected room in pipe due to the hasRoomForFragmentOfSize check."); 
				    }
				    
				    PipeWriter.writeInt(toBroker,  MQTTConnectionInSchema.MSG_PUBREL_9_FIELD_PACKETID_200, packetId);
				   
		            final int bytePos = Pipe.getWorkingBlobHeadPosition(toBroker);
		            byte[] byteBuffer = Pipe.blob(toBroker);
		            int byteMask = Pipe.blobMask(toBroker);
		            
				    int len = MQTTEncoder.buildPubRelPacket(bytePos, byteBuffer, byteMask, packetId);
	                             
		            PipeWriter.writeSpecialBytesPosAndLen(toBroker, MQTTConnectionInSchema.MSG_PUBREL_9_FIELD_PACKETDATA_300, len, bytePos);
		            
		            PipeWriter.publishWrites(toBroker);
				    
				break;
				default:
				    throw new UnsupportedOperationException("Unknown Mesage: "+msgIdx);
					
			}
			PipeReader.releaseReadLock(fromBroker);
		}
		businessLogic();
	}
	
	public void newConnection() {
		
	}
	
	
    protected void ackReceived1(int packetId) {
        ackReceived(packetId,1);
    }
    
    protected void ackReceived2(int packetId) {
        ackReceived(packetId,2);
    }    
	
    protected void ackReceived(int packetId, int qos) {
	    
	}
	
	public void newConnectionError(int err) {		
	}	
	
	public void businessLogic() {
	}
	
	
	//caller must pre-encode these fields so they can be re-used for mutiple calls?
	//but this connect would be called rarely?
	
	//TODO: these methods may be extracted because they need not be part of an actor, 
	//NOTE: these methods are not thread safe and are intended for sequential use.
	
	public boolean requestConnect(CharSequence url, int conFlags, byte[] willTopic, int willTopicIdx, int willTopicLength, int willTopicMask,  
	                                  byte[] willMessageBytes, int willMessageBytesIdx, int willMessageBytesLength, int willMessageBytesMask,
	                                  byte[] username, byte[] passwordBytes) {

		if (PipeWriter.tryWriteFragment(toBroker, MQTTConnectionInSchema.MSG_CONNECT_2)) {
			
			PipeWriter.writeASCII(toBroker, MQTTConnectionInSchema.MSG_CONNECT_2_FIELD_URL_400, url);
						
			//this is the high level API however we are writing bytes to to the end of the unstructured buffer.
			final int bytePos = Pipe.getWorkingBlobHeadPosition(toBroker);
			byte[] byteBuffer = Pipe.blob(toBroker);
			int byteMask = Pipe.blobMask(toBroker);
						
			int len = MQTTEncoder.buildConnectPacket(bytePos, byteBuffer, byteMask, ttlSec, conFlags, 
					                                 clientId, 0 , clientId.length, 0xFFFF,
					                                 willTopic, willTopicIdx , willTopicLength, willTopicMask,
					                                 willMessageBytes, willMessageBytesIdx, willMessageBytesLength, willMessageBytesMask,
					                                 username, 0, username.length, 0xFFFF, //TODO: add rest of fields
					                                 passwordBytes, 0, passwordBytes.length, 0xFFFF);//TODO: add rest of fields
			assert(len>0);
			PipeWriter.writeSpecialBytesPosAndLen(toBroker, MQTTConnectionInSchema.MSG_CONNECT_2_FIELD_PACKETDATA_300, len, bytePos);
			
			PipeWriter.publishWrites(toBroker);
			return true;
		} else {
			return false;
		}

	}

	public boolean requestDisconnect() {
		
	//    System.err.println("AAA :"+RingBuffer.bytesWriteBase(toCon));
	    
		if (PipeWriter.tryWriteFragment(toBroker, MQTTConnectionInSchema.MSG_DISCONNECT_5)) {
		//    System.err.println("BBB :"+RingBuffer.bytesWriteBase(toCon));
			PipeWriter.publishWrites(toBroker);
	//		 System.err.println("CCCC :"+RingBuffer.bytesWriteBase(toCon));
			return true;
		} else {
			return false;
		}		
				
	}

	public int requestPublish(byte[] topic, int topicIdx, int topicLength, int topicMask, 
			                   int qualityOfService, int retain, 
			                   byte[] payload, int payloadIdx, int payloadLength, int payloadMask) {
				
		if (nextFreePacketId >= nextFreePacketIdLimit) {
			//get next range
			if (Pipe.hasContentToRead(idGenIn, sizeOfPacketIdFragment)) {				
				loadNextPacketIdRange();				
			} else {
			    System.err.println("no id");
				return -1;
			}	
		}
		////
		
		if (PipeWriter.tryWriteFragment(toBroker, MQTTConnectionInSchema.MSG_PUBLISH_1)) {
						
		    
			PipeWriter.writeInt(toBroker, MQTTConnectionInSchema.MSG_PUBLISH_1_FIELD_QOS_100, qualityOfService);
			
			int localPacketId = (0==qualityOfService) ? -1 : nextFreePacketId++;
						
			PipeWriter.writeInt(toBroker, MQTTConnectionInSchema.MSG_PUBLISH_1_FIELD_PACKETID_200, localPacketId);
						
			final int bytePos = Pipe.getBlobWorkingHeadPosition(toBroker);
			byte[] byteBuffer = Pipe.byteBuffer(toBroker);
			int byteMask = Pipe.blobMask(toBroker);
			
			int len = MQTTEncoder.buildPublishPacket(bytePos, byteBuffer, byteMask, qualityOfService, retain, 
					                topic, topicIdx, topicLength, topicMask, 
					                payload, payloadIdx, payloadLength, payloadMask, localPacketId);
			PipeWriter.writeSpecialBytesPosAndLen(toBroker, MQTTConnectionInSchema.MSG_PUBLISH_1_FIELD_PACKETDATA_300, len, bytePos);
				
			PipeWriter.publishWrites(toBroker);

			return localPacketId<0 ? 0 : localPacketId;//TODO: we have no id for qos 0 this is dirty.
		} else {
		    System.err.println("no room to write");
			return -1;
		}
				
	}


	private void loadNextPacketIdRange() {
		int msgIdx = Pipe.takeMsgIdx(idGenIn);
		assert(theOneMsg == msgIdx);
		
		int range = Pipe.takeValue(idGenIn);
		nextFreePacketId = 0xFFFF&range;
		nextFreePacketIdLimit = 0xFFFF&(range>>16); 
						
		Pipe.releaseReads(idGenIn);
		Pipe.confirmLowLevelRead(idGenIn, sizeOfPacketIdFragment);
	}
	
}
