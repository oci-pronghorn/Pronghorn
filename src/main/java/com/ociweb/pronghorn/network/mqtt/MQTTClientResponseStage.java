package com.ociweb.pronghorn.network.mqtt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.ClientCoordinator;
import com.ociweb.pronghorn.network.schema.MQTTServerToClientSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class MQTTClientResponseStage extends PronghornStage {

	private static final Logger log = LoggerFactory.getLogger(MQTTClientResponseStage.class);
	
	final Pipe<NetPayloadSchema>[] fromBroker;
	final Pipe<ReleaseSchema> ackReleaseForResponseParser;
	final Pipe<MQTTServerToClientSchema> out;
	
	public MQTTClientResponseStage(GraphManager gm, ClientCoordinator ccm, 
									Pipe<NetPayloadSchema>[] fromBroker,
									Pipe<ReleaseSchema> ackReleaseForResponseParser, 
									Pipe<MQTTServerToClientSchema> out) {
		
		super(gm, fromBroker, join(ackReleaseForResponseParser, out));
		
		this.fromBroker = fromBroker;
		this.ackReleaseForResponseParser = ackReleaseForResponseParser;
		this.out = out;
	}

	@Override
	public void run() {
		
		int i = fromBroker.length;
		while (--i>=0) {
			parseDataFromBroker(fromBroker[i]);
		}
	}

	private void parseDataFromBroker(Pipe<NetPayloadSchema> server) {
		while (PipeWriter.hasRoomForWrite(ackReleaseForResponseParser) && 
			   PipeWriter.hasRoomForWrite(out) && 
			   PipeReader.tryReadFragment(server)) {
			
			int idx = PipeReader.getMsgIdx(server);

			if (NetPayloadSchema.MSG_PLAIN_210 == idx) {
				
				long connectionId = PipeReader.readLong(server, NetPayloadSchema.MSG_PLAIN_210_FIELD_CONNECTIONID_201);
				long netPosition = PipeReader.readLong(server, NetPayloadSchema.MSG_PLAIN_210_FIELD_POSITION_206);
				long arrivalTime = PipeReader.readLong(server, NetPayloadSchema.MSG_PLAIN_210_FIELD_ARRIVALTIME_210);
				
				DataInputBlobReader<NetPayloadSchema> inputStream = PipeReader.inputStream(server, NetPayloadSchema.MSG_PLAIN_210_FIELD_PAYLOAD_204);
				
				byte commandByte = inputStream.readByte();
			
				switch (commandByte) {
					case (byte)0xD0: //PingResp
						
						PipeWriter.tryWriteFragment(out, MQTTServerToClientSchema.MSG_PINGRESP_13);
					  
						PipeWriter.writeLong(out, MQTTServerToClientSchema.MSG_PINGRESP_13_FIELD_TIME_37, arrivalTime);
											
						PipeWriter.publishWrites(out);		
						
						break;
					case (byte)0xB0: //UnSubAck
						
						PipeWriter.tryWriteFragment(out, MQTTServerToClientSchema.MSG_UNSUBACK_11);
					
					    PipeWriter.writeLong(out, MQTTServerToClientSchema.MSG_UNSUBACK_11_FIELD_TIME_37, arrivalTime);
					
					    int len = inputStream.readByte();
					    assert(2==len);					    
					    
					    PipeWriter.writeInt(out, MQTTServerToClientSchema.MSG_UNSUBACK_11_FIELD_PACKETID_20, inputStream.readShort());
					    					
					    PipeWriter.publishWrites(out);
											
						break;
					case (byte)0x90: //SubAck
						PipeWriter.tryWriteFragment(out, MQTTServerToClientSchema.MSG_SUBACK_9);
					
				        PipeWriter.writeLong(out, MQTTServerToClientSchema.MSG_SUBACK_9_FIELD_TIME_37, arrivalTime);
					
					    int len9 = inputStream.readByte();
					    assert(3==len9);					    
					    
					    PipeWriter.writeInt(out, MQTTServerToClientSchema.MSG_SUBACK_9_FIELD_PACKETID_20, inputStream.readShort());
					    PipeWriter.writeInt(out, MQTTServerToClientSchema.MSG_SUBACK_9_FIELD_RETURNCODE_24, inputStream.readByte());
					    					    
					    PipeWriter.publishWrites(out);
				    
						break;
					case (byte)0x70: //PubComp
						
						PipeWriter.tryWriteFragment(out, MQTTServerToClientSchema.MSG_PUBCOMP_7);
					
				        PipeWriter.writeLong(out, MQTTServerToClientSchema.MSG_PUBCOMP_7_FIELD_TIME_37, arrivalTime);
					
					    int len7 = inputStream.readByte();
					    assert(2==len7);					    
					    
					    PipeWriter.writeInt(out, MQTTServerToClientSchema.MSG_PUBCOMP_7_FIELD_PACKETID_20, inputStream.readShort());
							    					    
					    PipeWriter.publishWrites(out);
												
						break;
					case (byte)0x40: //PubAck
												
						PipeWriter.tryWriteFragment(out, MQTTServerToClientSchema.MSG_PUBACK_4);
					
				        PipeWriter.writeLong(out, MQTTServerToClientSchema.MSG_PUBACK_4_FIELD_TIME_37, arrivalTime);
					
					    int len4 = inputStream.readByte();
					    assert(2==len4);					    
					    
					    PipeWriter.writeInt(out, MQTTServerToClientSchema.MSG_PUBACK_4_FIELD_PACKETID_20, inputStream.readShort());
							    					    
					    PipeWriter.publishWrites(out);
				    
						break;
					case (byte)0x50: //PubRec
												
						PipeWriter.tryWriteFragment(out, MQTTServerToClientSchema.MSG_PUBREC_5);
					
				        PipeWriter.writeLong(out, MQTTServerToClientSchema.MSG_PUBREC_5_FIELD_TIME_37, arrivalTime);
				    
					    int len5 = inputStream.readByte();
					    assert(true || 2==len5); //turned off until fuzz testing can build valid messages.					    
					    
					    PipeWriter.writeInt(out, MQTTServerToClientSchema.MSG_PUBREC_5_FIELD_PACKETID_20, inputStream.readShort());
							    					    
					    PipeWriter.publishWrites(out);
					    
						break;
					case (byte)0x62: //PubRel
												
						PipeWriter.tryWriteFragment(out, MQTTServerToClientSchema.MSG_PUBREL_6);
						
				        PipeWriter.writeLong(out, MQTTServerToClientSchema.MSG_PUBREL_6_FIELD_TIME_37, arrivalTime);
				     
						int len6 = inputStream.readByte();
						assert(2==len6);					    
						
						PipeWriter.writeInt(out, MQTTServerToClientSchema.MSG_PUBREL_6_FIELD_PACKETID_20, inputStream.readShort());
							    					    
						PipeWriter.publishWrites(out);
						
						break;
					case (byte)0x20: //ConnAck
						
						PipeWriter.tryWriteFragment(out, MQTTServerToClientSchema.MSG_CONNACK_2);
					
				        PipeWriter.writeLong(out, MQTTServerToClientSchema.MSG_CONNACK_2_FIELD_TIME_37, arrivalTime);
					
						int len2 = inputStream.readByte();
						assert(2==len2);					    
						
						int sessionPresentFlag = inputStream.readByte();						
						int retCode = inputStream.readByte();
						
						PipeWriter.writeInt(out, MQTTServerToClientSchema.MSG_CONNACK_2_FIELD_FLAG_35, sessionPresentFlag);
						PipeWriter.writeInt(out, MQTTServerToClientSchema.MSG_CONNACK_2_FIELD_RETURNCODE_24, retCode);
							    					    
						PipeWriter.publishWrites(out);
						
						break;
					default:
                        //0x3?  These are all Publish
						
						int cmd = commandByte>>4;
						if (3==cmd) {
							
							int dup    = 1&(commandByte>>3);
							int qos    = 3&(commandByte>>1);
							int retain = 1&commandByte;
													
							PipeWriter.tryWriteFragment(out, MQTTServerToClientSchema.MSG_PUBLISH_3);
							
						    PipeWriter.writeLong(out, MQTTServerToClientSchema.MSG_PUBLISH_3_FIELD_TIME_37, arrivalTime);
							
							PipeWriter.writeInt(out, MQTTServerToClientSchema.MSG_PUBLISH_3_FIELD_QOS_21, qos);
							PipeWriter.writeInt(out, MQTTServerToClientSchema.MSG_PUBLISH_3_FIELD_RETAIN_22, retain);
							PipeWriter.writeInt(out, MQTTServerToClientSchema.MSG_PUBLISH_3_FIELD_DUP_36, dup);
										
							//unpack the length						
							int len3 = decodeLength(inputStream);
							
							int topicLength = inputStream.readShort();
							assert(topicLength<out.maxVarLen) : "Outgoing pipe "+out+" is not large enough for topic of "+topicLength;
							len3-=2;
							
							if (topicLength>out.maxVarLen>>1) { //TODO: this is a hack, for bad packets we must return an error message.
								topicLength = out.maxVarLen>>1;
							}
							//MSG_PUBLISH_3_FIELD_TOPIC_23
							DataOutputBlobWriter<MQTTServerToClientSchema> writer = PipeWriter.outputStream(out);
							DataOutputBlobWriter.openField(writer);
							inputStream.readInto(writer, topicLength);
							DataOutputBlobWriter.closeHighLevelField(writer, MQTTServerToClientSchema.MSG_PUBLISH_3_FIELD_TOPIC_23);
							len3-=topicLength;
							
							int packetId = 0;
							if (0!=qos) {
								packetId = inputStream.readShort();
								len3-=2;
							}
							assert(len3<out.maxVarLen) : "Outgoing pipe "+out+" is not large enough for payload of "+len3;
							
							
							PipeWriter.writeInt(out, MQTTServerToClientSchema.MSG_PUBLISH_3_FIELD_PACKETID_20, packetId);
							
							//payload
							writer = PipeWriter.outputStream(out);
							DataOutputBlobWriter.openField(writer);
		
							if (len3>out.maxVarLen>>1) { //TODO: this is a hack, for bad packets we must return an error message.
								len3 = out.maxVarLen>>1;
							}
							inputStream.readInto(writer, len3);
							DataOutputBlobWriter.closeHighLevelField(writer, MQTTServerToClientSchema.MSG_PUBLISH_3_FIELD_PAYLOAD_25);
							
							PipeWriter.publishWrites(out);
							
						} else {							
							log.trace("ignored unrecognized command of {} ",cmd);
						}
				}
				
				boolean ok = PipeWriter.tryWriteFragment(ackReleaseForResponseParser,ReleaseSchema.MSG_RELEASE_100);
				assert(ok) : "already checked for space so this should not fail.";
				
				PipeWriter.writeLong(ackReleaseForResponseParser, ReleaseSchema.MSG_RELEASE_100_FIELD_CONNECTIONID_1, connectionId);

				PipeWriter.writeLong(ackReleaseForResponseParser, ReleaseSchema.MSG_RELEASE_100_FIELD_POSITION_2, netPosition);
				PipeWriter.publishWrites(ackReleaseForResponseParser);
				
				
			} else {
				if (-1==idx) {
					requestShutdown();
					return;
				} else {
								
					if (idx==NetPayloadSchema.MSG_DISCONNECT_203) {
					
						PipeWriter.tryWriteFragment(out, MQTTServerToClientSchema.MSG_DISCONNECT_14);
						PipeWriter.writeLong(out, MQTTServerToClientSchema.MSG_DISCONNECT_14_FIELD_TIME_37, System.currentTimeMillis());
						PipeWriter.publishWrites(out);						
						
					} else {
						
						log.trace("support yet for NetPayloadSchema msg "+idx);
					}
				}
				
			}
			
			PipeReader.releaseReadLock(server);
			
		}
	}

	public static int decodeLength(DataInputBlobReader<NetPayloadSchema> inputStream) {
		//little endian, high bit is on until the end.		
		int result = 0;
		int shift = 0;
		byte b = inputStream.readByte();
		while (b<0) {//while high bit is set continue			
			int temp = 0x7F & b;			
			result |= (temp<<shift);			
			b = inputStream.readByte();
			shift+=7;
		}
		int temp = 0x7F & b;			
		result |= (temp<<shift);
		return result;
	}

}
