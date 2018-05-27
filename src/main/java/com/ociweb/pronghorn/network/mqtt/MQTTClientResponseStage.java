package com.ociweb.pronghorn.network.mqtt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.ClientCoordinator;
import com.ociweb.pronghorn.network.schema.MQTTServerToClientSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.FragmentWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

/**
 * Performs MQTT client responses.
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class MQTTClientResponseStage extends PronghornStage {

	private static final Logger logger = LoggerFactory.getLogger(MQTTClientResponseStage.class);
	
	final Pipe<NetPayloadSchema>[] fromBroker;
	final Pipe<ReleaseSchema> ackReleaseForResponseParser;
	final Pipe<MQTTServerToClientSchema> out;

	/**
	 *
	 * @param gm
	 * @param ccm
	 * @param fromBroker _in_ Payload received from broker
	 * @param ackReleaseForResponseParser _out_ Writes release for the response parser
	 * @param out _out_ Outputs MQTTServerToClientSchema response
	 */
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
		
//		logger.info(PipeWriter.hasRoomForWrite(ackReleaseForResponseParser)+
//				           "  "+PipeWriter.hasRoomForWrite(out)+
//				           "  "+PipeReader.hasContentToRead(server));
		
		while (Pipe.hasRoomForWrite(ackReleaseForResponseParser) && 
			   Pipe.hasRoomForWrite(out) && 
			   Pipe.hasContentToRead(server)) {
			
			final int idx = Pipe.takeMsgIdx(server);

			if (NetPayloadSchema.MSG_PLAIN_210 == idx) {
				
				long connectionId = Pipe.takeLong(server);
				long arrivalTime = Pipe.takeLong(server);
				long netPosition = Pipe.takeLong(server);
				
				DataInputBlobReader<NetPayloadSchema> inputStream = Pipe.openInputStream(server); 

				byte commandByte = inputStream.readByte();

				switch (commandByte) {
					case (byte)0xD0: //PingResp
						
						Pipe.presumeRoomForWrite(out);					    
					 	FragmentWriter.writeL(out, MQTTServerToClientSchema.MSG_PINGRESP_13, 
					 			                   arrivalTime);
		
						break;
					case (byte)0xB0: //UnSubAck
						
						int len = inputStream.readByte();
						assert(2==len);					    
						
						Pipe.presumeRoomForWrite(out);
						FragmentWriter.writeLI(out, MQTTServerToClientSchema.MSG_UNSUBACK_11,
								arrivalTime,
								inputStream.readShort()
								);
				
											
						break;
					case (byte)0x90: //SubAck
						
						int len9 = inputStream.readByte();
					    assert(3==len9);					    
					
						Pipe.presumeRoomForWrite(out);
						
						FragmentWriter.writeLII(out, MQTTServerToClientSchema.MSG_SUBACK_9,
								arrivalTime,
								inputStream.readShort(),
								inputStream.readByte()
								);
							    
						break;
					case (byte)0x70: //PubComp
						
						int len7 = inputStream.readByte();
						assert(2==len7);					    
						
						Pipe.presumeRoomForWrite(out);
						
						FragmentWriter.writeLI(out, MQTTServerToClientSchema.MSG_PUBCOMP_7,
								arrivalTime,
								inputStream.readShort()
								);
												
						break;
					case (byte)0x40: //PubAck
											
						int len4 = inputStream.readByte();
						assert(2==len4);					    
					
						short packetId40 = inputStream.readShort();
						
						Pipe.presumeRoomForWrite(out);
						FragmentWriter.writeLI(out, MQTTServerToClientSchema.MSG_PUBACK_4, 
								              arrivalTime, 
								              packetId40);
								
						break;
					case (byte)0x50: //PubRec
						
						int len5 = inputStream.readByte();
						assert(true || 2==len5); //turned off until fuzz testing can build valid messages.					    
															
						Pipe.presumeRoomForWrite(out);
						FragmentWriter.writeLI(out, MQTTServerToClientSchema.MSG_PUBREC_5,
												arrivalTime, 
												inputStream.readShort()
												);
											    
						break;
					case (byte)0x62: //PubRel
						
						int len6 = inputStream.readByte();
						assert(2==len6);					    
											
						Pipe.presumeRoomForWrite(out);
						FragmentWriter.writeLI(out, MQTTServerToClientSchema.MSG_PUBREL_6,
								arrivalTime, 
								inputStream.readShort()
								);
						
						break;
					case (byte)0x20: //ConnAck
						
						int len2 = inputStream.readByte();
				 	    assert(2==len2);					    
					
				 	    int sessionPresentFlag = inputStream.readByte();						
				 	    int retCode = inputStream.readByte();
					
						Pipe.presumeRoomForWrite(out);
						FragmentWriter.writeLII(out, MQTTServerToClientSchema.MSG_CONNACK_2, 
													arrivalTime,
													sessionPresentFlag,
													retCode);

						break;
					default:
                        //0x3?  These are all Publish
						
						int cmd = commandByte>>4;
						if (3==cmd) {
							
							int dup    = 1&(commandByte>>3);
							int qos    = 3&(commandByte>>1);
							int retain = 1&commandByte;
										
							//unpack the length						
							int len3 = decodeLength(inputStream);							
							int topicLength = inputStream.readShort();
							assert(topicLength<out.maxVarLen) : "Outgoing pipe "+out+" is not large enough for topic of "+topicLength;
							len3-=2;

							if (topicLength>out.maxVarLen>>1) { //TODO: this is a hack, for bad packets we must return an error message.
								topicLength = out.maxVarLen>>1;
							}
							
							
							Pipe.presumeRoomForWrite(out);
						
							//  LIIIVIV
							Pipe.addMsgIdx(out, MQTTServerToClientSchema.MSG_PUBLISH_3);
							
							Pipe.addLongValue(arrivalTime, out);
							Pipe.addIntValue(qos, out);
							Pipe.addIntValue(retain, out);
							Pipe.addIntValue(dup, out);
							
							//MSG_PUBLISH_3_FIELD_TOPIC_23
							DataOutputBlobWriter<MQTTServerToClientSchema> writer = Pipe.openOutputStream(out);
							inputStream.readInto(writer, topicLength);
							DataOutputBlobWriter.closeLowLevelField(writer);
							len3-=topicLength;
							
							int packetId = 0;
							if (0!=qos) {
								packetId = inputStream.readShort();
								len3-=2;
							}
							assert(len3<out.maxVarLen) : "Outgoing pipe "+out+" is not large enough for payload of "+len3;
									
							Pipe.addIntValue(packetId, out);
														
							//payload
							writer = Pipe.openOutputStream(out);
		
							if (len3>out.maxVarLen>>1) { //TODO: this is a hack, for bad packets we must return an error message.
								len3 = out.maxVarLen>>1;
							}
							inputStream.readInto(writer, len3);
							DataOutputBlobWriter.closeLowLevelField(writer);
										
							Pipe.confirmLowLevelWrite(out, Pipe.sizeOf(MQTTServerToClientSchema.instance, MQTTServerToClientSchema.MSG_PUBLISH_3));							
							Pipe.publishWrites(out);
														
						} else {							
							logger.trace("ignored unrecognized command of {} ",cmd);
						}
				}
				
				releaseSocketData(connectionId, netPosition);
				
			} else {
				if (-1==idx) {
					requestShutdown();
					return;
				} else {
								
					if (NetPayloadSchema.MSG_DISCONNECT_203 == idx) {
						
						long connectionId = Pipe.takeLong(server);
						
						Pipe.presumeRoomForWrite(out);
						FragmentWriter.writeL(out, MQTTServerToClientSchema.MSG_DISCONNECT_14, System.currentTimeMillis());
							
					} else {
				
						if (NetPayloadSchema.MSG_BEGIN_208 == idx) {
							
							long sequienceNo = Pipe.takeInt(server);
							
						} else {
							logger.warn("support yet for NetPayloadSchema msg "+idx);
						}
					}
				}
			}
			
			Pipe.confirmLowLevelRead(server, Pipe.sizeOf(server, idx));			
			Pipe.releaseReadLock(server);
			
		}
	}

	private void releaseSocketData(long connectionId, long netPosition) {
		Pipe.presumeRoomForWrite(ackReleaseForResponseParser);
		FragmentWriter.writeLL(ackReleaseForResponseParser, 
				 ReleaseSchema.MSG_RELEASE_100, 
				 connectionId,
				 netPosition);
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
