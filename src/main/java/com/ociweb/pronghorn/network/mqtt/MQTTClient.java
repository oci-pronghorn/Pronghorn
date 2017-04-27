package com.ociweb.pronghorn.network.mqtt;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.network.schema.MQTTClientRequestSchema;
import com.ociweb.pronghorn.network.schema.MQTTClientResponseSchema;
import com.ociweb.pronghorn.network.schema.MQTTClientToServerSchema;
import com.ociweb.pronghorn.network.schema.MQTTIdRangeSchema;
import com.ociweb.pronghorn.network.schema.MQTTServerToClientSchema;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class MQTTClient extends PronghornStage {

	public static final int CON_ACK_ERR_FLAG = 1<<8;
	public static final int SUB_ACK_ERR_FLAG = 1<<9;
	private final Pipe<MQTTClientRequestSchema>  clientRequest;
	private final Pipe<MQTTIdRangeSchema>        idGenNew;
	private final Pipe<MQTTServerToClientSchema> serverToClient;     
	private final Pipe<MQTTClientResponseSchema> clientResponse;
	private final Pipe<MQTTIdRangeSchema>        idGenOld;
	private final Pipe<MQTTClientToServerSchema> clientToServer;    
	
	private IdGenCache genCache;
	private long mostRecentTime;
	
	public MQTTClient(GraphManager gm, 
			          Pipe<MQTTClientRequestSchema> clientRequest,
			          Pipe<MQTTIdRangeSchema> idGenNew,
			          Pipe<MQTTServerToClientSchema> serverToClient, 
			          
			          Pipe<MQTTClientResponseSchema> clientResponse,
			          Pipe<MQTTIdRangeSchema> idGenOld, 
			          Pipe<MQTTClientToServerSchema> clientToServer) {
		
		super(gm, join(clientRequest,idGenNew,serverToClient), join(clientResponse,idGenOld,clientToServer) );
		
		this.clientRequest=clientRequest;
		this.idGenNew=idGenNew;
		this.serverToClient=serverToClient;
		
		this.clientResponse=clientResponse;
		this.idGenOld=idGenOld;
		this.clientToServer=clientToServer;
				
		//TODO: add feature,  one more pipe back for ack.? Need custom schema.
		
	}
	
	ByteBuffer[] inFlight;//re-send until cleared.
	
	@Override
	public void startup() {

		genCache = new IdGenCache();		
		
		int inFlightCount = 10;
		inFlight = new ByteBuffer[inFlightCount];
		int i = inFlightCount;
		while (--i>=0) {
			inFlight[i] = ByteBuffer.allocate(clientToServer.maxVarLen);
		}
		
		
	}
	
	
	@Override
	public void run() {
		
		////////////////////////
		//read server responses
		///////////////////////
		processServerResponses();		

		//////////////////////////
		//read new client requests
		/////////////////////////
		processClientRequests();		
			

	}

	public void processClientRequests() {
		while(  
				MQTTEncoder.hasPacketId(genCache, idGenNew) //only process if we have new PacketIds ready
				&& PipeWriter.hasRoomForWrite(clientToServer) //only process if we have room to write
				&& PipeReader.tryReadFragment(clientRequest)  ) {
			
			int msgIdx = PipeReader.getMsgIdx(clientRequest);
			switch(msgIdx) {
						
				case MQTTClientRequestSchema.MSG_BROKERCONFIG_100:			
					boolean ok = PipeWriter.tryWriteFragment(clientToServer, MQTTClientToServerSchema.MSG_BROKERHOST_100);
					assert(ok);
					
					PipeReader.copyBytes(clientRequest, clientToServer, 
				             MQTTClientRequestSchema.MSG_BROKERCONFIG_100_FIELD_HOST_26, 
				             MQTTClientToServerSchema.MSG_BROKERHOST_100_FIELD_HOST_26);
			
					PipeReader.copyInt(clientRequest, clientToServer, 
							  MQTTClientRequestSchema.MSG_BROKERCONFIG_100_FIELD_PORT_27, MQTTClientToServerSchema.MSG_BROKERHOST_100_FIELD_PORT_27);
					
			        PipeWriter.publishWrites(clientToServer); 
			        
					break;
				case MQTTClientRequestSchema.MSG_CONNECT_1:
							
					boolean ok1 = PipeWriter.tryWriteFragment(clientToServer, MQTTClientToServerSchema.MSG_CONNECT_1);
					assert(ok1);
										
					PipeReader.copyInt(clientRequest, clientToServer, 
							  MQTTClientRequestSchema.MSG_CONNECT_1_FIELD_KEEPALIVESEC_28,
							  MQTTClientToServerSchema.MSG_CONNECT_1_FIELD_KEEPALIVESEC_28);
					
					PipeReader.copyInt(clientRequest, clientToServer, 
							  MQTTClientRequestSchema.MSG_CONNECT_1_FIELD_FLAGS_29,
							  MQTTClientToServerSchema.MSG_CONNECT_1_FIELD_FLAGS_29);
					
					PipeWriter.writeLong(clientToServer, 
							  MQTTClientToServerSchema.MSG_CONNECT_1_FIELD_TIME_37, 
							  System.currentTimeMillis());
										
					PipeReader.copyBytes(clientRequest, clientToServer, 
				             MQTTClientRequestSchema.MSG_CONNECT_1_FIELD_CLIENTID_30, 
				             MQTTClientToServerSchema.MSG_CONNECT_1_FIELD_CLIENTID_30);
										
					PipeReader.copyBytes(clientRequest, clientToServer, 
				             MQTTClientRequestSchema.MSG_CONNECT_1_FIELD_WILLTOPIC_31, 
				             MQTTClientToServerSchema.MSG_CONNECT_1_FIELD_WILLTOPIC_31);
					
					PipeReader.copyBytes(clientRequest, clientToServer, 
				             MQTTClientRequestSchema.MSG_CONNECT_1_FIELD_WILLPAYLOAD_32, 
				             MQTTClientToServerSchema.MSG_CONNECT_1_FIELD_WILLPAYLOAD_32);
					
					PipeReader.copyBytes(clientRequest, clientToServer, 
				             MQTTClientRequestSchema.MSG_CONNECT_1_FIELD_USER_33, 
				             MQTTClientToServerSchema.MSG_CONNECT_1_FIELD_USER_33);			
					
					PipeReader.copyBytes(clientRequest, clientToServer, 
				             MQTTClientRequestSchema.MSG_CONNECT_1_FIELD_PASS_34, 
				             MQTTClientToServerSchema.MSG_CONNECT_1_FIELD_PASS_34);	
					
					 PipeWriter.publishWrites(clientToServer); 
				
					break;			
				case MQTTClientRequestSchema.MSG_PUBLISH_3:
		
					boolean ok3 = PipeWriter.tryWriteFragment(clientToServer, MQTTClientToServerSchema.MSG_PUBLISH_3);
					assert(ok3);				
					
					int valueQoS = PipeReader.readInt(clientRequest, MQTTClientRequestSchema.MSG_PUBLISH_3_FIELD_QOS_21);
					PipeWriter.writeInt(clientToServer, 
							MQTTClientToServerSchema.MSG_PUBLISH_3_FIELD_QOS_21, 
							valueQoS);
										
					PipeWriter.writeLong(clientToServer, 
							  MQTTClientToServerSchema.MSG_PUBLISH_3_FIELD_TIME_37, 
							  System.currentTimeMillis());				
					
					PipeReader.copyBytes(clientRequest, clientToServer, 
							MQTTClientRequestSchema.MSG_PUBLISH_3_FIELD_TOPIC_23, 
							MQTTClientToServerSchema.MSG_PUBLISH_3_FIELD_TOPIC_23);
					
					PipeReader.copyBytes(clientRequest, clientToServer, 
							MQTTClientRequestSchema.MSG_PUBLISH_3_FIELD_PAYLOAD_25, 
							MQTTClientToServerSchema.MSG_PUBLISH_3_FIELD_PAYLOAD_25);
									    
					PipeReader.copyInt(clientRequest, clientToServer, 
							  MQTTClientRequestSchema.MSG_PUBLISH_3_FIELD_RETAIN_22,
							  MQTTClientToServerSchema.MSG_PUBLISH_3_FIELD_RETAIN_22);
				    					
					int packetId = -1;
					if (valueQoS != 0) {						
						//only consume a packetId for QoS 1 or 2.
						packetId = IdGenCache.nextPacketId(genCache);					
					}
					PipeWriter.writeInt(clientToServer, MQTTClientToServerSchema.MSG_PUBLISH_3_FIELD_PACKETID_20, packetId);
					
					
					PipeWriter.publishWrites(clientToServer);					
		
					break;				
				case MQTTClientRequestSchema.MSG_SUBSCRIBE_8:
										
					PipeWriter.tryWriteFragment(clientToServer, MQTTClientToServerSchema.MSG_SUBSCRIBE_8);
					
					PipeWriter.writeLong(clientToServer, 
							  MQTTClientToServerSchema.MSG_SUBSCRIBE_8_FIELD_TIME_37, 
							  System.currentTimeMillis());
					
					int subQoS = PipeReader.readInt(clientRequest, MQTTClientRequestSchema.MSG_SUBSCRIBE_8_FIELD_QOS_21);					
									
					PipeWriter.writeInt(clientToServer, MQTTClientToServerSchema.MSG_SUBSCRIBE_8_FIELD_PACKETID_20, IdGenCache.nextPacketId(genCache));	
						
					PipeWriter.writeInt(clientToServer, MQTTClientToServerSchema.MSG_SUBSCRIBE_8_FIELD_QOS_21, subQoS);
										
					PipeReader.copyBytes(clientRequest, clientToServer, 
				             MQTTClientRequestSchema.MSG_SUBSCRIBE_8_FIELD_TOPIC_23, 
				             MQTTClientToServerSchema.MSG_SUBSCRIBE_8_FIELD_TOPIC_23);
					
					
					break;				
				case MQTTClientRequestSchema.MSG_UNSUBSCRIBE_10:
					
					PipeWriter.tryWriteFragment(clientToServer, MQTTClientToServerSchema.MSG_UNSUBSCRIBE_10);
					
					PipeWriter.writeLong(clientToServer, 
							  MQTTClientToServerSchema.MSG_UNSUBSCRIBE_10_FIELD_TIME_37, 
							  System.currentTimeMillis());
					
					PipeWriter.writeInt(clientToServer, MQTTClientToServerSchema.MSG_UNSUBSCRIBE_10_FIELD_PACKETID_20, IdGenCache.nextPacketId(genCache));
										
					PipeReader.copyBytes(clientRequest, clientToServer, 
				             MQTTClientRequestSchema.MSG_UNSUBSCRIBE_10_FIELD_TOPIC_23, 
				             MQTTClientToServerSchema.MSG_UNSUBSCRIBE_10_FIELD_TOPIC_23);
					
					break;
				case -1:
					requestShutdown();
					break;
			}
			PipeReader.releaseReadLock(clientRequest);
			
		}
	}

	public void processServerResponses() {
		
//		System.err.println(PipeWriter.hasRoomForWrite(idGenOld) 
//			  && PipeWriter.hasRoomForWrite(clientToServer)
//			  && PipeWriter.hasRoomForWrite(clientResponse));
		
		
		while(PipeWriter.hasRoomForWrite(idGenOld) 
			  && PipeWriter.hasRoomForWrite(clientToServer)
			  && PipeWriter.hasRoomForWrite(clientResponse)
			  && PipeReader.tryReadFragment(serverToClient)) {
			
			int msgIdx = PipeReader.getMsgIdx(serverToClient);

			switch(msgIdx) {
				case MQTTServerToClientSchema.MSG_DISCONNECT_14:
					//NOTE: do not need do anything now, the connection will be re-attached.
				break;
				case MQTTServerToClientSchema.MSG_CONNACK_2:
				
					mostRecentTime = PipeReader.readLong(serverToClient, MQTTServerToClientSchema.MSG_CONNACK_2_FIELD_TIME_37);
					int sessionPresentFlag = PipeReader.readInt(serverToClient, MQTTServerToClientSchema.MSG_CONNACK_2_FIELD_FLAG_35);
					//TODO: what to do with session bit??
					
					int conAckReturnCode = PipeReader.readInt(serverToClient, MQTTServerToClientSchema.MSG_CONNACK_2_FIELD_RETURNCODE_24);
					
					if (0!=conAckReturnCode) {
						PipeWriter.tryWriteFragment(clientResponse, MQTTClientResponseSchema.MSG_ERROR_4);
	
						int fieldErrorCode = CON_ACK_ERR_FLAG | conAckReturnCode;
						CharSequence fieldErrorText = "";
						switch(conAckReturnCode) {
							case 1:
								fieldErrorText = "The Server does not support the level of the MQTT protocol requested by the Client";
								break;
							case 2:
								fieldErrorText = "The Client identifier is correct UTF-8 but not allowed by the Server";
								break;
							case 3:
								fieldErrorText = "The Network Connection has been made but the MQTT service is unavailable";
								break;
							case 4:
								fieldErrorText = "The data in the user name or password is malformed";
								break;
							case 5:
								fieldErrorText = "The Client is not authorized to connect";
								break;
							default:
								fieldErrorText = "Unknown connection error";
						}
						
						PipeWriter.writeInt(clientResponse,MQTTClientResponseSchema.MSG_ERROR_4_FIELD_ERRORCODE_41, fieldErrorCode);
						PipeWriter.writeUTF8(clientResponse,MQTTClientResponseSchema.MSG_ERROR_4_FIELD_ERRORTEXT_42, fieldErrorText);
						PipeWriter.publishWrites(clientResponse);
					}

					break;
				case MQTTServerToClientSchema.MSG_PINGRESP_13:
					
					mostRecentTime = PipeReader.readLong(serverToClient, MQTTServerToClientSchema.MSG_PINGRESP_13_FIELD_TIME_37);
															
					break;
				case MQTTServerToClientSchema.MSG_PUBACK_4:
					//clear the QoS 1 publishes so we stop re-sending these messages
					mostRecentTime = PipeReader.readLong(serverToClient, MQTTServerToClientSchema.MSG_PUBACK_4_FIELD_TIME_37);
					int packetId4 = PipeReader.readInt(serverToClient, MQTTServerToClientSchema.MSG_PUBACK_4_FIELD_PACKETID_20);
					
				    stopReSendingMessage(packetId4);					
					
					////////////////////
					//now release the packet Id
					////////////////////
					boolean ok = PipeWriter.tryWriteFragment(idGenOld, MQTTIdRangeSchema.MSG_IDRANGE_1);	
					assert(ok);
					PipeWriter.writeInt(idGenOld, MQTTIdRangeSchema.MSG_IDRANGE_1_FIELD_RANGE_100, IdGenStage.buildRange(packetId4, packetId4+1));
					PipeWriter.publishWrites(idGenOld);
					
					break;
				case MQTTServerToClientSchema.MSG_PUBCOMP_7:
					//last stop of QoS 2
					mostRecentTime = PipeReader.readLong(serverToClient, MQTTServerToClientSchema.MSG_PUBCOMP_7_FIELD_TIME_37);
					int packetId7 = PipeReader.readInt(serverToClient, MQTTServerToClientSchema.MSG_PUBCOMP_7_FIELD_PACKETID_20);
							
				    stopReSendingMessage(packetId7);
					
					////////////////////
					//now release the packet Id
					////////////////////
					boolean ok5 = PipeWriter.tryWriteFragment(idGenOld, MQTTIdRangeSchema.MSG_IDRANGE_1);	
					assert(ok5);
					PipeWriter.writeInt(idGenOld, MQTTIdRangeSchema.MSG_IDRANGE_1_FIELD_RANGE_100, IdGenStage.buildRange(packetId7, packetId7+1));
					PipeWriter.publishWrites(idGenOld);					
					
					break;
				case MQTTServerToClientSchema.MSG_PUBLISH_3:
					//data from our subscriptions.
					
					mostRecentTime = PipeReader.readLong(serverToClient, MQTTServerToClientSchema.MSG_PUBLISH_3_FIELD_TIME_37);
					
					int serverSidePacketId = IdGenStage.IS_REMOTE_BIT 
							                 | PipeReader.readInt(serverToClient, MQTTServerToClientSchema.MSG_PUBLISH_3_FIELD_PACKETID_20);
					
					int retain3 = PipeReader.readInt(serverToClient, MQTTServerToClientSchema.MSG_PUBLISH_3_FIELD_RETAIN_22);
					
					int dup3 = PipeReader.readInt(serverToClient, MQTTServerToClientSchema.MSG_PUBLISH_3_FIELD_DUP_36);
					int qos3 = PipeReader.readInt(serverToClient, MQTTServerToClientSchema.MSG_PUBLISH_3_FIELD_QOS_21);
					
					DataOutputBlobWriter<?> target3 = null;
					PipeReader.readBytes(serverToClient, MQTTServerToClientSchema.MSG_PUBLISH_3_FIELD_TOPIC_23,target3);
					PipeReader.readBytes(serverToClient, MQTTServerToClientSchema.MSG_PUBLISH_3_FIELD_PAYLOAD_25,target3);
					
					
					PipeWriter.tryWriteFragment(clientResponse, MQTTClientResponseSchema.MSG_MESSAGE_3);
					
					PipeWriter.writeInt(clientResponse, MQTTClientResponseSchema.MSG_MESSAGE_3_FIELD_QOS_21, qos3);
					PipeWriter.writeInt(clientResponse, MQTTClientResponseSchema.MSG_MESSAGE_3_FIELD_DUP_36, dup3);
					PipeWriter.writeInt(clientResponse, MQTTClientResponseSchema.MSG_MESSAGE_3_FIELD_RETAIN_22, retain3);
					
					PipeReader.copyBytes(serverToClient, clientResponse,
							MQTTServerToClientSchema.MSG_PUBLISH_3_FIELD_TOPIC_23, 
							MQTTClientResponseSchema.MSG_MESSAGE_3_FIELD_TOPIC_23);
					
					PipeReader.copyBytes(serverToClient, clientResponse,
							MQTTServerToClientSchema.MSG_PUBLISH_3_FIELD_PAYLOAD_25, 
							MQTTClientResponseSchema.MSG_MESSAGE_3_FIELD_PAYLOAD_25);
					
					PipeWriter.publishWrites(clientResponse);
					
					if (0!=qos3) {
						
						if (1==qos3) {		//send pubAck for 1
							PipeWriter.tryWriteFragment(clientToServer, MQTTClientToServerSchema.MSG_PUBACK_4);
							PipeWriter.writeInt(clientToServer, MQTTClientToServerSchema.MSG_PUBACK_4_FIELD_PACKETID_20, serverSidePacketId);
							PipeWriter.writeLong(clientToServer, MQTTClientToServerSchema.MSG_PUBACK_4_FIELD_TIME_37, mostRecentTime);
							PipeWriter.publishWrites(clientToServer);						
						} else if (2==qos3) {
							PipeWriter.tryWriteFragment(clientToServer, MQTTClientToServerSchema.MSG_PUBREC_5);
							PipeWriter.writeInt(clientToServer, MQTTClientToServerSchema.MSG_PUBREC_5_FIELD_PACKETID_20, serverSidePacketId);
							PipeWriter.writeLong(clientToServer, MQTTClientToServerSchema.MSG_PUBREC_5_FIELD_TIME_37, mostRecentTime);
							PipeWriter.publishWrites(clientToServer);							
						}
					}
					break;
				case MQTTServerToClientSchema.MSG_PUBREC_5:
					//for QoS 2 publish, now release the message
					
					mostRecentTime = PipeReader.readLong(serverToClient, MQTTServerToClientSchema.MSG_PUBREC_5_FIELD_TIME_37);
					int packetId5 = PipeReader.readInt(serverToClient, MQTTServerToClientSchema.MSG_PUBREC_5_FIELD_PACKETID_20);

					//////////////////////
					//send pubrel and stop re-sending the message
					//////////////////////
					PipeWriter.tryWriteFragment(clientToServer, MQTTClientToServerSchema.MSG_PUBREL_6);
					PipeWriter.writeLong(clientToServer, MQTTClientToServerSchema.MSG_PUBREL_6_FIELD_TIME_37, mostRecentTime);
					PipeWriter.writeLong(clientToServer, MQTTClientToServerSchema.MSG_PUBREL_6_FIELD_PACKETID_20, packetId5);
					PipeWriter.publishWrites(clientToServer);
					
					break;
				case MQTTServerToClientSchema.MSG_PUBREL_6:
						
					mostRecentTime = PipeReader.readLong(serverToClient, MQTTServerToClientSchema.MSG_PUBREL_6_FIELD_TIME_37);
					int serverSidePacketId6 = IdGenStage.IS_REMOTE_BIT 
											  | PipeReader.readInt(serverToClient, MQTTServerToClientSchema.MSG_PUBREL_6_FIELD_PACKETID_20);
										
					PipeWriter.tryWriteFragment(clientToServer, MQTTClientToServerSchema.MSG_PUBCOMP_7);
					PipeWriter.writeLong(clientToServer, MQTTClientToServerSchema.MSG_PUBCOMP_7_FIELD_TIME_37, mostRecentTime);
					PipeWriter.writeLong(clientToServer, MQTTClientToServerSchema.MSG_PUBCOMP_7_FIELD_PACKETID_20, serverSidePacketId6);
					PipeWriter.publishWrites(clientToServer);
					
					break;
				case MQTTServerToClientSchema.MSG_SUBACK_9:
					
					mostRecentTime = PipeReader.readLong(serverToClient, MQTTServerToClientSchema.MSG_SUBACK_9_FIELD_TIME_37);
					int packetId9 = PipeReader.readInt(serverToClient, MQTTServerToClientSchema.MSG_SUBACK_9_FIELD_PACKETID_20);
					int returnCode = PipeReader.readInt(serverToClient, MQTTServerToClientSchema.MSG_SUBACK_9_FIELD_RETURNCODE_24);
					
					if (0x80 == returnCode) {
						int fieldErrorCode = SUB_ACK_ERR_FLAG | 0x80;
						CharSequence fieldErrorText = "Unable to subscribe";
						
					    PipeWriter.tryWriteFragment(clientResponse, MQTTClientResponseSchema.MSG_ERROR_4);
					    PipeWriter.writeInt(clientResponse,MQTTClientResponseSchema.MSG_ERROR_4_FIELD_ERRORCODE_41, fieldErrorCode);
					    PipeWriter.writeUTF8(clientResponse,MQTTClientResponseSchema.MSG_ERROR_4_FIELD_ERRORTEXT_42, fieldErrorText);
					    PipeWriter.publishWrites(clientResponse);
					} else {
						
						//TODO: what do with return code??
//					Allowed return codes:
						
//						0x00 - Success - Maximum QoS 0 
//						0x01 - Success - Maximum QoS 1 
//						0x02 - Success - Maximum QoS 2 
//						0x80 - Failure 
						
					}
					
					
			    	stopReSendingMessage(packetId9);					
					
					////////////////////
					//now release the packet Id
					////////////////////
					boolean ok9 = PipeWriter.tryWriteFragment(idGenOld, MQTTIdRangeSchema.MSG_IDRANGE_1);	
					assert(ok9);
					PipeWriter.writeInt(idGenOld, MQTTIdRangeSchema.MSG_IDRANGE_1_FIELD_RANGE_100, IdGenStage.buildRange(packetId9, packetId9+1));
					PipeWriter.publishWrites(idGenOld);	
					
					break;
				case MQTTServerToClientSchema.MSG_UNSUBACK_11:
					
					mostRecentTime = PipeReader.readLong(serverToClient, MQTTServerToClientSchema.MSG_UNSUBACK_11_FIELD_TIME_37);
					int packetId11 = PipeReader.readInt(serverToClient, MQTTServerToClientSchema.MSG_UNSUBACK_11_FIELD_PACKETID_20);
					
				    stopReSendingMessage(packetId11);					
					
					////////////////////
					//now release the packet Id
					////////////////////
					boolean ok11 = PipeWriter.tryWriteFragment(idGenOld, MQTTIdRangeSchema.MSG_IDRANGE_1);	
					assert(ok11);
					PipeWriter.writeInt(idGenOld, MQTTIdRangeSchema.MSG_IDRANGE_1_FIELD_RANGE_100, IdGenStage.buildRange(packetId11, packetId11+1));
					PipeWriter.publishWrites(idGenOld);				    
					
					break;				
					
			}
			
			PipeReader.releaseReadLock(serverToClient);
			
		}
	}


	private void stopReSendingMessage(int packetId) {
		////////////////////////
		///stop re-sending the message
		///////////////////////
		PipeWriter.tryWriteFragment(clientToServer, MQTTClientToServerSchema.MSG_STOPREPUBLISH_99);
		PipeWriter.writeInt(clientToServer, MQTTClientToServerSchema.MSG_STOPREPUBLISH_99_FIELD_PACKETID_20, packetId);
		PipeWriter.publishWrites(clientToServer);
	}

}
