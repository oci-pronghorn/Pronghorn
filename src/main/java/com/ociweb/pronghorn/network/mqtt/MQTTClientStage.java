package com.ociweb.pronghorn.network.mqtt;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.code.StageTester;
import com.ociweb.pronghorn.network.schema.MQTTClientRequestSchema;
import com.ociweb.pronghorn.network.schema.MQTTClientResponseSchema;
import com.ociweb.pronghorn.network.schema.MQTTClientToServerSchema;
import com.ociweb.pronghorn.network.schema.MQTTClientToServerSchemaAck;
import com.ociweb.pronghorn.network.schema.MQTTIdRangeSchema;
import com.ociweb.pronghorn.network.schema.MQTTServerToClientSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class MQTTClientStage extends PronghornStage {

	public static final int CON_ACK_ERR_FLAG = 1<<8;
	public static final int SUB_ACK_ERR_FLAG = 1<<9;
	private final Pipe<MQTTClientRequestSchema>     clientRequest;
	private final Pipe<MQTTIdRangeSchema>           idGenNew;
	private final Pipe<MQTTServerToClientSchema>    serverToClient;     
	private final Pipe<MQTTClientResponseSchema>    clientResponse;
	private final Pipe<MQTTIdRangeSchema>           idGenOld;
	private final Pipe<MQTTClientToServerSchema>    clientToServer; 
	private final Pipe<MQTTClientToServerSchemaAck> clientToServerAck;
	
	private IdGenCache genCache;
	private long mostRecentTime;
	private boolean brokerAcknowledgedConnection = false;
	
	private static final Logger logger = LoggerFactory.getLogger(MQTTClientStage.class);
	
	public MQTTClientStage(GraphManager gm, 
			          Pipe<MQTTClientRequestSchema> clientRequest,
			          Pipe<MQTTIdRangeSchema> idGenNew,
			          Pipe<MQTTServerToClientSchema> serverToClient, 
			          
			          Pipe<MQTTClientResponseSchema> clientResponse,
			          Pipe<MQTTIdRangeSchema> idGenOld, 
			          Pipe<MQTTClientToServerSchema> clientToServer,
			          Pipe<MQTTClientToServerSchemaAck> clientToServerAck
			          
			) {
		
		super(gm, join(clientRequest,idGenNew,serverToClient), join(clientResponse,idGenOld,clientToServer,clientToServerAck) );
		
		this.clientRequest=clientRequest;
		this.idGenNew=idGenNew;
		this.serverToClient=serverToClient;
		
		this.clientResponse=clientResponse;
		this.idGenOld=idGenOld;
		this.clientToServer = clientToServer;
		this.clientToServerAck = clientToServerAck;
				
		Pipe.setPublishBatchSize(clientToServer, 0);
		
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
				(!PipeReader.hasContentToRead(serverToClient)) //server response is always more important.
				
				&& (
						PipeReader.peekMsg(clientRequest, 
								           MQTTClientRequestSchema.MSG_BROKERCONFIG_100, 
								           MQTTClientRequestSchema.MSG_CONNECT_1)
						|| 						
						(brokerAcknowledgedConnection && //for these messages the connection must already be established.
						 MQTTEncoder.hasPacketId(genCache, idGenNew)) //all other messsages require a packetId ready for use
	            )
				&& PipeWriter.hasRoomForWrite(clientToServer) //only process if we have room to write
				&& PipeReader.tryReadFragment(clientRequest)  ) {
			
			
			
			int msgIdx = PipeReader.getMsgIdx(clientRequest);

			switch(msgIdx) {
						
				case MQTTClientRequestSchema.MSG_BROKERCONFIG_100:			
					
					brokerAcknowledgedConnection = false;
					
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
							
					brokerAcknowledgedConnection = false;
					
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
		
					PipeWriter.presumeWriteFragment(clientToServer, MQTTClientToServerSchema.MSG_PUBLISH_3);
										
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
					
				    int nextPacketId = IdGenCache.nextPacketId(genCache);
				    PipeWriter.writeInt(clientToServer, 
							MQTTClientToServerSchema.MSG_SUBSCRIBE_8_FIELD_PACKETID_20, 
							nextPacketId);	
											
				    PipeWriter.writeInt(clientToServer, MQTTClientToServerSchema.MSG_SUBSCRIBE_8_FIELD_QOS_21, PipeReader.readInt(clientRequest, MQTTClientRequestSchema.MSG_SUBSCRIBE_8_FIELD_QOS_21));
										
////////////
//WARNING: if the server does not give us an ack back then we have lost one of the ID values 
//         mosquitto is not returning the ack after subscription
////////////
//				    StringBuilder target = new StringBuilder();
//				    Appendables.appendValue(target, nextPacketId).append(":");		
//				    PipeReader.readUTF8(clientRequest, MQTTClientRequestSchema.MSG_SUBSCRIBE_8_FIELD_TOPIC_23, target);
//				    target.append("\n");
//				    System.err.println("subscription request sent with packet "+target);
				    
				    
					PipeReader.copyBytes(clientRequest, clientToServer, 
				             				MQTTClientRequestSchema.MSG_SUBSCRIBE_8_FIELD_TOPIC_23, 
				             				MQTTClientToServerSchema.MSG_SUBSCRIBE_8_FIELD_TOPIC_23);
					
					PipeWriter.publishWrites(clientToServer);
					
					break;				
				case MQTTClientRequestSchema.MSG_UNSUBSCRIBE_10:
					
					PipeWriter.tryWriteFragment(clientToServer, MQTTClientToServerSchema.MSG_UNSUBSCRIBE_10);
					
					PipeWriter.writeLong(clientToServer, 
							  MQTTClientToServerSchema.MSG_UNSUBSCRIBE_10_FIELD_TIME_37, 
							  System.currentTimeMillis());
					
					PipeWriter.writeInt(clientToServer, MQTTClientToServerSchema.MSG_UNSUBSCRIBE_10_FIELD_PACKETID_20, 
							IdGenCache.nextPacketId(genCache));
										
					PipeReader.copyBytes(clientRequest, clientToServer, 
				             MQTTClientRequestSchema.MSG_UNSUBSCRIBE_10_FIELD_TOPIC_23, 
				             MQTTClientToServerSchema.MSG_UNSUBSCRIBE_10_FIELD_TOPIC_23);
					PipeWriter.publishWrites(clientToServer);
					
					break;
				case -1:
					requestShutdown();
					break;
			}
			PipeReader.releaseReadLock(clientRequest);
			
		}
	}

	
	
	
	public void processServerResponses() {
	
//		System.err.println("server response "+
//		     PipeWriter.hasRoomForWrite(idGenOld) + " " + 
//		     PipeWriter.hasRoomForWrite(clientToServer) + " " +
//		     PipeWriter.hasRoomForWrite(clientToServerAck) + " " +
//		     PipeWriter.hasRoomForWrite(clientResponse) + " "+
//		     PipeReader.hasContentToRead(serverToClient)
//				);
//		
	
		while(   PipeWriter.hasRoomForWrite(idGenOld) 
			  && PipeWriter.hasRoomForWrite(clientToServer)
			  && PipeWriter.hasRoomForWrite(clientToServerAck)
			  && PipeWriter.hasRoomForWrite(clientResponse)
			  && PipeReader.tryReadFragment(serverToClient)) {		

			int msgIdx = PipeReader.getMsgIdx(serverToClient);


			switch(msgIdx) {
				case MQTTServerToClientSchema.MSG_DISCONNECT_14:
					brokerAcknowledgedConnection = false;	
		
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
					} else {
					
						
						
						//We are now connected.
						brokerAcknowledgedConnection = true;
						
						PipeWriter.presumeWriteFragment(clientToServerAck, MQTTClientToServerSchemaAck.MSG_BROKERACKNOWLEDGEDCONNECTION_98);
						PipeWriter.publishWrites(clientToServerAck);	
												
					}

					break;
				case MQTTServerToClientSchema.MSG_PINGRESP_13:
					
					mostRecentTime = PipeReader.readLong(serverToClient, MQTTServerToClientSchema.MSG_PINGRESP_13_FIELD_TIME_37);
															
					break;
				case MQTTServerToClientSchema.MSG_PUBACK_4:
					
					//clear the QoS 1 publishes so we stop re-sending these messages
					mostRecentTime = PipeReader.readLong(serverToClient, MQTTServerToClientSchema.MSG_PUBACK_4_FIELD_TIME_37);
					int packetId4 = PipeReader.readInt(serverToClient, MQTTServerToClientSchema.MSG_PUBACK_4_FIELD_PACKETID_20);

					releaseIdForReuse(stopReSendingMessage(clientToServer, packetId4));					
								    
					break;
				case MQTTServerToClientSchema.MSG_PUBCOMP_7:
					//last stop of QoS 2
					mostRecentTime = PipeReader.readLong(serverToClient, MQTTServerToClientSchema.MSG_PUBCOMP_7_FIELD_TIME_37);
					int packetId7 = PipeReader.readInt(serverToClient, MQTTServerToClientSchema.MSG_PUBCOMP_7_FIELD_PACKETID_20);
							
					//logger.trace("QOS2 stop for packet {}",packetId7);
					releaseIdForReuse(stopReSendingMessage(clientToServer, packetId7)); 
						
				    
					break;
				case MQTTServerToClientSchema.MSG_PUBLISH_3:
					//data from our subscriptions.
					
					mostRecentTime = PipeReader.readLong(serverToClient, MQTTServerToClientSchema.MSG_PUBLISH_3_FIELD_TIME_37);
					
					int serverSidePacketId = IdGenStage.IS_REMOTE_BIT 
							                 | PipeReader.readInt(serverToClient, MQTTServerToClientSchema.MSG_PUBLISH_3_FIELD_PACKETID_20);
					
					int retain3 = PipeReader.readInt(serverToClient, MQTTServerToClientSchema.MSG_PUBLISH_3_FIELD_RETAIN_22);
					
					int dup3 = PipeReader.readInt(serverToClient, MQTTServerToClientSchema.MSG_PUBLISH_3_FIELD_DUP_36);
					int qos3 = PipeReader.readInt(serverToClient, MQTTServerToClientSchema.MSG_PUBLISH_3_FIELD_QOS_21);
				
					
					PipeWriter.tryWriteFragment(clientResponse, MQTTClientResponseSchema.MSG_MESSAGE_3);
					
					PipeWriter.writeInt(clientResponse, MQTTClientResponseSchema.MSG_MESSAGE_3_FIELD_QOS_21, qos3);
					PipeWriter.writeInt(clientResponse, MQTTClientResponseSchema.MSG_MESSAGE_3_FIELD_DUP_36, dup3);
					PipeWriter.writeInt(clientResponse, MQTTClientResponseSchema.MSG_MESSAGE_3_FIELD_RETAIN_22, retain3);
					
					int lenTopic = PipeReader.copyBytes(serverToClient, clientResponse,
							MQTTServerToClientSchema.MSG_PUBLISH_3_FIELD_TOPIC_23, 
							MQTTClientResponseSchema.MSG_MESSAGE_3_FIELD_TOPIC_23);
					
					int lenPayload = PipeReader.copyBytes(serverToClient, clientResponse,
							MQTTServerToClientSchema.MSG_PUBLISH_3_FIELD_PAYLOAD_25, 
							MQTTClientResponseSchema.MSG_MESSAGE_3_FIELD_PAYLOAD_25);
					
//				//// debug	
//				StringBuilder b = new StringBuilder("MQTTClient:");				
//			    PipeReader.readUTF8(serverToClient, MQTTServerToClientSchema.MSG_PUBLISH_3_FIELD_TOPIC_23, b).append(" ");
//  		    PipeReader.readUTF8(serverToClient, MQTTServerToClientSchema.MSG_PUBLISH_3_FIELD_PAYLOAD_25, b);
//				System.err.println(b);					
					
					PipeWriter.publishWrites(clientResponse);
					
					if (0!=qos3) {
						
						if (1==qos3) {		//send pubAck for 1
							PipeWriter.presumeWriteFragment(clientToServerAck, MQTTClientToServerSchemaAck.MSG_PUBACK_4);
							
							PipeWriter.writeInt(clientToServerAck, MQTTClientToServerSchemaAck.MSG_PUBACK_4_FIELD_PACKETID_20, serverSidePacketId);
							PipeWriter.writeLong(clientToServerAck, MQTTClientToServerSchemaAck.MSG_PUBACK_4_FIELD_TIME_37, mostRecentTime);
							PipeWriter.publishWrites(clientToServerAck);						
						} else if (2==qos3) {
							PipeWriter.presumeWriteFragment(serverToClient, MQTTClientToServerSchema.MSG_PUBREC_5);
							
							PipeWriter.writeInt(serverToClient, MQTTClientToServerSchema.MSG_PUBREC_5_FIELD_PACKETID_20, serverSidePacketId);
							PipeWriter.writeLong(serverToClient, MQTTClientToServerSchema.MSG_PUBREC_5_FIELD_TIME_37, mostRecentTime);
							PipeWriter.publishWrites(serverToClient);							
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
					PipeWriter.presumeWriteFragment(clientToServerAck, MQTTClientToServerSchemaAck.MSG_PUBREL_6);
					
					PipeWriter.writeLong(clientToServerAck, MQTTClientToServerSchemaAck.MSG_PUBREL_6_FIELD_TIME_37, mostRecentTime);
					PipeWriter.writeInt(clientToServerAck, MQTTClientToServerSchemaAck.MSG_PUBREL_6_FIELD_PACKETID_20, packetId5);
					PipeWriter.publishWrites(clientToServerAck);
					
					break;
				case MQTTServerToClientSchema.MSG_PUBREL_6:
						
					mostRecentTime = PipeReader.readLong(serverToClient, MQTTServerToClientSchema.MSG_PUBREL_6_FIELD_TIME_37);
					int serverSidePacketId6 = IdGenStage.IS_REMOTE_BIT 
											  | PipeReader.readInt(serverToClient, MQTTServerToClientSchema.MSG_PUBREL_6_FIELD_PACKETID_20);
										
					PipeWriter.presumeWriteFragment(clientToServerAck, MQTTClientToServerSchemaAck.MSG_PUBCOMP_7);
					
					PipeWriter.writeLong(clientToServerAck, MQTTClientToServerSchemaAck.MSG_PUBCOMP_7_FIELD_TIME_37, mostRecentTime);
					PipeWriter.writeInt(clientToServerAck, MQTTClientToServerSchemaAck.MSG_PUBCOMP_7_FIELD_PACKETID_20, serverSidePacketId6);
					PipeWriter.publishWrites(clientToServerAck);
					
					break;
				case MQTTServerToClientSchema.MSG_SUBACK_9:
					
					
					mostRecentTime = PipeReader.readLong(serverToClient, MQTTServerToClientSchema.MSG_SUBACK_9_FIELD_TIME_37);
					int packetId9 = PipeReader.readInt(serverToClient, MQTTServerToClientSchema.MSG_SUBACK_9_FIELD_PACKETID_20);
					int returnCode = PipeReader.readInt(serverToClient, MQTTServerToClientSchema.MSG_SUBACK_9_FIELD_RETURNCODE_24);

					if (0x80 == returnCode) {
						int fieldErrorCode = SUB_ACK_ERR_FLAG | 0x80;
						CharSequence fieldErrorText = "Unable to subscribe";
						
					    PipeWriter.presumeWriteFragment(clientResponse, MQTTClientResponseSchema.MSG_ERROR_4);
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
					//logger.trace("sub stop for packet {}",packetId9);
					//do we need to send the return code here?
					releaseIdForReuse(stopReSendingMessage(clientToServer, packetId9));					
		
			    	
					break;
				case MQTTServerToClientSchema.MSG_UNSUBACK_11:
					
					mostRecentTime = PipeReader.readLong(serverToClient, MQTTServerToClientSchema.MSG_UNSUBACK_11_FIELD_TIME_37);
					int packetId11 = PipeReader.readInt(serverToClient, MQTTServerToClientSchema.MSG_UNSUBACK_11_FIELD_PACKETID_20);
					
					releaseIdForReuse(stopReSendingMessage(clientToServer, packetId11));					
				    
				    
					break;				
					
			}
			
			PipeReader.releaseReadLock(serverToClient);
			
		}
	}


	private int relVal = 0;
	private int relLim = 0;
	
	private void releaseIdForReuse(int id) {
		
		if (relVal == relLim) {
			relVal = id;
			relLim = id+1;
		} else {
			if (id == relVal-1) {
				relVal = id;
			} else if (id == relLim) {
				relLim = id+1;
			} else {
				//must flush what we have				
				PipeWriter.presumeWriteFragment(idGenOld, MQTTIdRangeSchema.MSG_IDRANGE_1);
				PipeWriter.writeInt(idGenOld, MQTTIdRangeSchema.MSG_IDRANGE_1_FIELD_RANGE_100, IdGenStage.buildRange(relVal, relLim));
				PipeWriter.publishWrites(idGenOld);
	
				relVal = id;
				relLim = id+1;
				
				return;
			}
		}

		//after holding a lot of ids force a release of them all
		//this batching reduces the load on the IdGenStage
		if (relLim-relVal > 10_000) {
			PipeWriter.presumeWriteFragment(idGenOld, MQTTIdRangeSchema.MSG_IDRANGE_1);
			PipeWriter.writeInt(idGenOld, MQTTIdRangeSchema.MSG_IDRANGE_1_FIELD_RANGE_100, IdGenStage.buildRange(relVal, relLim));
			PipeWriter.publishWrites(idGenOld);

			relVal = 0;
			relLim = 0;
		}
		
	}

	


	private int stopReSendingMessage(Pipe<MQTTClientToServerSchema> clientToSerer, int packetId) {
		////////////////////////
		///stop re-sending the message
		///////////////////////
		PipeWriter.presumeWriteFragment(clientToServerAck, MQTTClientToServerSchemaAck.MSG_STOPREPUBLISH_99);
		PipeWriter.writeInt(clientToServerAck, MQTTClientToServerSchemaAck.MSG_STOPREPUBLISH_99_FIELD_PACKETID_20, packetId);
		PipeWriter.publishWrites(clientToServerAck);
		return packetId;
	}

}
