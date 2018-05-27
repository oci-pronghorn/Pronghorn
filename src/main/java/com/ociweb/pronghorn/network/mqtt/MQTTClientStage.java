package com.ociweb.pronghorn.network.mqtt;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.MQTTClientRequestSchema;
import com.ociweb.pronghorn.network.schema.MQTTClientResponseSchema;
import com.ociweb.pronghorn.network.schema.MQTTClientToServerSchema;
import com.ociweb.pronghorn.network.schema.MQTTClientToServerSchemaAck;
import com.ociweb.pronghorn.network.schema.MQTTIdRangeSchema;
import com.ociweb.pronghorn.network.schema.MQTTServerToClientSchema;
import com.ociweb.pronghorn.pipe.FragmentWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

/**
 * Represents a MQTT client and perfroms required client responsibilities according to specs.
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
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

	/**
	 *
	 * @param gm
	 * @param clientRequest _in_ Defines the MQTT client request
	 * @param idGenNew _in_ IDs for generation
	 * @param serverToClient _in_ Server to Client schema
	 * @param clientResponse _out_ Defines the MQTT client response
	 * @param idGenOld _out_ Old IDs
	 * @param clientToServer _out_ Defines client to server communication.
	 * @param clientToServerAck _out_ Sends acknowledgment to server.
	 */
	public MQTTClientStage(GraphManager gm, 
			          Pipe<MQTTClientRequestSchema> clientRequest,
			          Pipe<MQTTIdRangeSchema> idGenNew,
			          Pipe<MQTTServerToClientSchema> serverToClient, 
			          
			          Pipe<MQTTClientResponseSchema> clientResponse, //TODO: may ad ack for publish...
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
				(!Pipe.hasContentToRead(serverToClient)) //server response is always more important.
				
				&& (
						Pipe.peekMsg(clientRequest, 
								           MQTTClientRequestSchema.MSG_BROKERCONFIG_100, 
								           MQTTClientRequestSchema.MSG_CONNECT_1)
						|| 						
						(brokerAcknowledgedConnection && //for these messages the connection must already be established.
						 MQTTEncoder.hasPacketId(genCache, idGenNew)) //all other messsages require a packetId ready for use
	            )
				&& Pipe.hasRoomForWrite(clientToServer) //only process if we have room to write
				&& Pipe.hasContentToRead(clientRequest)  ) {
			
			
			
			final int msgIdx = Pipe.takeMsgIdx(clientRequest);

			switch(msgIdx) {
						
				case MQTTClientRequestSchema.MSG_BROKERCONFIG_100:			
					{
						brokerAcknowledgedConnection = false;
						
						Pipe.presumeRoomForWrite(clientToServer);
						int size = Pipe.addMsgIdx(clientToServer, MQTTClientToServerSchema.MSG_BROKERHOST_100);
						
						Pipe.addByteArray(clientRequest, clientToServer); //Host
						Pipe.addIntValue(Pipe.takeInt(clientRequest), clientToServer); //Port
				
						Pipe.confirmLowLevelWrite(clientToServer, size);
						Pipe.publishWrites(clientToServer); 
					}   
					break;
				case MQTTClientRequestSchema.MSG_CONNECT_1:
					{
						brokerAcknowledgedConnection = false;
						
						Pipe.presumeRoomForWrite(clientToServer);
						int size = Pipe.addMsgIdx(clientToServer, MQTTClientToServerSchema.MSG_CONNECT_1);
				
						Pipe.addLongValue(System.currentTimeMillis(), clientToServer); //TIME
						Pipe.addIntValue(Pipe.takeInt(clientRequest), clientToServer); //KEEPALIVESEC
						Pipe.addIntValue(Pipe.takeInt(clientRequest), clientToServer); //FLAGS
										
						Pipe.addByteArray(clientRequest, clientToServer); //CLIENTID
						Pipe.addByteArray(clientRequest, clientToServer); //WILLTOPIC
						Pipe.addByteArray(clientRequest, clientToServer); //WILLPAYLOAD
						Pipe.addByteArray(clientRequest, clientToServer); //USER
						Pipe.addByteArray(clientRequest, clientToServer); //PASS
						
						Pipe.confirmLowLevelWrite(clientToServer, size);
						Pipe.publishWrites(clientToServer); 
						 
					}
					break;			
				case MQTTClientRequestSchema.MSG_PUBLISH_3:
					{
						int valueQoS = Pipe.takeInt(clientRequest);
						
						Pipe.presumeRoomForWrite(clientToServer);
						int size = Pipe.addMsgIdx(clientToServer, MQTTClientToServerSchema.MSG_PUBLISH_3);					
						Pipe.addLongValue(System.currentTimeMillis(), clientToServer);				
						
						int packetId = -1;
						if (valueQoS != 0) {						
							//only consume a packetId for QoS 1 or 2.
							packetId = IdGenCache.nextPacketId(genCache);
						}
						Pipe.addIntValue(packetId, clientToServer);
						Pipe.addIntValue(valueQoS, clientToServer);
						Pipe.addIntValue(Pipe.takeInt(clientRequest), clientToServer);  //retain
									
						Pipe.addByteArray(clientRequest, clientToServer); //topic
						Pipe.addByteArray(clientRequest, clientToServer); //payload
												
						Pipe.confirmLowLevelWrite(clientToServer, size);
						Pipe.publishWrites(clientToServer); 					
					}
					break;				
				case MQTTClientRequestSchema.MSG_SUBSCRIBE_8:
					{			
						Pipe.presumeRoomForWrite(clientToServer);
						int size = Pipe.addMsgIdx(clientToServer, MQTTClientToServerSchema.MSG_SUBSCRIBE_8);	
			
						Pipe.addLongValue(System.currentTimeMillis(), clientToServer); //time
						int nextPacketId = IdGenCache.nextPacketId(genCache);
						Pipe.addIntValue(nextPacketId, clientToServer);
							
						Pipe.addIntValue(Pipe.takeInt(clientRequest), clientToServer); // QoS
						Pipe.addByteArray(clientRequest, clientToServer); // Topic
						
						Pipe.confirmLowLevelWrite(clientToServer, size);
						Pipe.publishWrites(clientToServer); 
					}
					break;				
				case MQTTClientRequestSchema.MSG_UNSUBSCRIBE_10:
					{
						Pipe.presumeRoomForWrite(clientToServer);
						int size = Pipe.addMsgIdx(clientToServer, MQTTClientToServerSchema.MSG_UNSUBSCRIBE_10);
						
						Pipe.addLongValue(System.currentTimeMillis(), clientToServer); //time
						Pipe.addIntValue(IdGenCache.nextPacketId(genCache), clientToServer);  //packetID
						
						Pipe.addByteArray(clientRequest, clientToServer); // Topic
						
						Pipe.confirmLowLevelWrite(clientToServer, size);
						Pipe.publishWrites(clientToServer); 
			     	}
					break;
				case -1:
					{
						Pipe.publishEOF(clientToServer);
						//Do not call this here: requestShutdown();
						//this is called elsewhere since we may be waiting for returning input
					}
					break;
			}
			Pipe.confirmLowLevelRead(clientRequest, Pipe.sizeOf(clientRequest, msgIdx));
			Pipe.releaseReadLock(clientRequest);
			
		}
	}

	private long[] QoS2Seen = new long[1<<16];
	
	public void processServerResponses() {
	
//		System.err.println("server response "+
//		     PipeWriter.hasRoomForWrite(idGenOld) + " " + 
//		     PipeWriter.hasRoomForWrite(clientToServer) + " " +
//		     PipeWriter.hasRoomForWrite(clientToServerAck) + " " +
//		     PipeWriter.hasRoomForWrite(clientResponse) + " "+
//		     PipeReader.hasContentToRead(serverToClient) + " "+serverToClient
//				);
		
	
		while(   Pipe.hasRoomForWrite(idGenOld) 
			  && Pipe.hasRoomForWrite(clientToServer)
			  && Pipe.hasRoomForWrite(clientToServerAck)
			  && Pipe.hasRoomForWrite(clientResponse)
			  && Pipe.hasContentToRead(serverToClient)) {		

			final int msgIdx = Pipe.takeMsgIdx(serverToClient);

			switch(msgIdx) {
				case MQTTServerToClientSchema.MSG_DISCONNECT_14:
					brokerAcknowledgedConnection = false;	
					mostRecentTime = Pipe.takeLong(serverToClient);
					//NOTE: do not need do anything now, the connection will be re-attached.
				break;
				case MQTTServerToClientSchema.MSG_CONNACK_2:
				
					mostRecentTime = Pipe.takeLong(serverToClient);
					int sessionPresentFlag = Pipe.takeInt(serverToClient);
					int retCode = Pipe.takeInt(serverToClient);

					if (0==retCode) {
						//We are now connected.
						brokerAcknowledgedConnection = true;

						Pipe.presumeRoomForWrite(clientToServerAck);
						FragmentWriter.write(clientToServerAck, MQTTClientToServerSchemaAck.MSG_BROKERACKNOWLEDGEDCONNECTION_98);

					}
					//System.err.println("connected with id "+retCode);
					
					Pipe.presumeRoomForWrite(clientResponse);
					FragmentWriter.writeII(clientResponse,
							   MQTTClientResponseSchema.MSG_CONNECTIONATTEMPT_5, 
							   retCode,
							   sessionPresentFlag);
					
					break;
				case MQTTServerToClientSchema.MSG_PINGRESP_13:
					
					mostRecentTime = Pipe.takeLong(serverToClient);
															
					break;
				case MQTTServerToClientSchema.MSG_PUBACK_4:
					
					//clear the QoS 1 publishes so we stop re-sending these messages
					mostRecentTime = Pipe.takeLong(serverToClient);
					int packetId4 = Pipe.takeInt(serverToClient);
					
					releaseIdForReuse(stopReSendingMessage(clientToServer, packetId4));					
								    
					break;
				case MQTTServerToClientSchema.MSG_PUBCOMP_7:
					//last stop of QoS 2
					mostRecentTime = Pipe.takeLong(serverToClient);
					int packetId7 = Pipe.takeInt(serverToClient);
							
					//logger.trace("QOS2 stop for packet {}",packetId7);
					releaseIdForReuse(stopReSendingMessage(clientToServer, packetId7)); 
						
				    
					break;
				case MQTTServerToClientSchema.MSG_PUBLISH_3:
					{
						//data from our subscriptions.
						
						mostRecentTime = Pipe.takeLong(serverToClient); //TIME
						int qos3 = Pipe.takeInt(serverToClient); //QOS
												
						Pipe.markHead(clientResponse);
					    Pipe.presumeRoomForWrite(clientResponse);
						int size = Pipe.addMsgIdx(clientResponse, MQTTClientResponseSchema.MSG_MESSAGE_3);
						Pipe.addIntValue(qos3, clientResponse);
						Pipe.addIntValue(Pipe.takeInt(serverToClient), clientResponse); //RETAIN
						Pipe.addIntValue(Pipe.takeInt(serverToClient), clientResponse); //DUP						
						Pipe.addByteArray(serverToClient, clientResponse); //topic
			
						int serverSidePacketId = IdGenStage.IS_REMOTE_BIT | Pipe.takeInt(serverToClient);

						Pipe.addByteArray(serverToClient, clientResponse); //payload
						
						
						boolean publishWrite = true;
						if(2==qos3) {							
							if (0!=QoS2Seen[serverSidePacketId]) {
								publishWrite = false;//already sent
							}						
						}
						
						if (publishWrite) {						
							Pipe.confirmLowLevelWrite(clientResponse, size);
							Pipe.publishWrites(clientResponse);
							
							if (2==qos3) {
								//do not send gain, send pubrec after tail clears this position
								QoS2Seen[serverSidePacketId] = Pipe.headPosition(clientResponse);
							}
							
						} else {
							//abandon what was written
							Pipe.resetHead(clientResponse);
						}
						
											
						if (0!=qos3) {
							
							if (1==qos3) {		//send pubAck for 1
								Pipe.presumeRoomForWrite(clientToServerAck);
								FragmentWriter.writeLI(clientToServerAck, MQTTClientToServerSchemaAck.MSG_PUBACK_4, mostRecentTime, serverSidePacketId);
								
							} else if (2==qos3) {
								
								//
								//TODO: only send rec after consumed, eg tail >= QoS2Seen[serverSidePacketId]
								//      until done we may ack back and never consume it (eg could be dropped)
								//      this only happens on power failure while the field is consumed.
								
								//Use this to get pipes outside and pass them in 
								//FileGraphBuilder.buildSequentialReplayer
								//will ensure we use same code over and that data is encrypted.
								
								//TODO: also store to disk as sent
								//      if not written to disk then same value could be sent upon power failure after						
								
								
								Pipe.presumeRoomForWrite(serverToClient);
								FragmentWriter.writeLI(serverToClient, MQTTClientToServerSchema.MSG_PUBREC_5, mostRecentTime, serverSidePacketId);
								
							}
						}
					}
					break;
				case MQTTServerToClientSchema.MSG_PUBREC_5:
					//for QoS 2 publish, now release the message
					
					mostRecentTime = Pipe.takeLong(serverToClient);
					int packetId5 = Pipe.takeInt(serverToClient);

					//////////////////////
					//send pubrel and stop re-sending the message
					//////////////////////
					Pipe.presumeRoomForWrite(clientToServerAck);
					FragmentWriter.writeLI(clientToServerAck, 
							MQTTClientToServerSchemaAck.MSG_PUBREL_6,
							mostRecentTime, 
							packetId5
							);
					
					break;
				case MQTTServerToClientSchema.MSG_PUBREL_6:
						
					
					mostRecentTime = Pipe.takeLong(serverToClient);
					int serverSidePacketId6 = IdGenStage.IS_REMOTE_BIT 
											  | Pipe.takeInt(serverToClient);//packetId 
											  
					/////////////////
					assert(QoS2Seen[serverSidePacketId6]==1) : "book keeping error in QoS 2";
					QoS2Seen[serverSidePacketId6] = 0;
					
					//TODO: clear from disk QoS 2 value, know its saved becaause we did not pubrec until saved
					
					/////////////////		
					
					Pipe.presumeRoomForWrite(clientToServerAck);
					FragmentWriter.writeLI(clientToServerAck, 
							 MQTTClientToServerSchemaAck.MSG_PUBCOMP_7, 
							 mostRecentTime, 
							 serverSidePacketId6);
				
					break;
				case MQTTServerToClientSchema.MSG_SUBACK_9:
					mostRecentTime = Pipe.takeLong(serverToClient);
					int packetId9 = Pipe.takeInt(serverToClient);
					int maxQoS = Pipe.takeInt(serverToClient);
					/* The spec says we may have an array of this enumerated byte but currently we send only one sub at a time
//						0x00 - Success - Maximum QoS 0
//						0x01 - Success - Maximum QoS 1
//						0x02 - Success - Maximum QoS 2
//						0x80 - Failure
					 */
										
					Pipe.presumeRoomForWrite(clientResponse);
					FragmentWriter.writeI(clientResponse, 
							        MQTTClientResponseSchema.MSG_SUBSCRIPTIONRESULT_4,
							        maxQoS); 

					releaseIdForReuse(stopReSendingMessage(clientToServer, packetId9));

					break;
				case MQTTServerToClientSchema.MSG_UNSUBACK_11:

					mostRecentTime = Pipe.takeLong(serverToClient);
					int packetId11 = Pipe.takeInt(serverToClient);
					
					releaseIdForReuse(stopReSendingMessage(clientToServer, packetId11));
				    
					break;
			}
			
			Pipe.confirmLowLevelRead(serverToClient, Pipe.sizeOf(serverToClient, msgIdx));
			Pipe.releaseReadLock(serverToClient);
			
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
				Pipe.presumeRoomForWrite(idGenOld);
				FragmentWriter.writeI(idGenOld, MQTTIdRangeSchema.MSG_IDRANGE_1, IdGenStage.buildRange(relVal, relLim));
				
				relVal = id;
				relLim = id+1;
				
				return;
			}
		}

		//after holding a lot of ids force a release of them all
		//this batching reduces the load on the IdGenStage
		if (relLim-relVal > 10_000) {
			Pipe.presumeRoomForWrite(idGenOld);
			FragmentWriter.writeI(idGenOld, MQTTIdRangeSchema.MSG_IDRANGE_1, IdGenStage.buildRange(relVal, relLim));
			
			relVal = 0;
			relLim = 0;
		}
		
	}

	


	private int stopReSendingMessage(Pipe<MQTTClientToServerSchema> clientToSerer, int packetId) {
		////////////////////////
		///stop re-sending the message
		///////////////////////
		Pipe.presumeRoomForWrite(clientToServerAck);
		FragmentWriter.writeI(clientToServerAck, MQTTClientToServerSchemaAck.MSG_STOPREPUBLISH_99, packetId);
		
		return packetId;
	}

}
