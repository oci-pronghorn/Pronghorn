package com.ociweb.pronghorn.network.mqtt;

import com.ociweb.pronghorn.network.ClientCoordinator;
import com.ociweb.pronghorn.network.schema.MQTTConnectionInSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class MQTTClientRequestStage extends PronghornStage {

	private final Pipe<MQTTConnectionInSchema> input;
	private final Pipe<NetPayloadSchema>[] toBroker;
	
	public MQTTClientRequestStage(GraphManager gm, Pipe<MQTTConnectionInSchema> input, Pipe<NetPayloadSchema>[] toBroker) {
		super(gm, input, toBroker);
		this.input = input;
		this.toBroker = toBroker;
	}

	@Override
	public void run() {
		
		while (PipeReader.tryReadFragment(input)) {
			
			int msgIdx = PipeReader.getMsgIdx(input);
			
			switch (msgIdx) {
			
					case MQTTConnectionInSchema.MSG_PUBLISH_1:
				
						PipeReader.readInt(input, MQTTConnectionInSchema.MSG_PUBLISH_1_FIELD_QOS_100);
						PipeReader.readInt(input, MQTTConnectionInSchema.MSG_PUBLISH_1_FIELD_PACKETID_200);
						
						DataInputBlobReader<MQTTConnectionInSchema> stream1 = PipeReader.inputStream(input, MQTTConnectionInSchema.MSG_PUBLISH_1_FIELD_PACKETDATA_300 );
												
						//TODO: publish logic?? 
						
						
					break;
					case MQTTConnectionInSchema.MSG_CONNECT_2:
				
						PipeReader.readUTF8(input, MQTTConnectionInSchema.MSG_CONNECT_2_FIELD_HOST_401, new StringBuilder());
						PipeReader.readInt(input, MQTTConnectionInSchema.MSG_CONNECT_2_FIELD_PORT_402);
						
						DataInputBlobReader<MQTTConnectionInSchema> stream2 = PipeReader.inputStream(input, MQTTConnectionInSchema.MSG_CONNECT_2_FIELD_PACKETDATA_300);
						
						//TODO: store connection data details
						
						//TODO: call for
						
			 			//activeConnection = ClientCoordinator.openConnection(ccm, hostBack, hostPos, hostLen, hostMask, port, userId, output, connectionId);

						
					break;					
					case MQTTConnectionInSchema.MSG_SUBSCRIBE_3:
						
						DataInputBlobReader<MQTTConnectionInSchema> stream3 = PipeReader.inputStream(input, MQTTConnectionInSchema.MSG_SUBSCRIBE_3_FIELD_PACKETDATA_300 );
						
						//TODO: send request
						
					break;
					case MQTTConnectionInSchema.MSG_UNSUBSCRIBE_4:
						
						DataInputBlobReader<MQTTConnectionInSchema> stream4 = PipeReader.inputStream(input, MQTTConnectionInSchema.MSG_UNSUBSCRIBE_4_FIELD_PACKETDATA_300 );
						
						//TODO: send request
						
					break;					
					case MQTTConnectionInSchema.MSG_DISCONNECT_5:
						
					break;					
					case MQTTConnectionInSchema.MSG_PUBACK_6:
						
						PipeReader.readInt(input, MQTTConnectionInSchema.MSG_PUBACK_6_FIELD_PACKETID_200);
						
						DataInputBlobReader<MQTTConnectionInSchema> stream6 = PipeReader.inputStream(input, MQTTConnectionInSchema.MSG_PUBACK_6_FIELD_PACKETDATA_300 );
						
						//TODO: send request
						
					break;					
					case MQTTConnectionInSchema.MSG_PUBREC_7:
						
						PipeReader.readInt(input, MQTTConnectionInSchema.MSG_PUBREC_7_FIELD_PACKETID_200 );
						DataInputBlobReader<MQTTConnectionInSchema> stream7 = PipeReader.inputStream(input, MQTTConnectionInSchema.MSG_PUBREC_7_FIELD_PACKETDATA_300  );
						
						//TODO: send request
						
						
					break;
					case MQTTConnectionInSchema.MSG_PUBCOMP_8:
						
						PipeReader.readInt(input, MQTTConnectionInSchema.MSG_PUBCOMP_8_FIELD_PACKETID_200 );
						DataInputBlobReader<MQTTConnectionInSchema> stream8 = PipeReader.inputStream(input, MQTTConnectionInSchema.MSG_PUBCOMP_8_FIELD_PACKETDATA_300 );
						
						//TODO: send request
												
					break;
					case MQTTConnectionInSchema.MSG_PUBREL_9:
			
						PipeReader.readInt(input, MQTTConnectionInSchema.MSG_PUBREL_9_FIELD_PACKETID_200 );
						DataInputBlobReader<MQTTConnectionInSchema> stream9 = PipeReader.inputStream(input, MQTTConnectionInSchema.MSG_PUBREL_9_FIELD_PACKETDATA_300 );
						
						//TODO: send request						
						
					break;
			}
			PipeReader.releaseReadLock(input);
			
		}
		
		
	}

}
