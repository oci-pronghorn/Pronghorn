package com.ociweb.pronghorn.network.mqtt;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.network.schema.MQTTServerToClientSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

/**
 * _no-docs_
 */
public class MQTTServerToClientEncodeStage extends PronghornStage {

	private final Pipe<MQTTServerToClientSchema> inputPipe;
	private final Pipe<NetPayloadSchema> outputPipe;
	
	protected MQTTServerToClientEncodeStage(GraphManager graphManager, Pipe<MQTTServerToClientSchema> input, Pipe<NetPayloadSchema> output) {
		super(graphManager, input, output);
		this.inputPipe = input;
		this.outputPipe = output;
	}

	@Override
	public void run() {
		
		
		while (PipeWriter.hasRoomForWrite(outputPipe)
			   &&	PipeReader.tryReadFragment(inputPipe)) {
			
			
//			boolean ok = PipeWriter.tryWriteFragment(outputPipe, NetPayloadSchema.MSG_PLAIN_210);
//			assert(ok) : "checked above and should not happen";
//						
//			DataOutputBlobWriter<NetPayloadSchema> output = PipeWriter.outputStream(outputPipe);
//			DataOutputBlobWriter.openField(output);
//
//			DataOutputBlobWriter.closeHighLevelField(writer, NetPayloadSchema.)
//			PipeWriter.publishWrites(outputPipe);
			
			
		    int msgIdx = PipeReader.getMsgIdx(inputPipe);
		    Pipe<MQTTServerToClientSchema> input = this.inputPipe;
		    switch(msgIdx) {
		        case MQTTServerToClientSchema.MSG_PUBACK_4:
				consumePubAck(input);
		        break;
		        case MQTTServerToClientSchema.MSG_PUBREC_5:
				consumePubRec(input);
		        break;
		        case MQTTServerToClientSchema.MSG_PUBREL_6:
				consumePubRel(input);
		        break;
		        case MQTTServerToClientSchema.MSG_CONNACK_2:
				consumeConnAck(input);
		        break;
		        case MQTTServerToClientSchema.MSG_PUBLISH_3:
				consumePublish(input);
		        break;
		        case MQTTServerToClientSchema.MSG_DISCONNECT_14:
				consumeDisconnect(input);
		        break;
		        case MQTTServerToClientSchema.MSG_PUBCOMP_7:
				consumePubComp(input);
		        break;
		        case MQTTServerToClientSchema.MSG_SUBACK_9:
				consumeSubAck(input);
		        break;
		        case MQTTServerToClientSchema.MSG_UNSUBACK_11:
				consumeUnsubAck(input);
		        break;
		        case MQTTServerToClientSchema.MSG_PINGRESP_13:
				consumePingResp(input);
		        break;
		        case -1:
		        requestShutdown();
		        break;
		    }
		    
		    
		    
		    PipeReader.releaseReadLock(input);
		}
		

	}

	private void consumePingResp(Pipe<MQTTServerToClientSchema> input) {
		long fieldTime = PipeReader.readLong(input,MQTTServerToClientSchema.MSG_PINGRESP_13_FIELD_TIME_37);
	}

	private void consumeUnsubAck(Pipe<MQTTServerToClientSchema> input) {
		long fieldTime = PipeReader.readLong(input,MQTTServerToClientSchema.MSG_UNSUBACK_11_FIELD_TIME_37);
		int fieldPacketId = PipeReader.readInt(input,MQTTServerToClientSchema.MSG_UNSUBACK_11_FIELD_PACKETID_20);
	}

	private void consumeSubAck(Pipe<MQTTServerToClientSchema> input) {
		long fieldTime = PipeReader.readLong(input,MQTTServerToClientSchema.MSG_SUBACK_9_FIELD_TIME_37);
		int fieldPacketId = PipeReader.readInt(input,MQTTServerToClientSchema.MSG_SUBACK_9_FIELD_PACKETID_20);
		int fieldReturnCode = PipeReader.readInt(input,MQTTServerToClientSchema.MSG_SUBACK_9_FIELD_RETURNCODE_24);
	}

	private void consumePubComp(Pipe<MQTTServerToClientSchema> input) {
		long fieldTime = PipeReader.readLong(input,MQTTServerToClientSchema.MSG_PUBCOMP_7_FIELD_TIME_37);
		int fieldPacketId = PipeReader.readInt(input,MQTTServerToClientSchema.MSG_PUBCOMP_7_FIELD_PACKETID_20);
	}

	private void consumeDisconnect(Pipe<MQTTServerToClientSchema> input) {
		long fieldTime = PipeReader.readLong(input,MQTTServerToClientSchema.MSG_DISCONNECT_14_FIELD_TIME_37);
	}

	private void consumePublish(Pipe<MQTTServerToClientSchema> input) {
		long fieldTime = PipeReader.readLong(input,MQTTServerToClientSchema.MSG_PUBLISH_3_FIELD_TIME_37);
		int fieldPacketId = PipeReader.readInt(input,MQTTServerToClientSchema.MSG_PUBLISH_3_FIELD_PACKETID_20);
		int fieldQOS = PipeReader.readInt(input,MQTTServerToClientSchema.MSG_PUBLISH_3_FIELD_QOS_21);
		int fieldRetain = PipeReader.readInt(input,MQTTServerToClientSchema.MSG_PUBLISH_3_FIELD_RETAIN_22);
		int fieldDup = PipeReader.readInt(input,MQTTServerToClientSchema.MSG_PUBLISH_3_FIELD_DUP_36);
		StringBuilder fieldTopic = PipeReader.readUTF8(input,MQTTServerToClientSchema.MSG_PUBLISH_3_FIELD_TOPIC_23,new StringBuilder(PipeReader.readBytesLength(input,MQTTServerToClientSchema.MSG_PUBLISH_3_FIELD_TOPIC_23)));
		ByteBuffer fieldPayload = PipeReader.readBytes(input,MQTTServerToClientSchema.MSG_PUBLISH_3_FIELD_PAYLOAD_25,ByteBuffer.allocate(PipeReader.readBytesLength(input,MQTTServerToClientSchema.MSG_PUBLISH_3_FIELD_PAYLOAD_25)));
	}

	private void consumeConnAck(Pipe<MQTTServerToClientSchema> input) {
		long fieldTime = PipeReader.readLong(input,MQTTServerToClientSchema.MSG_CONNACK_2_FIELD_TIME_37);
		int fieldFlag = PipeReader.readInt(input,MQTTServerToClientSchema.MSG_CONNACK_2_FIELD_FLAG_35);
		int fieldReturnCode = PipeReader.readInt(input,MQTTServerToClientSchema.MSG_CONNACK_2_FIELD_RETURNCODE_24);
	}

	private void consumePubRel(Pipe<MQTTServerToClientSchema> input) {
		long fieldTime = PipeReader.readLong(input,MQTTServerToClientSchema.MSG_PUBREL_6_FIELD_TIME_37);
		int fieldPacketId = PipeReader.readInt(input,MQTTServerToClientSchema.MSG_PUBREL_6_FIELD_PACKETID_20);
	}

	private void consumePubRec(Pipe<MQTTServerToClientSchema> input) {
		long fieldTime = PipeReader.readLong(input,MQTTServerToClientSchema.MSG_PUBREC_5_FIELD_TIME_37);
		int fieldPacketId = PipeReader.readInt(input,MQTTServerToClientSchema.MSG_PUBREC_5_FIELD_PACKETID_20);
	}

	private void consumePubAck(Pipe<MQTTServerToClientSchema> input) {
		long fieldTime = PipeReader.readLong(input,MQTTServerToClientSchema.MSG_PUBACK_4_FIELD_TIME_37);
		int fieldPacketId = PipeReader.readInt(input,MQTTServerToClientSchema.MSG_PUBACK_4_FIELD_PACKETID_20);
	}

}
