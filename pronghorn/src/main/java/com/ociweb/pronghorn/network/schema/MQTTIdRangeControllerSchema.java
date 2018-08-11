package com.ociweb.pronghorn.network.schema;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class MQTTIdRangeControllerSchema extends MessageSchema<MQTTIdRangeControllerSchema> {

	
	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
	    new int[]{0xc0400001,0xc0200001,0xc0400002,0x80000000,0xc0200002,0xc0400001,0xc0200001},
	    (short)0,
	    new String[]{"ClearAll",null,"IdRange","Range",null,"Ready",null},
	    new long[]{2, 0, 1, 100, 0, 3, 0},
	    new String[]{"global",null,"global",null,null,"global",null},
	    "MQTTIdControlRanges.xml",
	    new long[]{2, 2, 0},
	    new int[]{2, 2, 0});
	
	
	protected MQTTIdRangeControllerSchema() { 
	    super(FROM);
	}
	
	public static final MQTTIdRangeControllerSchema instance = new MQTTIdRangeControllerSchema();
    
	public static final int MSG_CLEARALL_2 = 0x00000000; //Group/OpenTempl/1
	public static final int MSG_IDRANGE_1 = 0x00000002; //Group/OpenTempl/2
	public static final int MSG_IDRANGE_1_FIELD_RANGE_100 = 0x00000001; //IntegerUnsigned/None/0
	public static final int MSG_READY_3 = 0x00000005; //Group/OpenTempl/1


	public static void consume(Pipe<MQTTIdRangeControllerSchema> input) {
	    while (PipeReader.tryReadFragment(input)) {
	        int msgIdx = PipeReader.getMsgIdx(input);
	        switch(msgIdx) {
	            case MSG_CLEARALL_2:
	                consumeClearAll(input);
	            break;
	            case MSG_IDRANGE_1:
	                consumeIdRange(input);
	            break;
	            case MSG_READY_3:
	                consumeReady(input);
	            break;
	            case -1:
	               //requestShutdown();
	            break;
	        }
	        PipeReader.releaseReadLock(input);
	    }
	}

	public static void consumeClearAll(Pipe<MQTTIdRangeControllerSchema> input) {
	}
	public static void consumeIdRange(Pipe<MQTTIdRangeControllerSchema> input) {
	    int fieldRange = PipeReader.readInt(input,MSG_IDRANGE_1_FIELD_RANGE_100);
	}
	public static void consumeReady(Pipe<MQTTIdRangeControllerSchema> input) {
	}

	public static void publishClearAll(Pipe<MQTTIdRangeControllerSchema> output) {
	        PipeWriter.presumeWriteFragment(output, MSG_CLEARALL_2);
	        PipeWriter.publishWrites(output);
	}
	public static void publishIdRange(Pipe<MQTTIdRangeControllerSchema> output, int fieldRange) {
	        PipeWriter.presumeWriteFragment(output, MSG_IDRANGE_1);
	        PipeWriter.writeInt(output,MSG_IDRANGE_1_FIELD_RANGE_100, fieldRange);
	        PipeWriter.publishWrites(output);
	}
	public static void publishReady(Pipe<MQTTIdRangeControllerSchema> output) {
	        PipeWriter.presumeWriteFragment(output, MSG_READY_3);
	        PipeWriter.publishWrites(output);
	}

}
