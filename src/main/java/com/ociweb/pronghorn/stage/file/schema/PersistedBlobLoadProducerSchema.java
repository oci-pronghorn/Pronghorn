package com.ociweb.pronghorn.stage.file.schema;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class PersistedBlobLoadProducerSchema extends MessageSchema<PersistedBlobLoadProducerSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
			new int[]{0xc0400002,0x90000000,0xc0200002},
			(short)0,
			new String[]{"AckWrite","BlockId",null},
			new long[]{11, 3, 0},
			new String[]{"global",null,null},
			"PersistedBlobLoadProducer.xml",
			new long[]{2, 2, 0},
			new int[]{2, 2, 0});

	protected PersistedBlobLoadProducerSchema() { 
	    super(FROM);
	}

	public static final PersistedBlobLoadProducerSchema instance = new PersistedBlobLoadProducerSchema();


	public static final int MSG_ACKWRITE_11 = 0x00000000; //Group/OpenTempl/2
	public static final int MSG_ACKWRITE_11_FIELD_BLOCKID_3 = 0x00800001; //LongUnsigned/None/0

	public static void consume(Pipe<PersistedBlobLoadProducerSchema> input) {
	    while (PipeReader.tryReadFragment(input)) {
	        int msgIdx = PipeReader.getMsgIdx(input);
	        switch(msgIdx) {
	            case MSG_ACKWRITE_11:
	                consumeAckWrite(input);
	            break;
	            case -1:
	               //requestShutdown();
	            break;
	        }
	        PipeReader.releaseReadLock(input);
	    }
	}

	public static void consumeAckWrite(Pipe<PersistedBlobLoadProducerSchema> input) {
	    long fieldBlockId = PipeReader.readLong(input,MSG_ACKWRITE_11_FIELD_BLOCKID_3);
	}

	public static void publishAckWrite(Pipe<PersistedBlobLoadProducerSchema> output, long fieldBlockId) {
	        PipeWriter.presumeWriteFragment(output, MSG_ACKWRITE_11);
	        PipeWriter.writeLong(output,MSG_ACKWRITE_11_FIELD_BLOCKID_3, fieldBlockId);
	        PipeWriter.publishWrites(output);
	}

}
