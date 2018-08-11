package com.ociweb.pronghorn.network.schema;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

/**
 * Defines alert notices.
 *
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class AlertNoticeSchema extends MessageSchema<AlertNoticeSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
	    new int[]{0xc0400005,0x90000000,0x90000001,0x90000001,0x80000000,0xc0200005},
	    (short)0,
	    new String[]{"RouteSLA","Connection","Sequence","Duration","Route",null},
	    new long[]{100, 1, 2, 2, 4, 0},
	    new String[]{"global",null,null,null,null,null},
	    "AlertNotice.xml",
	    new long[]{2, 2, 0},
	    new int[]{2, 2, 0});
	
	public AlertNoticeSchema(FieldReferenceOffsetManager from) { 
	    super(from);
	}

	public AlertNoticeSchema() { 
	    super(FROM);
	}


	public static final AlertNoticeSchema instance = new AlertNoticeSchema();
 
	public static final int MSG_ROUTESLA_100 = 0x00000000; //Group/OpenTempl/5
	public static final int MSG_ROUTESLA_100_FIELD_CONNECTION_1 = 0x00800001; //LongUnsigned/None/0
	public static final int MSG_ROUTESLA_100_FIELD_SEQUENCE_2 = 0x00800003; //LongUnsigned/None/1
	public static final int MSG_ROUTESLA_100_FIELD_DURATION_2 = 0x00800005; //LongUnsigned/None/1
	public static final int MSG_ROUTESLA_100_FIELD_ROUTE_4 = 0x00000007; //IntegerUnsigned/None/0

	public static void consume(Pipe<AlertNoticeSchema> input) {
	    while (PipeReader.tryReadFragment(input)) {
	        int msgIdx = PipeReader.getMsgIdx(input);
	        switch(msgIdx) {
	            case MSG_ROUTESLA_100:
	                consumeRouteSLA(input);
	            break;
	            case -1:
	               //requestShutdown();
	            break;
	        }
	        PipeReader.releaseReadLock(input);
	    }
	}

	public static void consumeRouteSLA(Pipe<AlertNoticeSchema> input) {
	    long fieldConnection = PipeReader.readLong(input,MSG_ROUTESLA_100_FIELD_CONNECTION_1);
	    long fieldSequence = PipeReader.readLong(input,MSG_ROUTESLA_100_FIELD_SEQUENCE_2);
	    long fieldDuration = PipeReader.readLong(input,MSG_ROUTESLA_100_FIELD_DURATION_2);
	    int fieldRoute = PipeReader.readInt(input,MSG_ROUTESLA_100_FIELD_ROUTE_4);
	}

	public static void publishRouteSLA(Pipe<AlertNoticeSchema> output, long fieldConnection, long fieldSequence, long fieldDuration, int fieldRoute) {
	        PipeWriter.presumeWriteFragment(output, MSG_ROUTESLA_100);
	        PipeWriter.writeLong(output,MSG_ROUTESLA_100_FIELD_CONNECTION_1, fieldConnection);
	        PipeWriter.writeLong(output,MSG_ROUTESLA_100_FIELD_SEQUENCE_2, fieldSequence);
	        PipeWriter.writeLong(output,MSG_ROUTESLA_100_FIELD_DURATION_2, fieldDuration);
	        PipeWriter.writeInt(output,MSG_ROUTESLA_100_FIELD_ROUTE_4, fieldRoute);
	        PipeWriter.publishWrites(output);
	}
}
