package com.ociweb;


import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class ConnectionData extends MessageSchema<ConnectionData> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400003,0x90000000,0x80000000,0xc0200003},
		    (short)0,
		    new String[]{"ConnectionData","ConnectionId","SequenceNo",null},
		    new long[]{1, 11, 12, 0},
		    new String[]{"global",null,null,null},
		    "ConnectionDataSchema.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});


		public ConnectionData() { 
		    super(FROM);
		}

		protected ConnectionData(FieldReferenceOffsetManager from) { 
		    super(from);
		}

		public static final ConnectionData instance = new ConnectionData();

		public static final int MSG_CONNECTIONDATA_1 = 0x00000000; //Group/OpenTempl/3
		public static final int MSG_CONNECTIONDATA_1_FIELD_CONNECTIONID_11 = 0x00800001; //LongUnsigned/None/0
		public static final int MSG_CONNECTIONDATA_1_FIELD_SEQUENCENO_12 = 0x00000003; //IntegerUnsigned/None/0

		public static void consume(Pipe<ConnectionData> input) {
		    while (PipeReader.tryReadFragment(input)) {
		        int msgIdx = PipeReader.getMsgIdx(input);
		        switch(msgIdx) {
		            case MSG_CONNECTIONDATA_1:
		                consumeConnectionData(input);
		            break;
		            case -1:
		               //requestShutdown();
		            break;
		        }
		        PipeReader.releaseReadLock(input);
		    }
		}

		public static void consumeConnectionData(Pipe<ConnectionData> input) {
		    long fieldConnectionId = PipeReader.readLong(input,MSG_CONNECTIONDATA_1_FIELD_CONNECTIONID_11);
		    int fieldSequenceNo = PipeReader.readInt(input,MSG_CONNECTIONDATA_1_FIELD_SEQUENCENO_12);
		}

		public static void publishConnectionData(Pipe<ConnectionData> output, long fieldConnectionId, int fieldSequenceNo) {
		        PipeWriter.presumeWriteFragment(output, MSG_CONNECTIONDATA_1);
		        PipeWriter.writeLong(output,MSG_CONNECTIONDATA_1_FIELD_CONNECTIONID_11, fieldConnectionId);
		        PipeWriter.writeInt(output,MSG_CONNECTIONDATA_1_FIELD_SEQUENCENO_12, fieldSequenceNo);
		        PipeWriter.publishWrites(output);
		}
}
