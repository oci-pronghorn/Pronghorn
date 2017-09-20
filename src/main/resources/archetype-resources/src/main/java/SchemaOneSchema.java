package ${package};


import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class SchemaOneSchema extends MessageSchema<SchemaOneSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400004,0x80000000,0x90000000,0xb8000000,0xc0200004,0xc0400003,0x88000001,0x98000001,0xc0200003},
		    (short)0,
		    new String[]{"SomeKindOfMesage","AnUnsignedInt","AnUnsignedLong","AnArrayOfBytes",null,"SomeOtherMesage",
		    "ASignedInt","ASignedLong",null},
		    new long[]{1, 101, 201, 301, 0, 2, 102, 202, 0},
		    new String[]{"global",null,null,null,null,"global",null,null,null},
		    "SchemaOne.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});


		protected SchemaOneSchema() { 
		    super(FROM);
		}

		public static final SchemaOneSchema instance = new SchemaOneSchema();

		public static final int MSG_SOMEKINDOFMESAGE_1 = 0x00000000; //Group/OpenTempl/4
		public static final int MSG_SOMEKINDOFMESAGE_1_FIELD_ANUNSIGNEDINT_101 = 0x00000001; //IntegerUnsigned/None/0
		public static final int MSG_SOMEKINDOFMESAGE_1_FIELD_ANUNSIGNEDLONG_201 = 0x00800002; //LongUnsigned/None/0
		public static final int MSG_SOMEKINDOFMESAGE_1_FIELD_ANARRAYOFBYTES_301 = 0x01c00004; //ByteVector/None/0
		public static final int MSG_SOMEOTHERMESAGE_2 = 0x00000005; //Group/OpenTempl/3
		public static final int MSG_SOMEOTHERMESAGE_2_FIELD_ASIGNEDINT_102 = 0x00400001; //IntegerSigned/None/1
		public static final int MSG_SOMEOTHERMESAGE_2_FIELD_ASIGNEDLONG_202 = 0x00c00002; //LongSigned/None/1


		public static void consume(Pipe<SchemaOneSchema> input) {
		    while (PipeReader.tryReadFragment(input)) {
		        int msgIdx = PipeReader.getMsgIdx(input);
		        switch(msgIdx) {
		            case MSG_SOMEKINDOFMESAGE_1:
		                consumeSomeKindOfMesage(input);
		            break;
		            case MSG_SOMEOTHERMESAGE_2:
		                consumeSomeOtherMesage(input);
		            break;
		            case -1:
		               //requestShutdown();
		            break;
		        }
		        PipeReader.releaseReadLock(input);
		    }
		}

		public static void consumeSomeKindOfMesage(Pipe<SchemaOneSchema> input) {
		    int fieldAnUnsignedInt = PipeReader.readInt(input,MSG_SOMEKINDOFMESAGE_1_FIELD_ANUNSIGNEDINT_101);
		    long fieldAnUnsignedLong = PipeReader.readLong(input,MSG_SOMEKINDOFMESAGE_1_FIELD_ANUNSIGNEDLONG_201);
		    DataInputBlobReader<SchemaOneSchema> fieldAnArrayOfBytes = PipeReader.inputStream(input, MSG_SOMEKINDOFMESAGE_1_FIELD_ANARRAYOFBYTES_301);
		}
		public static void consumeSomeOtherMesage(Pipe<SchemaOneSchema> input) {
		    int fieldASignedInt = PipeReader.readInt(input,MSG_SOMEOTHERMESAGE_2_FIELD_ASIGNEDINT_102);
		    long fieldASignedLong = PipeReader.readLong(input,MSG_SOMEOTHERMESAGE_2_FIELD_ASIGNEDLONG_202);
		}

		public static void publishSomeKindOfMesage(Pipe<SchemaOneSchema> output, int fieldAnUnsignedInt, long fieldAnUnsignedLong, byte[] fieldAnArrayOfBytesBacking, int fieldAnArrayOfBytesPosition, int fieldAnArrayOfBytesLength) {
		        PipeWriter.presumeWriteFragment(output, MSG_SOMEKINDOFMESAGE_1);
		        PipeWriter.writeInt(output,MSG_SOMEKINDOFMESAGE_1_FIELD_ANUNSIGNEDINT_101, fieldAnUnsignedInt);
		        PipeWriter.writeLong(output,MSG_SOMEKINDOFMESAGE_1_FIELD_ANUNSIGNEDLONG_201, fieldAnUnsignedLong);
		        PipeWriter.writeBytes(output,MSG_SOMEKINDOFMESAGE_1_FIELD_ANARRAYOFBYTES_301, fieldAnArrayOfBytesBacking, fieldAnArrayOfBytesPosition, fieldAnArrayOfBytesLength);
		        PipeWriter.publishWrites(output);
		}
		public static void publishSomeOtherMesage(Pipe<SchemaOneSchema> output, int fieldASignedInt, long fieldASignedLong) {
		        PipeWriter.presumeWriteFragment(output, MSG_SOMEOTHERMESAGE_2);
		        PipeWriter.writeInt(output,MSG_SOMEOTHERMESAGE_2_FIELD_ASIGNEDINT_102, fieldASignedInt);
		        PipeWriter.writeLong(output,MSG_SOMEOTHERMESAGE_2_FIELD_ASIGNEDLONG_202, fieldASignedLong);
		        PipeWriter.publishWrites(output);
		}
}
