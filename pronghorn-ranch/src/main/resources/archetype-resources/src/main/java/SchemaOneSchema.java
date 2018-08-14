package ${package};

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class SchemaOneSchema extends MessageSchema<SchemaOneSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
			new int[]{0xc0400006,0xa8000000,0xa0000001,0x80000000,0x90000000,0xb8000002,0xc0200006,0xc0400003,0x88000001,0x98000001,0xc0200003},
			(short)0,
			new String[]{"SomeKindOfMessage","AUTF8String","AnASCIIString","AnUnsignedInt","AnUnsignedLong",
					"AnArrayOfBytes",null,"SomeOtherMessage","ASignedInt","ASignedLong",null},
			new long[]{1, 99, 100, 102, 201, 301, 0, 2, 103, 202, 0},
			new String[]{"global",null,null,null,null,null,null,"global",null,null,null},
			"SchemaOne.xml",
			new long[]{2, 2, 0},
			new int[]{2, 2, 0});


	public SchemaOneSchema() {
		super(FROM);
	}

	protected SchemaOneSchema(FieldReferenceOffsetManager from) {
		super(from);
	}

	public static final SchemaOneSchema instance = new SchemaOneSchema();

	public static final int MSG_SOMEKINDOFMESSAGE_1 = 0x00000000; //Group/OpenTempl/6
	public static final int MSG_SOMEKINDOFMESSAGE_1_FIELD_AUTF8STRING_99 = 0x01400001; //UTF8/None/0
	public static final int MSG_SOMEKINDOFMESSAGE_1_FIELD_ANASCIISTRING_100 = 0x01000003; //ASCII/None/1
	public static final int MSG_SOMEKINDOFMESSAGE_1_FIELD_ANUNSIGNEDINT_102 = 0x00000005; //IntegerUnsigned/None/0
	public static final int MSG_SOMEKINDOFMESSAGE_1_FIELD_ANUNSIGNEDLONG_201 = 0x00800006; //LongUnsigned/None/0
	public static final int MSG_SOMEKINDOFMESSAGE_1_FIELD_ANARRAYOFBYTES_301 = 0x01c00008; //ByteVector/None/2
	public static final int MSG_SOMEOTHERMESSAGE_2 = 0x00000007; //Group/OpenTempl/3
	public static final int MSG_SOMEOTHERMESSAGE_2_FIELD_ASIGNEDINT_103 = 0x00400001; //IntegerSigned/None/1
	public static final int MSG_SOMEOTHERMESSAGE_2_FIELD_ASIGNEDLONG_202 = 0x00c00002; //LongSigned/None/1

	public static void consume(Pipe<SchemaOneSchema> input) {
		while (PipeReader.tryReadFragment(input)) {
			int msgIdx = PipeReader.getMsgIdx(input);
			switch(msgIdx) {
				case MSG_SOMEKINDOFMESSAGE_1:
					consumeSomeKindOfMessage(input);
					break;
				case MSG_SOMEOTHERMESSAGE_2:
					consumeSomeOtherMessage(input);
					break;
				case -1:
					//requestShutdown();
					break;
			}
			PipeReader.releaseReadLock(input);
		}
	}

	public static void consumeSomeKindOfMessage(Pipe<SchemaOneSchema> input) {
		StringBuilder fieldAUTF8String = PipeReader.readUTF8(input,MSG_SOMEKINDOFMESSAGE_1_FIELD_AUTF8STRING_99,new StringBuilder(PipeReader.readBytesLength(input,MSG_SOMEKINDOFMESSAGE_1_FIELD_AUTF8STRING_99)));
		StringBuilder fieldAnASCIIString = PipeReader.readUTF8(input,MSG_SOMEKINDOFMESSAGE_1_FIELD_ANASCIISTRING_100,new StringBuilder(PipeReader.readBytesLength(input,MSG_SOMEKINDOFMESSAGE_1_FIELD_ANASCIISTRING_100)));
		int fieldAnUnsignedInt = PipeReader.readInt(input,MSG_SOMEKINDOFMESSAGE_1_FIELD_ANUNSIGNEDINT_102);
		long fieldAnUnsignedLong = PipeReader.readLong(input,MSG_SOMEKINDOFMESSAGE_1_FIELD_ANUNSIGNEDLONG_201);
		DataInputBlobReader<SchemaOneSchema> fieldAnArrayOfBytes = PipeReader.inputStream(input, MSG_SOMEKINDOFMESSAGE_1_FIELD_ANARRAYOFBYTES_301);
	}
	public static void consumeSomeOtherMessage(Pipe<SchemaOneSchema> input) {
		int fieldASignedInt = PipeReader.readInt(input,MSG_SOMEOTHERMESSAGE_2_FIELD_ASIGNEDINT_103);
		long fieldASignedLong = PipeReader.readLong(input,MSG_SOMEOTHERMESSAGE_2_FIELD_ASIGNEDLONG_202);
	}

	public static void publishSomeKindOfMessage(Pipe<SchemaOneSchema> output, CharSequence fieldAUTF8String, CharSequence fieldAnASCIIString, int fieldAnUnsignedInt, long fieldAnUnsignedLong, byte[] fieldAnArrayOfBytesBacking, int fieldAnArrayOfBytesPosition, int fieldAnArrayOfBytesLength) {
		PipeWriter.presumeWriteFragment(output, MSG_SOMEKINDOFMESSAGE_1);
		PipeWriter.writeUTF8(output,MSG_SOMEKINDOFMESSAGE_1_FIELD_AUTF8STRING_99, fieldAUTF8String);
		PipeWriter.writeUTF8(output,MSG_SOMEKINDOFMESSAGE_1_FIELD_ANASCIISTRING_100, fieldAnASCIIString);
		PipeWriter.writeInt(output,MSG_SOMEKINDOFMESSAGE_1_FIELD_ANUNSIGNEDINT_102, fieldAnUnsignedInt);
		PipeWriter.writeLong(output,MSG_SOMEKINDOFMESSAGE_1_FIELD_ANUNSIGNEDLONG_201, fieldAnUnsignedLong);
		PipeWriter.writeBytes(output,MSG_SOMEKINDOFMESSAGE_1_FIELD_ANARRAYOFBYTES_301, fieldAnArrayOfBytesBacking, fieldAnArrayOfBytesPosition, fieldAnArrayOfBytesLength);
		PipeWriter.publishWrites(output);
	}
	public static void publishSomeOtherMessage(Pipe<SchemaOneSchema> output, int fieldASignedInt, long fieldASignedLong) {
		PipeWriter.presumeWriteFragment(output, MSG_SOMEOTHERMESSAGE_2);
		PipeWriter.writeInt(output,MSG_SOMEOTHERMESSAGE_2_FIELD_ASIGNEDINT_103, fieldASignedInt);
		PipeWriter.writeLong(output,MSG_SOMEOTHERMESSAGE_2_FIELD_ASIGNEDLONG_202, fieldASignedLong);
		PipeWriter.publishWrites(output);
	}

}