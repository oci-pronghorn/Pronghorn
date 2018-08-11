package com.ociweb.pronghorn.stage.math;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class HistogramSchema extends MessageSchema<HistogramSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400003,0x80000000,0xb8000000,0xc0200003},
		    (short)0,
		    new String[]{"Histogram","BucketsCount","PackedLongs",null},
		    new long[]{1, 12, 13, 0},
		    new String[]{"global",null,null,null},
		    "Histogram.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});


		public HistogramSchema() { 
		    super(FROM);
		}

		protected HistogramSchema(FieldReferenceOffsetManager from) { 
		    super(from);
		}

		public static final HistogramSchema instance = new HistogramSchema();

		public static final int MSG_HISTOGRAM_1 = 0x00000000; //Group/OpenTempl/3
		public static final int MSG_HISTOGRAM_1_FIELD_BUCKETSCOUNT_12 = 0x00000001; //IntegerUnsigned/None/0
		public static final int MSG_HISTOGRAM_1_FIELD_PACKEDLONGS_13 = 0x01c00002; //ByteVector/None/0

		public static void consume(Pipe<HistogramSchema> input) {
		    while (PipeReader.tryReadFragment(input)) {
		        int msgIdx = PipeReader.getMsgIdx(input);
		        switch(msgIdx) {
		            case MSG_HISTOGRAM_1:
		                consumeHistogram(input);
		            break;
		            case -1:
		               //requestShutdown();
		            break;
		        }
		        PipeReader.releaseReadLock(input);
		    }
		}

		public static void consumeHistogram(Pipe<HistogramSchema> input) {
		    int fieldBucketsCount = PipeReader.readInt(input,MSG_HISTOGRAM_1_FIELD_BUCKETSCOUNT_12);
		    DataInputBlobReader<HistogramSchema> fieldPackedLongs = PipeReader.inputStream(input, MSG_HISTOGRAM_1_FIELD_PACKEDLONGS_13);
		}

		public static void publishHistogram(Pipe<HistogramSchema> output, int fieldBucketsCount, byte[] fieldPackedLongsBacking, int fieldPackedLongsPosition, int fieldPackedLongsLength) {
		        PipeWriter.presumeWriteFragment(output, MSG_HISTOGRAM_1);
		        PipeWriter.writeInt(output,MSG_HISTOGRAM_1_FIELD_BUCKETSCOUNT_12, fieldBucketsCount);
		        PipeWriter.writeBytes(output,MSG_HISTOGRAM_1_FIELD_PACKEDLONGS_13, fieldPackedLongsBacking, fieldPackedLongsPosition, fieldPackedLongsLength);
		        PipeWriter.publishWrites(output);
		}
	
}
