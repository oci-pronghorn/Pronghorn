package com.ociweb.pronghorn.stage.math;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class ProbabilitySchema extends MessageSchema<ProbabilitySchema>{

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400004,0x90000000,0x80000000,0xb8000000,0xc0200004},
		    (short)0,
		    new String[]{"Selection","TotalSum","TotalBuckets","OrderedSelections",null},
		    new long[]{1, 12, 13, 14, 0},
		    new String[]{"global",null,null,null,null},
		    "Probability.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});


		public ProbabilitySchema() { 
		    super(FROM);
		}

		protected ProbabilitySchema(FieldReferenceOffsetManager from) { 
		    super(from);
		}

		public static final ProbabilitySchema instance = new ProbabilitySchema();

		public static final int MSG_SELECTION_1 = 0x00000000; //Group/OpenTempl/4
		public static final int MSG_SELECTION_1_FIELD_TOTALSUM_12 = 0x00800001; //LongUnsigned/None/0
		public static final int MSG_SELECTION_1_FIELD_TOTALBUCKETS_13 = 0x00000003; //IntegerUnsigned/None/0
		public static final int MSG_SELECTION_1_FIELD_ORDEREDSELECTIONS_14 = 0x01c00004; //ByteVector/None/0

		public static void consume(Pipe<ProbabilitySchema> input) {
		    while (PipeReader.tryReadFragment(input)) {
		        int msgIdx = PipeReader.getMsgIdx(input);
		        switch(msgIdx) {
		            case MSG_SELECTION_1:
		                consumeSelection(input);
		            break;
		            case -1:
		               //requestShutdown();
		            break;
		        }
		        PipeReader.releaseReadLock(input);
		    }
		}

		public static void consumeSelection(Pipe<ProbabilitySchema> input) {
		    long fieldTotalSum = PipeReader.readLong(input,MSG_SELECTION_1_FIELD_TOTALSUM_12);
		    int fieldTotalBuckets = PipeReader.readInt(input,MSG_SELECTION_1_FIELD_TOTALBUCKETS_13);
		    DataInputBlobReader<ProbabilitySchema> fieldOrderedSelections = PipeReader.inputStream(input, MSG_SELECTION_1_FIELD_ORDEREDSELECTIONS_14);
		}

		public static void publishSelection(Pipe<ProbabilitySchema> output, long fieldTotalSum, int fieldTotalBuckets, byte[] fieldOrderedSelectionsBacking, int fieldOrderedSelectionsPosition, int fieldOrderedSelectionsLength) {
		        PipeWriter.presumeWriteFragment(output, MSG_SELECTION_1);
		        PipeWriter.writeLong(output,MSG_SELECTION_1_FIELD_TOTALSUM_12, fieldTotalSum);
		        PipeWriter.writeInt(output,MSG_SELECTION_1_FIELD_TOTALBUCKETS_13, fieldTotalBuckets);
		        PipeWriter.writeBytes(output,MSG_SELECTION_1_FIELD_ORDEREDSELECTIONS_14, fieldOrderedSelectionsBacking, fieldOrderedSelectionsPosition, fieldOrderedSelectionsLength);
		        PipeWriter.publishWrites(output);
		}
}
