package com.ociweb.pronghorn.stage.phast;

import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.Pipe.PaddedLong;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class PhastEncodeStage extends PronghornStage {
    
    private final Pipe<PhastCodecSchema> input;
    private final Pipe<RawDataSchema> output;
    
    private DataOutputBlobWriter<RawDataSchema> writer;
    private short[] lengthLookup;
    private int[] sizeLookup;
    private final static int OUTPUT_MAX_MSG_SIZE = RawDataSchema.FROM.fragDataSize[RawDataSchema.MSG_CHUNKEDSTREAM_1];
    private final int packedBatches;
    private int batchCount;
    private long sum = 0;
    private long inputTail;
    private long outputHead;
    private static final int MAX_BYTES_PER_VALUE = 10;
    private static final int INPUT_MAX_MSG_SIZE = FieldReferenceOffsetManager.maxFragmentSize(PhastCodecSchema.FROM);
    
    protected PhastEncodeStage(GraphManager graphManager, Pipe<PhastCodecSchema> input, Pipe<RawDataSchema> output, int chunkSize) {
        super(graphManager, input, output);
        this.input = input;
        this.output = output;
        
        int maxBat = output.maxAvgVarLen/(PhastCodecSchema.FROM.messageStarts.length*MAX_BYTES_PER_VALUE);
        if (maxBat < chunkSize) {
            throw new UnsupportedOperationException("Make var data bigger on output pipe by "+(chunkSize/(float)maxBat));
        }
        
        this.packedBatches = chunkSize;  
        this.inputTail = Pipe.tailPosition(input);
                
        assert( packedBatches>=1) : "need room for 30 values"; 
        
        //Does not need batching to be done by producer or consumer
    //    this.supportsBatchedPublish = false;
        this.supportsBatchedRelease = false;
        
    }
    
    long startup;
    
    @Override
    public void startup() {
        startup = System.currentTimeMillis();
        
        writer = new DataOutputBlobWriter<RawDataSchema>(output);
        int i = PhastCodecSchema.FROM.fieldIdScript.length;
        lengthLookup = new short[i];
        sizeLookup = new int[i];
        while (--i>=0) {
            lengthLookup[i] = (short) (PhastCodecSchema.FROM.fieldIdScript[i]-10000);
            sizeLookup[i] = PhastCodecSchema.FROM.fragDataSize[i];
        }
        outputHead = Pipe.headPosition(output);
        
        
    }
    
    @Override
    public void shutdown() {
        long duration = System.currentTimeMillis() - startup;
        
        long mpbs = (8*8*sum)/(duration*1000);
        
        System.out.println("encoder bytes read:"+(8*sum)+" mbps "+mpbs);
    }

    @Override
    public void run() {

        
        //how much room is there for writing?
        DataOutputBlobWriter<RawDataSchema> localWriter = writer;
    
        long head = Pipe.headPosition(input);               
               
        long localInputTail = inputTail;
        int count = (int) Math.min((output.sizeOfSlabRing-(outputHead-Pipe.tailPosition(output)))/OUTPUT_MAX_MSG_SIZE, (head-localInputTail)/INPUT_MAX_MSG_SIZE);
        if (0==count) {
            return;
        }
        
        int[] inputSlab = Pipe.slab(input);     
        int inputMask = Pipe.slabMask(input);
        short[] localLookup = lengthLookup;

        int[] outputSlab = Pipe.slab(output);
        int outputMask = Pipe.slabMask(output);
        
        int localSum = 0;
        PaddedLong wrkHheadPos = Pipe.getWorkingHeadPositionObject(output);

        int i = count;
        while (--i>=0) {
            
            int msgIdx = inputSlab[(int)(inputMask & localInputTail++)];
            int localFieldCount = (int) localLookup[msgIdx];
            localSum += localFieldCount;
                        
            writeChunk(inputSlab, inputMask, localWriter, outputSlab, outputMask, localInputTail, wrkHheadPos, localFieldCount);
                        
            localInputTail = localInputTail + (localFieldCount<<1) + 1L; //one to skip the byte count
 
        }

        inputTail = localInputTail;
        outputHead = wrkHheadPos.value;
        //avoid direct modification of head or tail because it will contend with other end and slow the throughput.
        Pipe.publishHeadPositions(output);         
        Pipe.batchedReleasePublish(input, 0 , localInputTail);

        sum += localSum;
       
    }

    private void writeChunk(int[] inputSlab, int inputMask, DataOutputBlobWriter<RawDataSchema> localWriter,
            int[] outputSlab, int outputMask, long localInputTail, PaddedLong wrkHheadPos, int localFieldCount) {
        
        if (0 == batchCount) {
            Pipe.markBytesWriteBase(output);            
            outputSlab[outputMask & (int) wrkHheadPos.value++] = RawDataSchema.MSG_CHUNKEDSTREAM_1;
            localWriter.openField();
            
        }
        
        packAllFields(localWriter, (int)localInputTail, localFieldCount, inputSlab, inputMask);
        
        if (++batchCount >= packedBatches) {            
            int len = localWriter.closeLowLevelField(); //side effect it writes pos and len before the final byte count.
            outputSlab[outputMask & (int) wrkHheadPos.value++] = len;
            batchCount = 0;
        }
        
    }
    
    //TODO: convert to use longs instead.

    private void packAllFields(DataOutputBlobWriter<RawDataSchema> localWriter, 
                               int pos,
                               int i, int[] slab, int mask) {
            while (--i >= 0) {
                    
                    long value = (((long) slab[mask & pos]) << 32) | (((long) slab[mask & (1+pos)]) & 0xFFFFFFFFl);
                    pos+=2;
                    DataOutputBlobWriter.writePackedLong(localWriter, value);
            }

    }

}
