package com.ociweb.pronghorn.stage.phast;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.Pipe.PaddedLong;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class PhastPackingStage extends PronghornStage {
    
    private final Pipe<PhastCodecSchema> input1;
    private final Pipe<RawDataSchema>    input2;
        
    private final Pipe<RawDataSchema> output;
    
    private DataOutputBlobWriter<RawDataSchema> writer;
    private DataInputBlobReader<RawDataSchema> input2Reader;    
    
    private short[] lengthLookup;
    private int[] sizeLookup;
    private final static int OUTPUT_MAX_MSG_SIZE = RawDataSchema.FROM.fragDataSize[RawDataSchema.MSG_CHUNKEDSTREAM_1];
    private long startup;
    private long totalLongs;
    private int maxBytesPerMessage;
    public static final int ESCAPE_VALUE = -63;//in packed encoding this is the biggest negative value which takes only 1 byte
    
    protected PhastPackingStage(GraphManager graphManager, Pipe<PhastCodecSchema> input1, Pipe<RawDataSchema> input2, Pipe<RawDataSchema> output) {
        super(graphManager, input1, output);
        this.input1 = input1;
        this.input2 = input2;
        this.output = output;

        //Does not need batching to be done by producer or consumer
        this.supportsBatchedPublish = false;
        this.supportsBatchedRelease = false;
    }
    
    
    @Override
    public void startup() {
        startup = System.currentTimeMillis();
        
        input2Reader = new DataInputBlobReader<RawDataSchema>(input2);
        writer = new DataOutputBlobWriter<RawDataSchema>(output);
        int i = PhastCodecSchema.FROM.fieldIdScript.length;
        lengthLookup = new short[i];
        sizeLookup = new int[i];
        while (--i>=0) {
            lengthLookup[i] = (short) (PhastCodecSchema.FROM.fieldIdScript[i]-10000);
            sizeLookup[i] = PhastCodecSchema.FROM.fragDataSize[i];
        }     
        
        int maxFieldsPerMessage = PhastCodecSchema.FROM.messageStarts.length;
        maxBytesPerMessage = maxFieldsPerMessage * 10;

        
    }
    
    @Override
    public void shutdown() {
        long duration = System.currentTimeMillis() - startup;        
        long mpbs = (8*8*totalLongs)/(duration*1000);        
        //System.out.println("encoder unpacked bytes read:"+(8*totalLongs)+" mbps "+mpbs);
    }
    
    @Override
    public void run() {
        pump(input1, input2, output, lengthLookup, writer, input2Reader);      
    }

    private void pump(Pipe<PhastCodecSchema> localInput1,
                     Pipe<RawDataSchema> localInput2,
                     Pipe<RawDataSchema> localOutput, 
                     short[] lookup, 
                     DataOutputBlobWriter<RawDataSchema> localWriter, DataInputBlobReader<RawDataSchema> input2Reader) {

        while (Pipe.hasRoomForWrite(localOutput, OUTPUT_MAX_MSG_SIZE) && (Pipe.hasContentToRead(localInput1) || input2Reader.hasRemainingBytes()) ) {
            
            int size = Pipe.addMsgIdx(localOutput, RawDataSchema.MSG_CHUNKEDSTREAM_1);      
            localWriter.openField();    
            
            //continue writing these bytes because we did not have enough room in the last chunk.
            if (input2Reader.hasRemainingBytes()) {
               int rem =  DataInputBlobReader.bytesRemaining(input2Reader);
               if (rem<=localOutput.maxVarLen) {
                   DataOutputBlobWriter.writeBytes(localWriter,input2Reader,rem);
                   Pipe.releaseReadLock(localInput2);                   
               } else {
                   DataOutputBlobWriter.writeBytes(localWriter,input2Reader,localOutput.maxVarLen);
               }
            }            
            
            //if there is room left in this open outgoing message and if there is new content to add keep going            
            combineContentForSingleMessage( localInput1, localInput2, localOutput, 
                                                      lookup, localWriter, size, 
                                                      maxBytesPerMessage, localOutput.maxVarLen, 
                                                      false, localWriter);
            
            localWriter.closeLowLevelField();
            
            //publish the outgoing message
            Pipe.confirmLowLevelWrite(localOutput, size); 
            Pipe.publishWrites(localOutput);
            
            //The very last read lock must only be released after the write publish, this is to ensure the
            //schedulers do no loose track of this data.
            Pipe.releaseReadLock(localInput1);            
                       
        }              
    }

    private void combineContentForSingleMessage(Pipe<PhastCodecSchema> localInput1,
            Pipe<RawDataSchema> localInput2, Pipe<RawDataSchema> localOutput, short[] lookup,
            DataOutputBlobWriter<RawDataSchema> localWriter, int size, int maxPerMsg, int outputMaxLen,
            boolean holdingReleaseReadLock, DataOutputBlobWriter<RawDataSchema> writer) {
        
        int localSum = 0;
        int avail = 0;
        while ( ( (avail = (outputMaxLen - writer.length())) >= maxPerMsg) && Pipe.hasContentToRead(localInput1)) {
                            
            if (holdingReleaseReadLock) {
                Pipe.releaseReadLock(localInput1);
                holdingReleaseReadLock = false;
            }
            
           int msgIdx = Pipe.takeMsgIdx(localInput1);
           
           if (PhastCodecSchema.MSG_BLOBCHUNK_1000 != msgIdx) {                   
               localSum = writePackedFields(localSum, localInput1, localWriter, msgIdx, (int) lookup[msgIdx]);   
           }  else {
               localSum = writeByteData(localSum, localInput2, avail);                   
           }               
           Pipe.confirmLowLevelRead(localInput1, Pipe.sizeOf(localInput1, msgIdx));
           holdingReleaseReadLock = true;                            
        }    
        assert(holdingReleaseReadLock);
        totalLongs += localSum;
                
    }

    private int writeByteData(int localSum, Pipe<RawDataSchema> localInput2, int roomAvail) {
        //we were told there was content.
           assert (Pipe.hasContentToRead(localInput2));
           
           int msgIdx2 = Pipe.takeMsgIdx(localInput2);
           int length = input2Reader.openLowLevelAPIField();
           localSum += (length>>3);
           
           writer.writePackedInt(ESCAPE_VALUE);
           writer.writePackedInt(length);           

           if (length > roomAvail) {
               DataOutputBlobWriter.writeBytes(writer,input2Reader,roomAvail);
           } else {        
               DataOutputBlobWriter.writeBytes(writer,input2Reader,length);
               //only release when we are fully done
               Pipe.releaseReadLock(localInput2);
           }
           Pipe.confirmLowLevelRead(localInput2, Pipe.sizeOf(localInput2, msgIdx2));
                     
           return localSum;
    }

    private int writePackedFields(int localSum, Pipe<PhastCodecSchema> localInput1,
           DataOutputBlobWriter<RawDataSchema> localWriter, int msgIdx, int localFieldCount) {
       
           int i = localFieldCount;
           //System.err.println("field count "+i);
           while (--i >= 0) {
                packField(localWriter, Pipe.takeLong(localInput1));                    
           }         
           localSum += localFieldCount;
           return localSum;
    }


    private void packField(final DataOutputBlobWriter<RawDataSchema> localWriter, long value) {
        if (ESCAPE_VALUE != value) {
            DataOutputBlobWriter.writePackedLong(localWriter, value);
        } else {
            //TODO: have escape prebuilt as an array of bytes to copy
            DataOutputBlobWriter.writePackedLong(localWriter, ESCAPE_VALUE);
            DataOutputBlobWriter.writePackedLong(localWriter, ESCAPE_VALUE);
        }
    }

}
