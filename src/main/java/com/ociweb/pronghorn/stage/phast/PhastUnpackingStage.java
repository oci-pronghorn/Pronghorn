package com.ociweb.pronghorn.stage.phast;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class PhastUnpackingStage extends PronghornStage {

    private final Pipe<RawDataSchema> input;
    private DataInputBlobReader<RawDataSchema> reader;

    private final Pipe<PhastCodecSchema>                output1;
    private final Pipe<RawDataSchema>                   output2;
    private DataOutputBlobWriter<RawDataSchema>         output2Writer;
    
    private int[] idxReverseLookup;
    private int bytesRemainingToCopy = 0;

    private static final int MAX_FIELD_COUNT = PhastCodecSchema.FROM.messageStarts.length-1;
    private static final int ESCAPE = -63;
  
    protected PhastUnpackingStage(GraphManager graphManager, Pipe<RawDataSchema> input, Pipe<PhastCodecSchema> output1, Pipe<RawDataSchema> output2) {
        super(graphManager, input, new Pipe[]{output1, output2});
        this.input = input;
        this.output1 = output1;
        this.output2 = output2;        
    }

    @Override
    public void startup() {
        reader = new DataInputBlobReader<RawDataSchema>(input);
        output2Writer = new DataOutputBlobWriter<RawDataSchema>(output2);
        
        int maxValue = PhastCodecSchema.FROM.messageStarts.length+1;
        idxReverseLookup = new int[maxValue];
        int i = PhastCodecSchema.FROM.fieldIdScript.length;
        while (--i>=0) {
            long id = PhastCodecSchema.FROM.fieldIdScript[i];
            if (id>10000) {
                idxReverseLookup[(int)(id-10000)] =  i;
            }
        }          

    }    
    
    //TODO: needs testing of byte blocks
    
    @Override
    public void run() {
        
        if (bytesRemainingToCopy>0) {
            if (Pipe.hasRoomForWrite(output1) && Pipe.hasRoomForWrite(output2)) {
                writeBytesToOutput(output1, output2, output2Writer, reader);
            } else {
                return;//try later and read bytesRemainingToCopy
            }
        }
        
        while (Pipe.hasContentToRead(input) || reader.hasRemainingBytes()) {
                        
            if (!reader.hasRemainingBytes()) {
                int msgIdx = Pipe.takeMsgIdx(input);
                Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, msgIdx));
                if (msgIdx>=0) {
                    reader.openLowLevelAPIField(); 
                    if (!reader.hasRemainingBytes()) {
                        Pipe.releaseReadLock(input);
                        continue;
                    }
                } else {
                    requestShutdown();
                    break;
                }
            }
            
            long msgIdxPos = -1;
            
            int localFieldCount = 0;
            while ((localFieldCount>0 || Pipe.hasRoomForWrite(output1)) && reader.hasRemainingBytes()) {
                                                               
                long value = reader.readPackedLong();
                if (ESCAPE == value) {
                    value = reader.readPackedLong();
                    if (ESCAPE != value) {
                        //value now holds the length of the byte run to be copied over.
                        
                        if (localFieldCount>0) { //close the packed value message
                            closePackedFieldsMessage(msgIdxPos, idxReverseLookup[localFieldCount], output1);
                            localFieldCount = 0;
                        }
                        bytesRemainingToCopy = (int)value;
                        
                        if (Pipe.hasRoomForWrite(output1) && Pipe.hasRoomForWrite(output2)) {
                            writeBytesToOutput(output1, output2, output2Writer, reader);
                        } else {
                            return;//try later and read bytesRemainingToCopy
                        }
                        continue;//back to top of the while 
                    }
                }
                
                if (0==localFieldCount) {
                    msgIdxPos = Pipe.getWorkingHeadPositionObject(output1).value;
                    Pipe.addMsgIdx(output1, 0) ;//not sure now so we set it later
                }
                
                Pipe.addLongValue(value, output1);
                 
                if (++localFieldCount == MAX_FIELD_COUNT) { 
                    closePackedFieldsMessage(msgIdxPos, idxReverseLookup[localFieldCount], output1);
                    localFieldCount = 0;
                }
            }
            
            if (localFieldCount>0) {//not a full message but this is all we can do so go with it.
                closePackedFieldsMessage(msgIdxPos, idxReverseLookup[localFieldCount], output1);
                localFieldCount = 0;
            }            
            
            if (!reader.hasRemainingBytes()) {                
                Pipe.releaseReadLock(input);
            }
            
        }
    }

    private void closePackedFieldsMessage(long msgIdxPos, int msgIdx, Pipe<PhastCodecSchema> output1) {
        Pipe.setIntValue(msgIdx, output1, msgIdxPos);
        Pipe.confirmLowLevelWrite(output1, Pipe.sizeOf(output1, msgIdx));
        Pipe.publishWrites(output1);
    }

    private void writeBytesToOutput(Pipe<PhastCodecSchema> output1, Pipe<RawDataSchema> output2, DataOutputBlobWriter<RawDataSchema> output2Writer, DataInputBlobReader<RawDataSchema> reader) {
        Pipe.addMsgIdx(output1, PhastCodecSchema.MSG_BLOBCHUNK_1000);
        Pipe.confirmLowLevelWrite(output1, Pipe.sizeOf(output1, PhastCodecSchema.MSG_BLOBCHUNK_1000));
        
        Pipe.addMsgIdx(output2, RawDataSchema.MSG_CHUNKEDSTREAM_1);
        
        output2Writer.openField();                            
        DataOutputBlobWriter.writeBytes(output2Writer, reader, bytesRemainingToCopy);
        output2Writer.closeLowLevelField();
        
        bytesRemainingToCopy = 0;
        Pipe.confirmLowLevelWrite(output2, Pipe.sizeOf(output2, RawDataSchema.MSG_CHUNKEDSTREAM_1));
        
        Pipe.publishWrites(output2);
        Pipe.publishWrites(output1);
    }

}
