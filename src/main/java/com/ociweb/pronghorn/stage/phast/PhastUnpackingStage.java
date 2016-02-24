package com.ociweb.pronghorn.stage.phast;

import java.io.IOException;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.Pipe.PaddedLong;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class PhastUnpackingStage extends PronghornStage {

    private final Pipe<RawDataSchema> input;
    private final Pipe<PhastCodecSchema> output;
    
    private DataInputBlobReader<RawDataSchema> reader;
    private int[] idxReverseLookup;
    private int fieldCount = 0;
    private int recentMsgIdxPos = -1;
    private PaddedLong inputWorkingTail;
    private int inputBlobWorkingTail;
    private PaddedLong outputWorkingHead;
    private static final int MAX_FIELD_SIZE = FieldReferenceOffsetManager.maxFragmentSize(PhastCodecSchema.FROM);
    private int MAX_INT_FIELDS = 63; //TODO: SSET FROM SCHEMA.
    
    protected PhastUnpackingStage(GraphManager graphManager, Pipe<RawDataSchema> input, Pipe<PhastCodecSchema> output) {
        super(graphManager, input, output);
        this.input = input;
        this.output = output;
    }

    @Override
    public void startup() {
        reader = new DataInputBlobReader<RawDataSchema>(input);
        
        int maxValue = PhastCodecSchema.FROM.messageStarts.length+1;
        idxReverseLookup = new int[maxValue];
        int i = PhastCodecSchema.FROM.fieldIdScript.length;
        while (--i>=0) {
            long id = PhastCodecSchema.FROM.fieldIdScript[i];
            if (id>10000) {
                idxReverseLookup[(int)(id-10000)] =  i;
            }
        }           
        inputWorkingTail = Pipe.getWorkingTailPositionObject(input);
        inputBlobWorkingTail = Pipe.getWorkingBlobRingTailPosition(input);
        outputWorkingHead = Pipe.getWorkingHeadPositionObject(output);
        
    }    
    
    
    @Override
    public void run() {
        
        
        int[] inputSlab = Pipe.slab(input);
        int inputMask = Pipe.slabMask(input);
        
        int[] outputSlab = Pipe.slab(output);
        int outputMask = Pipe.slabMask(output);
                            
        long head = Pipe.headPosition(input);

        while (inputWorkingTail.value < head) {
            try {
            
                if (0==reader.available()) {
                    int msgIdx = inputSlab[inputMask & (int) inputWorkingTail.value++];
                    if (msgIdx<0) {
                        requestShutdown();
                        return;
                    }
                    int len = reader.openLowLevelAPIField();
                    inputBlobWorkingTail += len;
                }                
                
                int pos = (int)outputWorkingHead.value;
                int localFieldCount = fieldCount;
                DataInputBlobReader<RawDataSchema> localReader = reader;
                Pipe<PhastCodecSchema> localOutput = output;
                while (localReader.hasRemainingBytes() && (0!=localFieldCount || (localOutput.sizeOfSlabRing - Pipe.contentRemaining(localOutput)>=32))) {//TODO optimize.
                
                    if (0==localFieldCount) {  
                      //  Pipe.markBytesWriteBase(output);
                        //start new message
                        int mPos = outputMask & pos++;
                        outputSlab[mPos] = PhastCodecSchema.MSG_MAX_FIELDS;
                        recentMsgIdxPos = mPos;
                        
                    }
                    
                    //System.out.println("field count "+localFieldCount);
                    
                    long value = DataInputBlobReader.readPackedLong(localReader) ;
                    
                   // System.out.println("value "+value+ "  "+inputWorkingTail.value+"  "+head);
                    
                    outputSlab[outputMask & pos++] = (int)(value >>> 32);
                    outputSlab[outputMask & pos++] = ((int)value);

                
                    if (++localFieldCount == MAX_INT_FIELDS) {      
                        outputWorkingHead.value += MAX_FIELD_SIZE;
                        
                       //publish message
                          outputSlab[outputMask & pos++] = 0;//zero bytes used
                       
                          Pipe.publishHeadPositions(localOutput);    
                        
                          localFieldCount = 0;
                          
                    } 
                }
                fieldCount = localFieldCount;
                
                if (0==localReader.available()) {
                    int bytesLen = inputSlab[inputMask & (int) inputWorkingTail.value++];
                    
                    Pipe.markBytesReadBase(input, bytesLen);
                } else {
                    //there is no room to write new message
                    return;
                }
                
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
        }
        
        
        if (0!=fieldCount) {           
            outputWorkingHead.value += ((fieldCount<<1)+1);
            outputSlab[outputMask & (int) outputWorkingHead.value++] = 0;//zero bytes used
            //modify the msgIdx since we had to pick a shorter message than normal
            outputSlab[recentMsgIdxPos] = idxReverseLookup[fieldCount];   
            Pipe.publishWritesBatched(output);    
            
            fieldCount = 0;
        }
        
        Pipe.batchedReleasePublish(input, inputBlobWorkingTail, inputWorkingTail.value);
        
                
    }

}
