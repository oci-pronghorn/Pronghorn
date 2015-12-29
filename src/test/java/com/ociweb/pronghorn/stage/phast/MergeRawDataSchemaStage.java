package com.ociweb.pronghorn.stage.phast;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.Pipe.PaddedLong;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class MergeRawDataSchemaStage extends PronghornStage {

    private final Pipe<RawDataSchema>[] inputs;
    private final Pipe<RawDataSchema> output;
    private int pipeIdx = 0;
    
    private final static int OUTPUT_MAX_MSG_SIZE = RawDataSchema.FROM.fragDataSize[RawDataSchema.MSG_CHUNKEDSTREAM_1];
    
    private PaddedLong outputHead;
    private PaddedLong[] inputTail;
    
    protected MergeRawDataSchemaStage(GraphManager graphManager, Pipe<RawDataSchema>[] inputs, Pipe<RawDataSchema> output) {
        super(graphManager, inputs, output);
        this.inputs = inputs;
        this.output = output;
        
        supportsBatchedPublish = false;
        supportsBatchedRelease  = false;
        
    }
    
    @Override
    public void startup() {
        this.outputHead = Pipe.getWorkingHeadPositionObject(output);
        
        int i = inputs.length;
        this.inputTail = new PaddedLong[i];
        while (--i >= 0) {
            inputTail[i] = Pipe.getWorkingTailPositionObject(inputs[i]);
        }
        
    }

    long outputCount;
    
    @Override
    public void run() {
        
        
        if (outputCount<=0) {
            outputCount = output.sizeOfSlabRing-(outputHead.value-Pipe.tailPosition(output));
        }
        
        int[] outputSlab = Pipe.slab(output);
        int outputMask = Pipe.slabMask(output);                     
        
        Pipe<RawDataSchema> activeInput = inputs[pipeIdx];
        PaddedLong localTail = inputTail[pipeIdx];
        long inputCount = activeInput.sizeOfSlabRing-(localTail.value-Pipe.headPosition(activeInput));
        
        
        while ((outputCount-=OUTPUT_MAX_MSG_SIZE) >= 0  && (inputCount>=OUTPUT_MAX_MSG_SIZE) ) {
                               
                int inputMask = Pipe.slabMask(activeInput); 
                int[] inputSlab = Pipe.slab(activeInput);
                int inputMsgIdx = inputSlab[inputMask & (int) localTail.value++];    
                
                if (inputMsgIdx<0) {
                    requestShutdown();
                    return;
                }
                            
                Pipe.markBytesWriteBase(output);            
                outputSlab[outputMask & (int) outputHead.value++] = RawDataSchema.MSG_CHUNKEDSTREAM_1;      
                
                int inputMeta = Pipe.takeRingByteMetaData(activeInput);
                int inputLength    = Pipe.takeRingByteLen(activeInput);
                Pipe.addByteArrayWithMask(output,Pipe.blobMask(activeInput), inputLength, Pipe.blob(activeInput), Pipe.bytePosition(inputMeta, activeInput, inputLength));                  
                                
                outputSlab[outputMask & (int) outputHead.value++] = inputLength;
                
                Pipe.publishHeadPositions(output);
                Pipe.markBytesReadBase(activeInput, inputSlab[inputMask & (int) localTail.value++]);
                Pipe.batchedReleasePublish(activeInput, Pipe.getWorkingBlobRingTailPosition(activeInput), localTail.value);
                
                if (++pipeIdx >= inputs.length) {
                    pipeIdx = 0;
                }
                activeInput = inputs[pipeIdx];
                localTail = inputTail[pipeIdx];
                inputCount = activeInput.sizeOfSlabRing-(localTail.value-Pipe.headPosition(activeInput));
        }
        
    }

    
}
