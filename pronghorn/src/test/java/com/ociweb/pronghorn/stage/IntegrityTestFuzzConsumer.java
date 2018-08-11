package com.ociweb.pronghorn.stage;

import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;

/**
 * Created by jake on 12/11/16.
 */
public class IntegrityTestFuzzConsumer extends PronghornStage {

    Pipe<MessageSchemaDynamic> input;
    StringBuilder result;

    public IntegrityTestFuzzConsumer(GraphManager gm, Pipe<MessageSchemaDynamic> input, StringBuilder result){
        super(gm,input,NONE);
        this.input = input;
        this.result = result;
    }

    @Override
    public void run(){
        while(Pipe.contentRemaining(input) > 0){
            switch(Pipe.takeMsgIdx(input)){
                case 0:
                    result.setLength(0);

                    result.append(Pipe.takeInt(input) + ",");
                    result.append(Pipe.takeInt(input) + ",");
                    result.append(Pipe.takeInt(input) + ",");
                    result.append(Pipe.takeInt(input) + ",");

                    result.append(Pipe.takeLong(input) + ",");
                    result.append(Pipe.takeLong(input) + ",");
                    result.append(Pipe.takeLong(input) + ",");
                    result.append(Pipe.takeLong(input));

                    StringBuilder optional = new StringBuilder();
                    StringBuilder notOptional = new StringBuilder();

                    Pipe.readASCII(input, Appendables.truncate(optional), Pipe.takeByteArrayMetaData(input), Pipe.takeByteArrayLength(input));
                    Pipe.readASCII(input, Appendables.truncate(notOptional), Pipe.takeByteArrayMetaData(input), Pipe.takeByteArrayLength(input));

                    Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, 0));
                    Pipe.releaseReadLock(input);
                default:
                    requestShutdown();
            }
        }
    }
}
