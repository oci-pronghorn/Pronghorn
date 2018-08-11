package com.ociweb.pronghorn.stage.test;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.stream.StreamingReadVisitor;
import com.ociweb.pronghorn.pipe.stream.StreamingReadVisitorMatcher;
import com.ociweb.pronghorn.pipe.stream.StreamingVisitorReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class TestValidator<T extends MessageSchema<T>> extends PronghornStage {
    
    private final Pipe<T> expectedInput;
    private final Pipe<T> checkedInput;
    
    private final StreamingVisitorReader reader;
    
    public TestValidator(GraphManager gm, @SuppressWarnings("unchecked") Pipe<T> ... input) {
        super(gm, input, NONE);
        
        this.expectedInput = input[0];
        this.checkedInput = input[1];
                
        StreamingReadVisitor visitor = new StreamingReadVisitorMatcher(expectedInput);
        
        this.reader = new StreamingVisitorReader(checkedInput,  visitor); //*/ new StreamingReadVisitorDebugDelegate(visitor));////visitor);
        
    }

    @Override
    public void startup() {       
        reader.startup();
    }
        
    @Override
    public void run() {
        reader.run();
    }    

    @Override
    public void shutdown() {
        reader.shutdown();
    }

}
