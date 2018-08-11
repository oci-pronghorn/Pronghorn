package com.ociweb.pronghorn.stage.test;

import java.util.Random;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.stream.StreamingVisitorWriter;
import com.ociweb.pronghorn.pipe.stream.StreamingWriteVisitor;
import com.ociweb.pronghorn.pipe.stream.StreamingWriteVisitorGenerator;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class TestGenerator extends PronghornStage {

    /*
     * API Decision: 
     *                Do not know the fields at compile time -> visitors or low level
     *                Only 1 message type -> Low level easier
     *                Specific business logic on fields -> High level perhaps visitor
     *                Complex nested structure -> not low level.
     *                Bulk data routing -> low level, perhaps high level.
     *                Object event interfaces? for existing API methods that reqire objects
     *                write/read fields in order -> low level or visitor
     *                write/read out of order -> high level
     *                Other: Code generated POJOs - Objects that need to leave scope, 
     *                Other: Code generated Flyweight - beter performance than proxy.
     *                
     *                
     *                                 Performant    RuntimeFields   Nested  Objects  DataRoute  OutOfOrder  ManyMessages
     *                LowLevel             X              X                              X                    
     *                HighLevel            X              X/           X                 X          X             X
     *                Visitor              X/             X            X                                          X
     *                Proxy                                                     X                   X
     *                
     *                DTO - POJO                                       ?        X                   X             X
     *                GenFly               X                           ?        X                   X             
     *                
     *                
     *                
     *                LowLevel  ->  Visitor
     *                HighLievel -> Proxy
     *                           -> DTO
     *                           -> GenFly
     *                
     *                
     *                
     *                
     *                
     *                
     *  
     * 
     */
    
    private final StreamingVisitorWriter writer;
    private int iterations;
    
    public TestGenerator(GraphManager gm, long seed, int iterations, Pipe output) {
        super(gm, NONE, output);
        
        this.iterations = iterations;
        StreamingWriteVisitor visitor = new StreamingWriteVisitorGenerator(Pipe.from(output), new Random(seed), 
                                           output.maxVarLen>>3,  //room for UTF8 
                                           output.maxVarLen>>1); //just use half       
        this.writer = new StreamingVisitorWriter(output, visitor  );
        
    }

    
    @Override
    public void startup() {      
        writer.startup();
    }
    
    @Override
    public void run() {
        if (--iterations>0) {
            writer.run();
        } else {
            requestShutdown();
        }
    }
    
    @Override
    public void shutdown() {
        writer.shutdown();
    }

}
