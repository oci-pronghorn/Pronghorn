package com.ociweb.pronghorn.code;

import java.util.Random;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.stream.StreamingVisitorWriter;
import com.ociweb.pronghorn.pipe.stream.StreamingWriteVisitor;
import com.ociweb.pronghorn.pipe.stream.StreamingWriteVisitorGenerator;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

/**
 * Generates fuzz for testing.
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class FuzzGeneratorStage extends PronghornStage{

    private final StreamingVisitorWriter writer;
    private final long duration;
    private long timeLimit;

    /**
     *
     * @param gm
     * @param random
     * @param duration
     * @param output _out_ Pipe onto which fuzz will be written.
     */
    public FuzzGeneratorStage(GraphManager gm, Random random, long duration, Pipe output) {
        super(gm, NONE, output);
        
        this.duration = duration;
        StreamingWriteVisitor visitor = new StreamingWriteVisitorGenerator(Pipe.from(output), random, 
                                           output.maxVarLen>>3,  //room for UTF8 
                                           output.maxVarLen>>1); //just use half       
        this.writer = new StreamingVisitorWriter(output, visitor  );
        
    }

    
    @Override
    public void startup() {
    	timeLimit = System.currentTimeMillis()+duration;
        writer.startup();
    }
    
    @Override
    public void run() {
        if (System.currentTimeMillis()<timeLimit) {
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
