package com.ociweb.pronghorn.stage.file;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class SequentialFileIOStage<T extends MessageSchema> extends PronghornStage {

    private Pipe ctrlPipe; // renameFile, readFile
    private Pipe<T> toWritePipe;
    private Pipe readingMetaPipe; // readMessagesCount
    private Pipe<T> readingPipe;
    
    protected SequentialFileIOStage(GraphManager graphManager, Pipe<SequentialFileIORequestSchema> ctrlPipe, Pipe<T> toWritePipe, 
                                                               Pipe<SequentialFileIOResponseSchema> readingMetaPipe, Pipe<T> readingPipe) {
        super(graphManager, new Pipe[]{ctrlPipe, toWritePipe}, new Pipe[]{readingMetaPipe, readingPipe});
       
        this.ctrlPipe = ctrlPipe;
        this.toWritePipe = toWritePipe;
        this.readingMetaPipe = readingMetaPipe;
        this.readingPipe = readingPipe;
        
    }

    @Override
    public void startup() {
        
    }
    
    @Override
    public void run() {
        
        
    }
    
    @Override
    public void shutdown() {
        
    }

    
    
}
