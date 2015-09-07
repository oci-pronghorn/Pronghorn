package com.ociweb.pronghorn.stage.file;

import java.nio.channels.FileChannel;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class TapeReadStage extends PronghornStage {

    protected TapeReadStage(GraphManager graphManager, FileChannel fileChannel, Pipe target) {
        super(graphManager, NONE, target);
    }

    @Override
    public void run() {
        // TODO Auto-generated method stub

    }

}
