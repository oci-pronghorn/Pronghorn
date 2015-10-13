package com.ociweb.pronghorn.stage.file;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class TapeRoundTripTest {

    @Test
    public void roundTripTest() {
        
        //assertTrue(true);
        
        //TODO: AAAA, Finish round trip testing of the TapeRead and TapeWrite
        
        
        FieldReferenceOffsetManager from = FieldReferenceOffsetManager.RAW_BYTES;
        
        PipeConfig config = new PipeConfig(from);
        
        
        Pipe testDataPipeOut = new Pipe(config);
        Pipe testDataPipeIn  = new Pipe(config);
        
        
        GraphManager gm = new GraphManager();
        
        //write data to pipe
        
        //write from pipe to file
        
        String tapeFilePath = "testFile.dat";
        FileChannel fileOutChannel;
        try {
        
            fileOutChannel = new RandomAccessFile(tapeFilePath, "rws").getChannel();
            TapeWriteStage writeStage = new TapeWriteStage(gm, testDataPipeOut, fileOutChannel);
            FileChannel fileInChannel = new RandomAccessFile(tapeFilePath, "rws").getChannel();
            TapeReadStage readStage = new TapeReadStage(gm, testDataPipeIn, fileInChannel);

        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        //read from file to pipe
        
        
        
        //read pipe and confirm same
        
        //TODO: move test equals stage to easy place.
        
        
        
        
        
        
    }
    
    
    
}
