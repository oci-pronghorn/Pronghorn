package com.ociweb.pronghorn.stage.network;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AccessMode;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Random;

import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.network.config.HTTPHeaderKeyDefaults;
import com.ociweb.pronghorn.stage.network.config.HTTPRevisionDefaults;
import com.ociweb.pronghorn.stage.network.config.HTTPVerb;
import com.ociweb.pronghorn.stage.network.config.HTTPVerbDefaults;
import com.ociweb.pronghorn.stage.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

import junit.framework.Assert;

public class StaticFileRequestGeneratorStage extends PronghornStage {

    private final int iterations;
    private final int fileCount;
    private final int fileSize;
    private int count;
    private Pipe<HTTPRequestSchema> output;
    private DataOutputBlobWriter<HTTPRequestSchema> writer;

    private final HTTPVerbDefaults verb;
    
    private int sequence;
    
    private int pathIdx;

    
    private final TestDataFiles testDataFiles;


    protected StaticFileRequestGeneratorStage(GraphManager graphManager, Pipe<HTTPRequestSchema> output, int iterations, int fileCount, int fileSize, HTTPVerbDefaults verb) {
        super(graphManager, NONE, output);
        
        this.iterations = iterations;
        this.count = iterations;
        this.fileCount = fileCount;
        this.fileSize = fileSize;
        this.output = output;
        this.verb = verb;   //HTTPVerbDefaults.GET;
        
        //Must be done extra early so other stages can see these files, so we do it here in the constuctor before startup.
        this.testDataFiles = new TestDataFiles(new File(System.getProperty("java.io.tmpdir"),"staticFileRequestGeneratorStage"), fileCount, fileSize);
    }
    
    public String tempFolder() {
        return testDataFiles.tempDirectory.toString();      
    }
    
    public void startup() {
        pathIdx = fileCount;
        writer = new DataOutputBlobWriter<HTTPRequestSchema>(output);
          
    }


    
  
    public static StaticFileRequestGeneratorStage newInstance(GraphManager gm, Pipe<HTTPRequestSchema> requestPipe, int iterations, int fileCount, int fileSize, HTTPVerbDefaults verb) {
        return new StaticFileRequestGeneratorStage(gm, requestPipe, iterations, fileCount, fileSize, verb);
    }

    
    @Override
    public void run() {
        
        while (Pipe.hasRoomForWrite(output)) {
            
            if (--pathIdx < 0) {
                if (--count < 0) {
                    System.out.println("generator has finished");
                    requestShutdown();
                    return;
                } 
                pathIdx = testDataFiles.testFilePaths.length-1;
            }
            
            int size = Pipe.addMsgIdx(output, HTTPRequestSchema.MSG_FILEREQUEST_200);
            Pipe.addLongValue(0, output); //channelId
            Pipe.addIntValue(sequence++, output); //sequence            
            Pipe.addIntValue(verb.ordinal(), output); //verb
            
                DataOutputBlobWriter.openField(writer);   
                int localLen = testDataFiles.testFilePathsBytes[pathIdx].length - testDataFiles.rootLen;
                writer.writeShort(localLen); //this is a UTF8 encode sequence of bytes so the length is required up front
                DataOutputBlobWriter.write(writer, testDataFiles.testFilePathsBytes[pathIdx], testDataFiles.rootLen, localLen, 0xFFF);
                DataOutputBlobWriter.closeLowLevelField(writer);
 
            
            Pipe.addIntValue(HTTPRevisionDefaults.HTTP_1_1.ordinal(), output);
            Pipe.addIntValue(0, output); //request context
            
            Pipe.confirmLowLevelWrite(output, size);
            Pipe.publishWrites(output);
        }
    }
    
    

}
