package com.ociweb.pronghorn.stage.test;

import static com.ociweb.pronghorn.ring.RingBufferConfig.pipe;
import static com.ociweb.pronghorn.stage.scheduling.GraphManager.getOutputPipe;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import javax.xml.parsers.ParserConfigurationException;

import org.junit.Test;
import org.xml.sax.SAXException;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.loader.TemplateHandler;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.route.SplitterStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

public class GeneratorValidatorTest {

    private final int seed = 420;
    private final int iterations = 10;
    private final long TIMEOUT_SECONDS = 4;

    public static FieldReferenceOffsetManager buildFROM() {
        try {
            return TemplateHandler.loadFrom("/template/smallExample.xml");
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }   
        return null;        
    }
    
    
    @Test
    public void confirmInputs() {
        
        FieldReferenceOffsetManager from = buildFROM();        
        assertTrue(null!=from);
        
        RingBufferConfig busConfig = new RingBufferConfig(from, 10, 64);
        
        GraphManager gm = new GraphManager();
        
        RingBuffer inputRing1 = pipe(busConfig);
        inputRing1.initBuffers();
        
        RingBuffer inputRing2 = pipe(busConfig); 
        inputRing2.initBuffers();
        
        PronghornStage generator1 = new TestGenerator(gm, seed, iterations, inputRing1);        
        PronghornStage generator2 = new TestGenerator(gm, seed, iterations, inputRing2);  
        
        generator1.startup();
        generator2.startup();
        
        generator1.run();
        generator2.run();
        
        generator1.run();
        generator2.run();
        
        generator1.shutdown();
        generator2.shutdown();
        
        RingBuffer ring1 = getOutputPipe(gm, generator1);
        RingBuffer ring2 = getOutputPipe(gm, generator2);
        
        assertTrue(inputRing1 == ring1);
        assertTrue(inputRing2 == ring2);
                
        
        assertTrue(Arrays.equals(ring1.buffer,ring2.buffer));
        assertTrue(Arrays.equals(ring1.byteBuffer,ring2.byteBuffer));
        assertEquals(RingBuffer.headPosition(ring1),RingBuffer.headPosition(ring2));
        assertEquals(RingBuffer.tailPosition(ring1),RingBuffer.tailPosition(ring2));
                
        
        PronghornStage validateResults = new TestValidator(gm, 
                ring1, ring2
                );
        
        assertTrue(RingBuffer.tailPosition(ring1)<RingBuffer.headPosition(ring1));
        validateResults.startup();
        validateResults.run();
        validateResults.shutdown();
        
        assertTrue(RingBuffer.tailPosition(ring1)==RingBuffer.headPosition(ring1));
   
    }
    
    
    
    @Test
    public void twoGeneratorsTest() {
        
        FieldReferenceOffsetManager from = buildFROM();        
        assertTrue(null!=from);
        
        RingBufferConfig busConfig = new RingBufferConfig(from, 10, 64);
        
        GraphManager gm = new GraphManager();
        
        
//simple test with no split        
        PronghornStage generator1 = new TestGenerator(gm, seed, iterations, pipe(busConfig));        
        PronghornStage generator2 = new TestGenerator(gm, seed, iterations, pipe(busConfig));   
        
        PronghornStage validateResults = new TestValidator(gm, 
                                                getOutputPipe(gm, generator1), 
                                                getOutputPipe(gm, generator2));
               
        
        //start the timer       
        final long start = System.currentTimeMillis();
        
        GraphManager.enableBatching(gm);
        
        StageScheduler scheduler = new ThreadPerStageScheduler(GraphManager.cloneAll(gm));        
        scheduler.startup();        
        
        //blocks until all the submitted runnables have stopped
       

        //this timeout is set very large to support slow machines that may also run this test.
        boolean cleanExit = scheduler.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      
        long duration = System.currentTimeMillis()-start;
        
        
    }
    
    @Test
    public void splitterTest() {
        
        FieldReferenceOffsetManager from = buildFROM();        
        assertTrue(null!=from);
        
        RingBufferConfig busConfig = new RingBufferConfig(from, 10, 64);
        
        GraphManager gm = new GraphManager();
        
        int seed = 420;
        int iterations = 10;

        
//simple test using split
        PronghornStage generator = new TestGenerator(gm, seed, iterations, pipe(busConfig));        
        SplitterStage splitter = new SplitterStage(gm, getOutputPipe(gm, generator), pipe(busConfig.grow2x()), pipe(busConfig.grow2x()));       
        PronghornStage validateResults = new TestValidator(gm, getOutputPipe(gm, splitter, 2), getOutputPipe(gm, splitter, 1));
  
        
        
        //start the timer       
        final long start = System.currentTimeMillis();
        
      //  GraphManager.enableBatching(gm);
        
        StageScheduler scheduler = new ThreadPerStageScheduler(GraphManager.cloneAll(gm));        
        scheduler.startup();        
        
        //blocks until all the submitted runnables have stopped
       
        //this timeout is set very large to support slow machines that may also run this test.
        boolean cleanExit = scheduler.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      
        long duration = System.currentTimeMillis()-start;
        
        
    }
    
    
}
