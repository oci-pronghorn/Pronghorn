package com.ociweb.pronghorn.stage.phast;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.util.build.FROMValidation;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.test.ConsoleSummaryStage;
import com.ociweb.pronghorn.stage.test.PipeCleanerStage;

public class PhastCodecSchemaTest {
	
	
    @Test
	public void testFROMMatchesXML() {
		assertTrue(FROMValidation.testForMatchingFROMs("/phastCodec.xml", PhastCodecSchema.instance));
	};
	
	@Test
	public void testConstantFields() { //too many unused constants.
	    assertEquals(PhastCodecSchema.MSG_MAX_FIELDS, FieldReferenceOffsetManager.lookupTemplateLocator(10063, PhastCodecSchema.FROM));
	}
	
	@Test
	public void testEncoderStage() {
	    	    
	    GraphManager gm = new GraphManager();
	    
        PipeConfig<RawDataSchema> outputConfig    = new PipeConfig<RawDataSchema>(RawDataSchema.instance, 300, 800);
        PipeConfig<PhastCodecSchema> inputConfig  = new PipeConfig<PhastCodecSchema>(PhastCodecSchema.instance, 300);
        PipeConfig<RawDataSchema>    inputConfig2 = new PipeConfig<RawDataSchema>(RawDataSchema.instance, 10, 1024);
        
        
        Pipe<PhastCodecSchema> testValuesToEncode  = new Pipe<PhastCodecSchema>(inputConfig);
        Pipe<RawDataSchema>    testValuesToEncode2 = new Pipe<RawDataSchema>(inputConfig2);
        
        Pipe<RawDataSchema> encodedValuesToValidate = new Pipe<RawDataSchema>(outputConfig);
                
        PhastPackingStage stage = new PhastPackingStage(gm, testValuesToEncode, testValuesToEncode2, encodedValuesToValidate, 1 );
	    
        testValuesToEncode.initBuffers();
        testValuesToEncode2.initBuffers();
        
        encodedValuesToValidate.initBuffers();
        
        int j = 10;
        while (--j>=0) {
            Pipe.addMsgIdx(testValuesToEncode, PhastCodecSchema.MSG_MAX_FIELDS);
        
            int k = 63;
            while (--k>=0) {
                
                long value = 1000L * k * j;
                Pipe.addLongValue(value, testValuesToEncode);              
               
            }
        
            Pipe.publishWrites(testValuesToEncode);
        }
        Pipe.publishAllBatchedWrites(testValuesToEncode);
        
        //should take all the data from the input pipe and write it out 
        stage.startup();
        stage.run();
        Pipe.publishAllBatchedWrites(encodedValuesToValidate);
        
        DataInputBlobReader<RawDataSchema> reader = new DataInputBlobReader<RawDataSchema>(encodedValuesToValidate);
        
        j = 10;
        while (--j>=0) {
            int msgIdx = Pipe.takeMsgIdx(encodedValuesToValidate);            
            assertEquals(RawDataSchema.MSG_CHUNKEDSTREAM_1, msgIdx);
            reader.openLowLevelAPIField();
            
            int k = 63;
            while (--k>=0) {
                
                assertTrue(reader.hasRemainingBytes());
                
                long value = 1000L * k*j;
                long value2 = reader.readPackedLong();
                                
                assertEquals(value,value2);
                
            }
            assertTrue(!reader.hasRemainingBytes());
            
            Pipe.releaseReadLock(encodedValuesToValidate);
            
        }
	}
	
	@Test
	public void testDecoderStage() {
	    
	    
	       GraphManager gm = new GraphManager();
	        
	       PipeConfig<RawDataSchema> inputConfig = new PipeConfig<RawDataSchema>(RawDataSchema.instance, 100, 1000);
	       PipeConfig<PhastCodecSchema> outputConfig = new PipeConfig<PhastCodecSchema>(PhastCodecSchema.instance, 300, 8000);
	     
	       Pipe<PhastCodecSchema> decodedDataToValidate = new Pipe<PhastCodecSchema>(outputConfig);
	       Pipe<RawDataSchema> testDataToDecode = new Pipe<RawDataSchema>(inputConfig);
	      
	       PhastUnpackingStage stage = new PhastUnpackingStage(gm, testDataToDecode, decodedDataToValidate );
	        
	       Pipe.setPublishBatchSize(testDataToDecode, 0);
	       
	       testDataToDecode.initBuffers();
	       decodedDataToValidate.initBuffers();
	       
	       //prepopulate ring with known data
	       
	       DataOutputBlobWriter<RawDataSchema> writer = new DataOutputBlobWriter<RawDataSchema>(testDataToDecode);
	       
	        int  j = 10;
	        while (--j>=0) {
	            
	            Pipe.addMsgIdx(testDataToDecode, RawDataSchema.MSG_CHUNKEDSTREAM_1);
	            	            
	            writer.openField();
	            
	            int k = 63;
	            while (--k>=0) {
	                
	                long value = 1000 * k * j;
	                DataOutputBlobWriter.writePackedLong(writer, value);
	            }
	            
	            writer.closeLowLevelField();	            
	            
	            Pipe.publishWrites(testDataToDecode);
	        }
	        Pipe.publishAllBatchedWrites(testDataToDecode);
	        assertEquals(Pipe.headPosition(testDataToDecode), Pipe.workingHeadPosition(testDataToDecode) );
	        
	       stage.startup();
	       stage.run();
	       
	        
	        j = 10;
	        while (--j>=0) {
	            
	            int msgIdx = Pipe.takeMsgIdx(decodedDataToValidate);
	            
	            assertEquals(PhastCodecSchema.MSG_MAX_FIELDS, msgIdx);
	            
	            int count = (int)PhastCodecSchema.FROM.fieldIdScript[msgIdx]-10000;
	            assertEquals(63, count);
	            	            
	            int k = count;
	            while (--k>=0) {
	                long value = 1000 * k * j;
	                
	                long value2 = Pipe.takeLong(decodedDataToValidate);
	                assertEquals("at "+j+","+k,value,value2);
	                
	            }
	            
	            Pipe.releaseReadLock(decodedDataToValidate);
	            
	        }
	    
	}
	
	
	public static void main(String[] args) {
	    
	    speedTestEncodeDecode();
	    //speedTestEncodeDecodeParallel();
	    
	}
	
	private static void speedTestEncodeDecode() {
	    	    

        GraphManager gm = new GraphManager();
         
        PipeConfig<PhastCodecSchema> phastCodeConfig = new PipeConfig<PhastCodecSchema>(PhastCodecSchema.instance, 128); //TODO: AA, must optimize queue size based on load. Build tool for this.
        PipeConfig<RawDataSchema>    phastCodeConfig2 = new PipeConfig<RawDataSchema>(RawDataSchema.instance, 20);
        
        int chunkSize = 32;
        PipeConfig<RawDataSchema> rawDataConfig = new PipeConfig<RawDataSchema>(RawDataSchema.instance, 16, 63*10*chunkSize);

        Pipe<PhastCodecSchema> inputPipe = new Pipe<PhastCodecSchema>(phastCodeConfig);
        Pipe<RawDataSchema> inputPipe2 = new Pipe<RawDataSchema>(phastCodeConfig2);
        
        Pipe<RawDataSchema> packedDataPipe = new Pipe<RawDataSchema>(rawDataConfig);
        Pipe<PhastCodecSchema> outputPipe = new Pipe<PhastCodecSchema>(phastCodeConfig);
        Pipe<RawDataSchema> rePackedDataPipe = new Pipe<RawDataSchema>(rawDataConfig);
        
	    
        //Add production stage?
        int iterations = 10000000;// * 1000;
        LongDataGenStage  genStage = new LongDataGenStage(gm, new Pipe[]{inputPipe}, iterations, chunkSize);        
        //PipeCleanerStage<PhastCodecSchema> dumpStage = new PipeCleanerStage<PhastCodecSchema>(gm, inputPipe); //21-36 Gbps

        PhastPackingStage encodeStage = new PhastPackingStage(gm, inputPipe, inputPipe2, packedDataPipe, chunkSize );
        PipeCleanerStage<RawDataSchema> dumpStage = new PipeCleanerStage<RawDataSchema>(gm, packedDataPipe); //consumes 6-8 but writes 3
        
        
  //      PhastDecodeStage decodeStage = new PhastDecodeStage(gm, packedDataPipe, outputPipe );   
    //    PhastEncodeStage encodeStage2 = new PhastEncodeStage(gm, outputPipe, rePackedDataPipe, 16 );  

     // PipeCleanerStage<RawDataSchema> dumpStage = new PipeCleanerStage<RawDataSchema>(gm, rePackedDataPipe);      
//      PipeCleanerStage<PhastCodecSchema> dumpStage = new PipeCleanerStage<PhastCodecSchema>(gm, outputPipe);        

        
    //   GraphManager.enableBatching(gm); //due to internal batching nature of stages this does not help 
        
        MonitorConsoleStage monitor = MonitorConsoleStage.attach(gm,20000000);//TODO: only gets triggered on shutdown call, TODO: need to fix this.
        final ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
        scheduler.playNice = false; //this may or may not help

        scheduler.startup();
        
        //This thread must park a little or the monitor does not collect any results, NOTE: needs investigation why this is true, almost spooky action at a distance...
        LockSupport.parkUntil(System.currentTimeMillis()+1000);
              

        scheduler.awaitTermination(500, TimeUnit.SECONDS);//blocks until genStage requests shutdown or timeout

        
        //TODO: the encoder is slow however must solve this with multiplex writer
	    
	}

	   private static void speedTestEncodeDecodeParallel() {
           

	        GraphManager gm = new GraphManager();
	         
	        
	        PipeConfig<PhastCodecSchema> phastCodeConfig = new PipeConfig<PhastCodecSchema>(PhastCodecSchema.instance, 256); //TODO: AA, must optimize queue size based on load. Build tool for this.
	        PipeConfig<RawDataSchema>    phastCodeConfig2 = new PipeConfig<RawDataSchema>(RawDataSchema.instance, 10,1000); //TODO: AA, must optimize queue size based on load. Build tool for this.
            
	        int chunkSize = 32;
	        PipeConfig<RawDataSchema> rawDataConfig = new PipeConfig<RawDataSchema>(RawDataSchema.instance, 4,10*63*chunkSize);
	        
	        Pipe<PhastCodecSchema> inputPipe1 = new Pipe<PhastCodecSchema>(phastCodeConfig);
	        Pipe<RawDataSchema> inputPipe1B = new Pipe<RawDataSchema>(phastCodeConfig2);
	        
	        Pipe<PhastCodecSchema> inputPipe2 = new Pipe<PhastCodecSchema>(phastCodeConfig);
	        Pipe<RawDataSchema> inputPipe2B = new Pipe<RawDataSchema>(phastCodeConfig2);
	        
	       // Pipe<PhastCodecSchema> inputPipe3 = new Pipe<PhastCodecSchema>(phastCodeConfig);
	        
	        Pipe<RawDataSchema> packedDataPipe1 = new Pipe<RawDataSchema>(rawDataConfig);
	        Pipe<RawDataSchema> packedDataPipe2 = new Pipe<RawDataSchema>(rawDataConfig);
	       // Pipe<RawDataSchema> packedDataPipe3 = new Pipe<RawDataSchema>(rawDataConfig);
	        
	        Pipe<RawDataSchema> packedDataPipeFinal = new Pipe<RawDataSchema>(rawDataConfig);
            
	        
	        Pipe<PhastCodecSchema> outputPipe = new Pipe<PhastCodecSchema>(phastCodeConfig);
	        Pipe<RawDataSchema> rePackedDataPipe = new Pipe<RawDataSchema>(rawDataConfig);
	        
	        
	        //Add production stage?
	        int iterations = 3000000;//*1000;
	        LongDataGenStage  genStage = new LongDataGenStage(gm, new Pipe[]{inputPipe1, inputPipe2}, iterations, chunkSize);      
	        
	        PhastPackingStage encodeStage1 = new PhastPackingStage(gm, inputPipe1, inputPipe1B, packedDataPipe1, chunkSize);       
	        PhastPackingStage encodeStage2 = new PhastPackingStage(gm, inputPipe2, inputPipe2B, packedDataPipe2, chunkSize);       
	      //  PhastEncodeStage encodeStage3 = new PhastEncodeStage(gm, inputPipe3, packedDataPipe3, chunkSize);  
	        
	    //    PhastDecodeStage decodeStage = new PhastDecodeStage(gm, packedDataPipe, outputPipe );   
	  //      PhastEncodeStage encodeStage2 = new PhastEncodeStage(gm, outputPipe, rePackedDataPipe );  

//	      PipeCleanerStage<RawDataSchema> dumpStage = new PipeCleanerStage<RawDataSchema>(gm, rePackedDataPipe);      
//	      PipeCleanerStage<PhastCodecSchema> dumpStage = new PipeCleanerStage<PhastCodecSchema>(gm, outputPipe);
	        
	        
	        MergeRawDataSchemaStage mergeStage = new MergeRawDataSchemaStage(gm, new Pipe[]{packedDataPipe1, packedDataPipe2},packedDataPipeFinal);
	  //      PhastDecodeStage decodeStage = new PhastDecodeStage(gm, packedDataPipeFinal, outputPipe );   
	        
	       PipeCleanerStage<RawDataSchema> dumpStage0 = new PipeCleanerStage<RawDataSchema>(gm, packedDataPipeFinal);
	        
//	        PipeCleanerStage<RawDataSchema> dumpStage1 = new PipeCleanerStage<RawDataSchema>(gm, packedDataPipe1);
//	        PipeCleanerStage<RawDataSchema> dumpStage2 = new PipeCleanerStage<RawDataSchema>(gm, packedDataPipe2);
	  //    PipeCleanerStage<RawDataSchema> dumpStage3 = new PipeCleanerStage<RawDataSchema>(gm, packedDataPipe3);
	      
	//      PipeCleanerStage<PhastCodecSchema> dumpStage = new PipeCleanerStage<PhastCodecSchema>(gm, outputPipe);
	        
	  //      GraphManager.enableBatching(gm); //due to internal batching nature of stages this does not help 
	        
	        MonitorConsoleStage monitor = MonitorConsoleStage.attach(gm,20000000);//TODO: only gets triggered on shutdown call, TODO: need to fix this.
	        final ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
	        scheduler.playNice = false; //this may or may not help

	        scheduler.startup();
	        
	        //This thread must park a little or the monitor does not collect any results, NOTE: needs investigation why this is true, almost spooky action at a distance...
	        LockSupport.parkUntil(System.currentTimeMillis()+500);
	              

	        scheduler.awaitTermination(500, TimeUnit.SECONDS);//blocks until genStage requests shutdown or timeout

	        
	        //TODO: the encoder is slow however must solve this with multiplex writer
	        
	    }
	
	
}
