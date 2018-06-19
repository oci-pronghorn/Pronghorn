package com.ociweb.pronghorn.stage.math;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.util.build.FROMValidation;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.test.ConsoleJSONDumpStage;

public class HistogramTest {

	@Test
	public void testHistogramFROMMatchesXML() {
		assertTrue(FROMValidation.checkSchema("/Histogram.xml", HistogramSchema.class));
	}
	
	@Test
	public void testProbabilityFROMMatchesXML() {
		assertTrue(FROMValidation.checkSchema("/Probability.xml", ProbabilitySchema.class));
	}
	
	@Test
	public void histogramSumTest() {
		
		int inPipes = 4;
		int inLength = 10;
		
		GraphManager gm = new GraphManager();
		
		PipeConfig<HistogramSchema> pipeConfig = HistogramSchema.instance.newPipeConfig(4,1<<19);
		
		Pipe<HistogramSchema>[] inputs = Pipe.buildPipes(inPipes, pipeConfig);
		Pipe<HistogramSchema> output = new Pipe(pipeConfig);
		
		//populate test data
		int i = inPipes;
		while (--i >= 0) {
			Pipe<HistogramSchema> p = inputs[i];
			p.initBuffers();
			
			int size = Pipe.addMsgIdx(p, HistogramSchema.MSG_HISTOGRAM_1);
			Pipe.addIntValue(inLength, p);
			DataOutputBlobWriter<HistogramSchema> outStream = Pipe.openOutputStream(p);
			for(int j = 0; j<inLength; j++) {
				outStream.writePackedLong(outStream, j&0x1); //0 and 1 values
			}
			DataOutputBlobWriter.closeLowLevelField(outStream);
			Pipe.confirmLowLevelWrite(p, size);
			Pipe.publishWrites(p);
			
			Pipe.publishEOF(p);
	
		}		
		
		HistogramSumStage.newInstance(gm, output, inputs);
		
		StringBuilder b = new StringBuilder();
		ConsoleJSONDumpStage watch = ConsoleJSONDumpStage.newInstance(gm, output, b, false);
		
		StageScheduler scheduler = StageScheduler.defaultScheduler(gm);
		scheduler.startup();
		
		GraphManager.blockUntilStageTerminated(gm, watch);
		
		int idx = b.indexOf("0x80,0x84,0x80,0x84,0x80,0x84,0x80,0x84,0x80,0x84");
		assertTrue(idx>0);		
		
	}
	
	@Test
	public void histogramSelectionsTest() {
		
		int inLength = 20;
		
		GraphManager gm = new GraphManager();
		PipeConfig<HistogramSchema> pipeConfigHist = HistogramSchema.instance.newPipeConfig(4,1<<19);
		PipeConfig<ProbabilitySchema> pipeConfigProb = ProbabilitySchema.instance.newPipeConfig(4,1<<19);
		
		Pipe<HistogramSchema> input = new Pipe(pipeConfigHist);
		
		input.initBuffers();
		int size = Pipe.addMsgIdx(input, HistogramSchema.MSG_HISTOGRAM_1);
		Pipe.addIntValue(inLength, input);
		DataOutputBlobWriter<HistogramSchema> outStream = Pipe.openOutputStream(input);
		for(int j = 0; j<inLength; j++) {
			//add sin wave 	
			int value = 0x3F&((int)(10*Math.sin(j)));
			outStream.writePackedLong(outStream, value); //0 and 1 values
		}
		DataOutputBlobWriter.closeLowLevelField(outStream);
		Pipe.confirmLowLevelWrite(input, size);
		Pipe.publishWrites(input);
		Pipe.publishEOF(input);
		
		Pipe<ProbabilitySchema> output = new Pipe(pipeConfigProb);
		
		HistogramSelectPeakStage.newInstance(gm, input, output);
		
		StringBuilder b = new StringBuilder();
		ConsoleJSONDumpStage watch = ConsoleJSONDumpStage.newInstance(gm, output, b, false);
		
		StageScheduler scheduler = StageScheduler.defaultScheduler(gm);
		scheduler.startup();
		
		GraphManager.blockUntilStageTerminated(gm, watch);

		int idx = b.indexOf("0xbe,0x80,0xbe,0x80,0xbb,0x80,0xbb,0x80,0xb9,0x80,0xb9,0x80,0xb7,0x80,0xb7,0x80,0xb7");
		assertTrue(idx>0);		

	}
	
	
}
