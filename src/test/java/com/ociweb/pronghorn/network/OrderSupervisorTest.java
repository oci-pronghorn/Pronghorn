package com.ociweb.pronghorn.network;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.BeforeClass;
import org.junit.Test;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.test.ConsoleJSONDumpStage;

public class OrderSupervisorTest {

	private final ServerCoordinator coordinator = new ServerCoordinator(null,
			"127.0.0.1",9999,
			12,
			4,4,
			1,
			false);
	private final boolean isTLS = false;
	private final PipeConfig<ServerResponseSchema> resConfig = ServerResponseSchema.instance.newPipeConfig(6,100);
	private final PipeConfig<NetPayloadSchema> netConfig = NetPayloadSchema.instance.newPipeConfig(6,100);
	
	private final byte[] fieldPayloadBacking = new byte[0];
	private final int fieldPayloadPosition = 0;
	private final int fieldPayloadLength = 0;
	private final int fieldRequestContext = OrderSupervisorStage.END_RESPONSE_MASK;
	private final int outputCount = 1;

	private static boolean runTests=false;
	
	@BeforeClass
	public static void checkForAssert() {
		assert(runTests=true);
	}
	
	@Test
	public void orderSuperHangTest() {
				
		if (!runTests) {
			assertTrue(true);
			return;
		}
		
		
		GraphManager gm = new GraphManager();
		
		Pipe<ServerResponseSchema>[] inputPipes = Pipe.buildPipes(outputCount, resConfig);
		Pipe<NetPayloadSchema>[] outgoingPipes = Pipe.buildPipes(outputCount, netConfig);
				
		inputPipes[0].initBuffers();
		
	
		Pipe<ServerResponseSchema> output = inputPipes[0];

		//we did not send 1 but we sent 0 and 2, this should cause the hang logic to trigger.
		ServerResponseSchema.publishToChannel(output, 1, 0,				
				fieldPayloadBacking, 
				fieldPayloadPosition, 
				fieldPayloadLength, 
				fieldRequestContext);
		
		ServerResponseSchema.publishToChannel(output, 1, 2,				
				fieldPayloadBacking, 
				fieldPayloadPosition, 
				fieldPayloadLength, 
				fieldRequestContext);
		
		Pipe.publishEOF(output);
		
		OrderSupervisorStage.newInstance(gm, 
				inputPipes, 
				outgoingPipes, 
				coordinator, 
				isTLS);
		
		StringBuilder console = new StringBuilder();
		PronghornStage watch = ConsoleJSONDumpStage.newInstance(gm, outgoingPipes[0], console);
				
		StageScheduler scheduler = new ThreadPerStageScheduler(gm);
		
		ByteArrayOutputStream baost = new ByteArrayOutputStream();
		PrintStream temp = System.err;
		System.setErr(new PrintStream(baost));
		
		scheduler.startup();
		
		GraphManager.blockUntilStageBeginsShutdown(gm, watch);
			
		scheduler.shutdown();
		
		System.setErr(temp);
		
		String captured = new String(baost.toByteArray());
		
		assertTrue(captured, captured.indexOf("Hang detected")>=0);
				
	}
	
	@Test
	public void orderSuperNoHangTest() {
			
				
		if (!runTests) {
			assertTrue(true);
			return;
		}
		
		GraphManager gm = new GraphManager();
		
		Pipe<ServerResponseSchema>[] inputPipes = Pipe.buildPipes(outputCount, resConfig);
		Pipe<NetPayloadSchema>[] outgoingPipes = Pipe.buildPipes(outputCount, netConfig);
				
		inputPipes[0].initBuffers();
		
	
		Pipe<ServerResponseSchema> output = inputPipes[0];

		//we did not send 1 but we sent 0 and 2, this should cause the hang logic to trigger.
		ServerResponseSchema.publishToChannel(output, 1, 0,				
				fieldPayloadBacking, 
				fieldPayloadPosition, 
				fieldPayloadLength, 
				fieldRequestContext);
		
		ServerResponseSchema.publishToChannel(output, 1, 1,				
				fieldPayloadBacking, 
				fieldPayloadPosition, 
				fieldPayloadLength, 
				fieldRequestContext);
		
		ServerResponseSchema.publishToChannel(output, 1, 2,				
				fieldPayloadBacking, 
				fieldPayloadPosition, 
				fieldPayloadLength, 
				fieldRequestContext);
		
		Pipe.publishEOF(output);
		
		OrderSupervisorStage.newInstance(gm, 
				inputPipes, 
				outgoingPipes, 
				coordinator, 
				isTLS);
		
		StringBuilder console = new StringBuilder();
		PronghornStage watch = ConsoleJSONDumpStage.newInstance(gm, outgoingPipes[0], console);
				
		StageScheduler scheduler = new ThreadPerStageScheduler(gm);
		
		ByteArrayOutputStream baost = new ByteArrayOutputStream();
		PrintStream temp = System.err;
		System.setErr(new PrintStream(baost));
		
		scheduler.startup();
		
		GraphManager.blockUntilStageBeginsShutdown(gm, watch);
			
		scheduler.shutdown();
		
		System.setErr(temp);
		
		String captured = new String(baost.toByteArray());
		
		assertFalse(captured, captured.indexOf("Hang detected")>=0);
				
	}
}
