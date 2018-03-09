package com.ociweb.pronghorn.stage.scheduling;

import static org.junit.Assert.*;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;

import com.ociweb.pronghorn.HTTPServer;
import com.ociweb.pronghorn.network.NetGraphBuilder;
import com.ociweb.pronghorn.network.TLSCertificates;
import com.ociweb.pronghorn.network.http.ModuleConfig;

public class ScriptedThreadSchedulingTest {

	//simple server test with telemry
	//confirm that the stages are built in the right order
	
	@Test
	public void serverTest() {
		
		GraphManager gm = new GraphManager();
		
		//Test 1 processor and N
		//Must caputre the threading selected and assert its right
		StringBuilder target = new StringBuilder();
		ScriptedNonThreadScheduler.debugStageOrder = target;
		
		//must test with different threading limits.
		int maxThreads = 6;
		
		int procssors = 3;
		
		int fileOutgoing = 4;
		int fileChunkSize = 1<<10;
		String resourcesRoot = "";
		String resourcesDefault = "index.html";
		
		Path currentRelativePath = Paths.get("");
		String s = currentRelativePath.toAbsolutePath().toString();

		//TODO: these file paths are confusing and should be fixed..
		
		//File pathRoot = new File(s+"/src/main/resources/telemetry/images");
		File pathRoot = new File("/resources/telemetry/images");
		
		//TODO: thread join, must allow splitter to joing first
		//         must allow joiner to join first.
		
		ModuleConfig modules = HTTPServer.simpleFileServerConfig(
						fileOutgoing, fileChunkSize, 
						resourcesRoot, resourcesDefault, 
						pathRoot);
		
		TLSCertificates defaultcerts = TLSCertificates.defaultCerts; //test with TLS later.
		NetGraphBuilder.httpServerSetup(defaultcerts,"127.0.0.1",8084,gm, procssors, modules);
		
		NetGraphBuilder.telemetryServerSetup(defaultcerts,"127.0.0.1",8094, gm, GraphManager.TELEMTRY_SERVER_RATE);;
		
		boolean threadLimitHard = false; //almost never want this to be true.
		StageScheduler scheduler = StageScheduler.defaultScheduler(gm, maxThreads, threadLimitHard);
		scheduler.startup();

		String textA = "----------full stages -------------Clock:2000000\n"
			+ "   0 full stages ServerSocketReaderStage:1  inputs:8 ,7 ,6 ,5 ,4 ,3 outputs:2 ,1 ,0\n"
			+ "   1 full stages SSLEngineUnWrapStage:2  inputs:0 outputs:9 ,12 ,6\n"
			+ "   2 full stages HTTP1xRouterStage:13  inputs:9 outputs:36 ,5 ,45\n"
			+ "   3 full stages FileReadModuleStage:10  inputs:36 outputs:37\n"
			+ "   4 full stages OrderSupervisorStage:16  inputs:45 ,37 outputs:19\n"
			+ "   5 full stages SSLEngineWrapStage:7  inputs:19 outputs:20\n"
			+ "   6 full stages ServerSocketWriterStage:8  inputs:12 ,16 ,13 ,18 ,14 ,20 outputs:\n";

		assertTrue(target.toString(), target.indexOf(textA)>=0);

		String textB = "----------full stages -------------Clock:2000000\n"
			+ "   0 full stages SSLEngineUnWrapStage:3  inputs:1 outputs:10 ,13 ,7\n"
			+ "   1 full stages HTTP1xRouterStage:14  inputs:10 outputs:35 ,4 ,46\n"
		    + "   2 full stages FileReadModuleStage:11  inputs:35 outputs:39\n"
		    + "   3 full stages OrderSupervisorStage:17  inputs:46 ,39 outputs:17\n"
		    + "   4 full stages SSLEngineWrapStage:6  inputs:17 outputs:18\n";
		
		assertTrue(target.toString(), target.indexOf(textB)>=0);
		
		////
		//System.out.println(target);
		////
		
	}
	
	
}
