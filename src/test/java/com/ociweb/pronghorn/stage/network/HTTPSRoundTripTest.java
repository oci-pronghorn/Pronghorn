package com.ociweb.pronghorn.stage.network;

import static org.junit.Assert.*;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;

import com.ociweb.pronghorn.network.ClientConnectionManager;
import com.ociweb.pronghorn.network.HTTPClientRequestStage;
import com.ociweb.pronghorn.network.HTTPModuleFileReadStage;
import com.ociweb.pronghorn.network.ModuleConfig;
import com.ociweb.pronghorn.network.NetGraphBuilder;
import com.ociweb.pronghorn.network.config.HTTPContentTypeDefaults;
import com.ociweb.pronghorn.network.config.HTTPHeaderKeyDefaults;
import com.ociweb.pronghorn.network.config.HTTPRevisionDefaults;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.config.HTTPVerbDefaults;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.NetParseAckSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.NetRequestSchema;
import com.ociweb.pronghorn.network.schema.NetResponseSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.test.PipeCleanerStage;

public class HTTPSRoundTripTest {


    private static final int groups = 1;
    private static final int apps = 1; 
      
    
	@Ignore
	public void roundTripTest() {
				
		String testFile = "OCILogo.png";
				
		String root = buildStaticFileFolderPath(testFile);
		
    	GraphManager gm = new GraphManager();
    	GraphManager.addDefaultNota(gm, GraphManager.SCHEDULE_RATE, 1_000_000); 
    	
        /////////////////
        /////////////////
    	
    	gm = defineServer(root, gm);     
        
        /////////////////
        /////////////////
    	
		final int inputsCount = 2;
		
		int base2SimultaniousConnections = 1;//TODO: bug, why can we not set this to larger nubmer??
		final int outputsCount = 2;//must be < connections
		int maxPartialResponses = 2;
		final int maxListeners = 1<<base2SimultaniousConnections;
		

		PipeCleanerStage[] cleaners = new PipeCleanerStage[maxListeners];
		//holds new requests
		Pipe<NetRequestSchema>[] input = new Pipe[inputsCount];		
		
		defineClient(gm, inputsCount, base2SimultaniousConnections, outputsCount, maxPartialResponses, maxListeners,	cleaners, input);
		          
        /////////////////
        /////////////////
		
		StageScheduler scheduler = runGraph(gm);
        
        /////////////////
        /////////////////
		
//		try {
//		Thread.sleep(1000_000);
//	} catch (InterruptedException e1) {
//		// TODO Auto-generated catch block
//		e1.printStackTrace();
//	}
		
		
        
		final int MSG_SIZE = 6;
		
		int testSize = 20; 
		
		int requests = testSize;
		
		
		long timeout = System.currentTimeMillis()+(testSize*1500); //reasonable timeout
		
		long start = System.currentTimeMillis();
		int d = 0;

		while (requests>0 && System.currentTimeMillis()<timeout) {
						
			Pipe<NetRequestSchema> pipe = input[0];
			
			if (PipeWriter.tryWriteFragment(pipe, NetRequestSchema.MSG_HTTPGET_100)) {
				
				PipeWriter.writeUTF8(pipe, NetRequestSchema.MSG_HTTPGET_100_FIELD_HOST_2, "127.0.0.1");
				PipeWriter.writeInt(pipe, NetRequestSchema.MSG_HTTPGET_100_FIELD_LISTENER_10,  requests%maxListeners);
				PipeWriter.writeUTF8(pipe, NetRequestSchema.MSG_HTTPGET_100_FIELD_PATH_3, "/OCILogo.png");
				PipeWriter.writeInt(pipe, NetRequestSchema.MSG_HTTPGET_100_FIELD_PORT_1, 8443);
				PipeWriter.publishWrites(pipe);

				//if (requests==testSize) {
					try {
						Thread.sleep(600);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				//}

				requests--;
			
				d+=MSG_SIZE;
				
				//TODO: if we go to fast we mess up the handshake order, TODO: do not send data until after handshake completes.
				
				Thread.yield();
			} 
//			else {
//				try {
//					Thread.sleep(1);
//				} catch (InterruptedException e) {
//					break;
//				}
//				if (++b==1000){
//					System.err.println("pipe is backed up, can not make new requests to "+input[0]);
//					System.err.println("requests pipe is "+clientRequests[0]);
//				}
//			}
		}
		

		//count total messages, we know the parser will only send 1 message for each completed event, it does not yet have streaming.

		System.out.println("watching for responses");

				
//		
//		try {
//			Thread.sleep(10_000);
//		} catch (InterruptedException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
//
//		
//		if (true) {
//			System.exit(0);
//		}


		
		int expected = MSG_SIZE*(testSize);
				
		int count = 0;
		int lastCount = 0;
		do {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				break;
			}
			
			count = doneCount(cleaners);
			
			if (count!=lastCount) {				
				lastCount = count;
				
				//if (System.currentTimeMillis()-start>20_000) {				
					System.err.println("pct "+((100f*lastCount)/(float)expected));
				//}
			}

		} while (count<expected );//&& System.currentTimeMillis()<timeout);

		long duration = System.currentTimeMillis()-start;
		//59 65 67 63
		
		//74 69 74
		System.out.println("shutdown  "+count+" vs "+expected);
		scheduler.shutdown();
		scheduler.awaitTermination(2, TimeUnit.SECONDS);
			
		System.out.println("duration: "+duration);
		System.out.println("ms per call: "+(duration/(float)testSize));
		
		assertEquals(expected, lastCount);

        
	}


	private String buildStaticFileFolderPath(String testFile) {
		URL dir = ClassLoader.getSystemResource(testFile);
		String root = "";	//file:/home/nate/Pronghorn/target/test-classes/OCILogo.png
						
		try {
		
			String uri = dir.toURI().toString();			
			root = uri.substring("file:".length(), uri.lastIndexOf('/'));
			
		} catch (URISyntaxException e) {						
			e.printStackTrace();
			fail();
		}
		return root;
	}


	private GraphManager defineServer(String root, GraphManager gm) {
		final String path = root;
    	
    	ModuleConfig config = new ModuleConfig() {

 			@Override
 			public long addModule(int a, GraphManager graphManager, Pipe<HTTPRequestSchema> input,
 					Pipe<ServerResponseSchema> output,
 					HTTPSpecification<HTTPContentTypeDefaults, HTTPRevisionDefaults, HTTPVerbDefaults, HTTPHeaderKeyDefaults> spec) {
 				
 				HTTPModuleFileReadStage.newInstance(graphManager, input, output, spec, path);
 				
 				//return needed headers
 				return 0;
 			}

 			@Override
 			public CharSequence getPathRoute(int a) {
 				return "/%b";
 			}        
         	
         };
    	    	
        gm = NetGraphBuilder.buildHTTPTLSServerGraph(gm, groups, apps, config, 8443);
		return gm;
	}


	private StageScheduler runGraph(GraphManager gm) {
		
		
		GraphManager.exportGraphDotFile(gm, "HTTPSRoundTripTest");
        MonitorConsoleStage.attach(gm); 
        
        final ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
        
        scheduler.startup();                
                       
        
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                    scheduler.shutdown();
                    scheduler.awaitTermination(1, TimeUnit.MINUTES);
            }
        });
        
        return scheduler;
		
	}
	


	private void defineClient(GraphManager gm, final int inputsCount, int base2SimultaniousConnections,
			final int outputsCount, int maxPartialResponses, final int maxListeners, PipeCleanerStage[] cleaners,
			Pipe<NetRequestSchema>[] input) {
		{

		GraphManager.addDefaultNota(gm, GraphManager.SCHEDULE_RATE, 20_000);
		
		ClientConnectionManager ccm = new ClientConnectionManager(base2SimultaniousConnections,inputsCount);
		IntHashTable listenerPipeLookup = new IntHashTable(base2SimultaniousConnections+2);
		
		System.out.println("listeners "+maxListeners);
		int i = maxListeners;
		while (--i>=0) {
			IntHashTable.setItem(listenerPipeLookup, i, i);//put this key on that pipe
			
		}
		
		//IntHashTable.setItem(listenerPipeLookup, 42, 0);//put on pipe 0
		
		
		PipeConfig<NetRequestSchema> netREquestConfig = new PipeConfig<NetRequestSchema>(NetRequestSchema.instance, 30,1<<9);
		PipeConfig<NetPayloadSchema> clientNetRequestConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance,4,16000); 
		PipeConfig<NetParseAckSchema> parseAckConfig = new PipeConfig<NetParseAckSchema>(NetParseAckSchema.instance, 4);
		
		PipeConfig<NetPayloadSchema> clientNetResponseConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, 10, 1<<16); 		
		PipeConfig<NetResponseSchema> netResponseConfig = new PipeConfig<NetResponseSchema>(NetResponseSchema.instance, 10, 1<<15); //if this backs up we get an error TODO: fix

		
		//responses from the server	
		Pipe<NetResponseSchema>[] toReactor = new Pipe[maxListeners];	
		
		
		int m = maxListeners;
		while (--m>=0) {
			toReactor[m] = new Pipe<NetResponseSchema>(netResponseConfig);
		}
		
		
		int k = inputsCount;
		while (--k>=0) {
            input[k] = new Pipe<NetRequestSchema>(netREquestConfig);	
		}	
		
		Pipe<NetPayloadSchema>[] clientRequests = new Pipe[outputsCount];
		int r = outputsCount;
		while (--r>=0) {
			clientRequests[r] = new Pipe<NetPayloadSchema>(clientNetRequestConfig);		
		}
		HTTPClientRequestStage requestStage = new HTTPClientRequestStage(gm, ccm, input, clientRequests);
		
		
		NetGraphBuilder.buildHTTPClientGraph(gm, 
				                             outputsCount, maxPartialResponses, ccm, 
				                             listenerPipeLookup, 
				                             clientNetRequestConfig,
											 parseAckConfig, 
											 clientNetResponseConfig, 
											 clientRequests, 
											 toReactor);
		
		i = maxListeners;
		while (--i>=0) {
			cleaners[i] = new PipeCleanerStage<>(gm, toReactor[i]); 
		}

		}
	}

	private int doneCount(PipeCleanerStage[] cleaners) {
		int count;
		{
		int k = cleaners.length;
		int c=0;
		while (--k>=0) {
			c+=cleaners[k].getTotalSlabCount();
		}
		
		count = c;
		}
		return count;
	}
	
	
	
}
