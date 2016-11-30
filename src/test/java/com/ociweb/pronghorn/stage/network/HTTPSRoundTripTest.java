package com.ociweb.pronghorn.stage.network;

import static org.junit.Assert.*;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;
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
import com.ociweb.pronghorn.stage.scheduling.FixedThreadsScheduler;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.test.PipeCleanerStage;

public class HTTPSRoundTripTest {


    private static final int groups = 1;//2;
    private static final int apps = 1; 
      
    
	@Ignore
	public void roundTripTest() {
				
//		String testFile = "OCILogo.png";
//		int    testFileSize = 9572;
//				
		boolean isTLS = true;
		
		String testFile = "SQRL.svg";
		int    testFileSize = 0;
		
		String root = buildStaticFileFolderPath(testFile);
		
    	GraphManager gm = new GraphManager();
    	GraphManager.addDefaultNota(gm, GraphManager.SCHEDULE_RATE, 1000);
    	GraphManager.enableBatching(gm);
    	
        /////////////////
        /////////////////
    	int base2SimultaniousConnections = 3;
    	final int maxListeners = 1<<base2SimultaniousConnections;
    	
    	//TODO: the stages must STAY if there is work to do an NOT return !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    	
    	gm = defineServer(isTLS, root, gm, maxListeners);     
        
        /////////////////
      	
		final int inputsCount = maxListeners;//4;//also number of max connections		
		int maxPartialResponses = maxListeners;//4;
		
		final int outputsCount = 1;//2;//must be < connections
		
		//holds new requests
		Pipe<NetRequestSchema>[] input = new Pipe[inputsCount];		
		
		Pipe<NetResponseSchema>[] toReactor = defineClient(isTLS, gm, inputsCount, base2SimultaniousConnections, outputsCount, maxPartialResponses, input);
		     
		PipeCleanerStage<NetResponseSchema> cleaner = new PipeCleanerStage<>(gm, toReactor, "Reactor");
		final StageScheduler scheduler = setupScheduler(gm);
		
		
		long start = System.currentTimeMillis();
		
		scheduler.startup();
		
        runTestData(testFile, testFileSize, maxListeners, input, cleaner, scheduler, start);

	}

	private void runTestData(String testFile, int testFileSize, final int maxListeners, Pipe<NetRequestSchema>[] input,
			PipeCleanerStage<NetResponseSchema> cleaner, StageScheduler scheduler, long start) {
		//		try {
		//		Thread.sleep(1000_000);
		//	} catch (InterruptedException e1) {
		//		// TODO Auto-generated catch block
		//		e1.printStackTrace();
		//	}
				
				//test this on jdk 9
				//-Djdk.nio.maxCachedBufferSize=262144
						
		        
				final int MSG_SIZE = 6;
				
				//TODO: thread scheduler grouping
				//TODO: muti response pattern for PET integration
				
				
				int testSize = 1_000_000;//_000;//
								//250;
				              // 250_000;//300_000; //TODO: must be small enough to hold in queue.
				
				int expectedData = testSize*testFileSize;
				
				int requests = testSize;		
				
				long timeout = System.currentTimeMillis()+(testSize*20); //reasonable timeout
			
				int d = 0;
				
				//Histogram hist = new Histogram(2);
				//long[] startTimes = new long[testSize];
				
				//TODO: need stage which does both produce and consume to capture Histogram
			    
		
				while (requests>0 && System.currentTimeMillis()<timeout) {
								
					Pipe<NetRequestSchema> pipe = input[requests%input.length];
								
					if (PipeWriter.tryWriteFragment(pipe, NetRequestSchema.MSG_HTTPGET_100)) {
		
						PipeWriter.writeUTF8(pipe, NetRequestSchema.MSG_HTTPGET_100_FIELD_HOST_2, "127.0.0.1");
		
						int user = requests % maxListeners;
						PipeWriter.writeInt(pipe, NetRequestSchema.MSG_HTTPGET_100_FIELD_LISTENER_10,  //0);
						                                                                               user);
						
						PipeWriter.writeUTF8(pipe, NetRequestSchema.MSG_HTTPGET_100_FIELD_PATH_3, "/"+testFile);
						PipeWriter.writeInt(pipe, NetRequestSchema.MSG_HTTPGET_100_FIELD_PORT_1, 8443);
						PipeWriter.publishWrites(pipe);
						
						requests--;				
					//	startTimes[requests] = System.nanoTime();
						
						d+=MSG_SIZE;
						
					} else {	
						Thread.yield();
					}
				}				
		
				//count total messages, we know the parser will only send 1 message for each completed event, it does not yet have streaming.
		
				System.out.println("--------------------------    watching for responses");
		
				requests = testSize;	
				
				
				
				int expected = MSG_SIZE*(testSize);
						
				int count = 0;
				int lastCount = 0;
				long nextNotice = System.currentTimeMillis()+2000;
				do {
					try {
						Thread.sleep(2);
					} catch (InterruptedException e) {
						break;
					}
					
					count = (int)cleaner.getTotalSlabCount();
					
					long now = System.currentTimeMillis();
					if (count!=lastCount) {
						
		//				int responseCount = (count-lastCount)/MSG_SIZE;
		//				long now2 = System.nanoTime();
		//				while (--responseCount>=0) {					
		//					hist.recordValue(now2-startTimes[--requests]);					
		//				}
						
						lastCount = count;
						
						if (now>nextNotice) {				
							System.err.println("pct "+((100f*lastCount)/(float)expected));
							nextNotice = now+2000;
						}
					} else {
						if (now>(nextNotice+40_000)) {
							System.err.println("value is no longer changing, break. msg total: "+(count/MSG_SIZE));
							break;
						}
					}
		
				} while (count<expected /*&& System.currentTimeMillis()<timeout*/);
						
				
		//		//do not shut down this way because the handshake will get dropped midstream. The server does not know if or when client will respond.
		//		int z = input.length;
		//		while (--z>=0) {
		//			PipeWriter.publishEOF(input[z]);		
		//		}
				
		
			//	hist.outputPercentileDistribution(System.out, 0d);
				
				
				System.out.println("total bytes returned:"+cleaner.getTotalBlobCount()+" expected "+expectedData); //434_070  23_930_000
				
				long duration = System.currentTimeMillis()-start;
		
					
				
				System.out.println("duration: "+duration);
				System.out.println("ms per call: "+(duration/(float)(count/(float)MSG_SIZE)));
				
				scheduler.shutdown();
				scheduler.awaitTermination(60, TimeUnit.SECONDS);
		
				assertEquals("Killed by timeout",expected,count);
				assertEquals(expected, lastCount);
	}

	@Ignore
	public void openCloseTest() {
		
		//String testFile = "OCILogo.png";
		{
			String testFile = "SQRL.svg";
			
			boolean isTLS = true;
			
			String root = buildStaticFileFolderPath(testFile);
			
			GraphManager gm = new GraphManager();
			GraphManager.addDefaultNota(gm, GraphManager.SCHEDULE_RATE, 10_000);
			//GraphManager.enableBatching(gm);
			
		    /////////////////
		    /////////////////
			int base2SimultaniousConnections = 13;//8K    //out of memory 14 - 16K connection
			
			final int totalUsersCount = 1<<base2SimultaniousConnections;
			
		//	final int totalUsersCount = maxListeners;// + 20;//30;//max users attached from this client, one input and one response pipe for each of these.
			
			//base2SimultaniousConnections+=7;
			//bits needed to hold all the possible users
			
			
			
			//TODO: the stages must STAY if there is work to do an NOT return !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
			
			//This must be large enough for both partials and new handshakes.
			int maxPartialResponsesServer = 16;//16;//8; //this must be larger for large connections to ensure an open pipe is always available.
			gm = defineServer(isTLS, root, gm, maxPartialResponsesServer);     
		    
		    /////////////////
			int maxPartialResponsesClient = 16;  //can and should be less than the number of client users
			int testSize = 0;
			
			
			//extra 500mb.
			ClientConnectionManager ccm = new ClientConnectionManager(base2SimultaniousConnections, maxPartialResponsesClient);
			IntHashTable listenerPipeLookup = new IntHashTable(base2SimultaniousConnections);
			
			
		  	
			
//			
//			//client output count of pipes, this is the max count of handshakes from this client since they block all following content.
//			final int outputsCount = 8;//should be < client connections,  number of pipes getting wrappers and sent out put stream 
//			
//			//holds new requests
//			Pipe<NetRequestSchema>[] input = new Pipe[totalUsersCount];		//TODO: add one here!!!
//			
//			Pipe<NetResponseSchema>[] toReactor = defineClient(isTLS, gm, totalUsersCount, base2SimultaniousConnections, outputsCount, maxPartialResponsesClient, input);
//			assert(toReactor.length == input.length);
//			
//			//TODO: test without encryption to find pure latency of framework.
//			
//			//2K is optimal? balance between handshake and optimizatios, 256 calls per client ..68 ms per result so 174ms for all + 268 latency , 442ms
//			int testSize = 100_000;//1<<11;//1_000_000; //for small values the overhead of the handshake is in the way?
//			
//			///93  268ms
//			
//			
//			RegulatedLoadTestStage client = new RegulatedLoadTestStage(gm, toReactor, input, testSize);
//			
			
			//use a single client or few pipe instances to open and close many different clients
			
			
			if (base2SimultaniousConnections<=7) {
				GraphManager.exportGraphDotFile(gm, "HTTPSRoundTripTest");			
		    	MonitorConsoleStage.attach(gm); 
			}
		    
		    
			final StageScheduler scheduler = setupScheduler(gm);
			
			long start = System.currentTimeMillis();
			scheduler.startup();
		
		    
		    /////////////////
		    /////////////////
		try {
			Thread.sleep(30_000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	//		GraphManager.blockUntilStageBeginsShutdown(gm,  client);		
		
		//	hist.outputPercentileDistribution(System.out, 0d);
			
			
		//	System.out.println("total bytes returned:"+cleaner.getTotalBlobCount()+" expected "+expectedData); //434_070  23_930_000
			
			long duration = System.currentTimeMillis()-start;
				
			
			System.out.println("duration: "+duration);
			if (testSize>0) {
				float msPerCall = duration/(float)testSize;
				System.out.println("ms per call: "+msPerCall);		
				System.out.println("calls per sec: "+(1000f/msPerCall));
			}
			scheduler.shutdown();
			scheduler.awaitTermination(3, TimeUnit.SECONDS);
		}
		System.gc();
				
		
	}
	
	
	@Ignore
	public void roundTripTest2() {
				
//		String testFile = "OCILogo.png";
		{
			String testFile = "SQRL.svg";
			
			boolean isTLS = true;
			
			String root = buildStaticFileFolderPath(testFile);
			
	    	GraphManager gm = new GraphManager();
	    	GraphManager.addDefaultNota(gm, GraphManager.SCHEDULE_RATE, 10_000);
	    	//GraphManager.enableBatching(gm);
	    	
	        /////////////////
	        /////////////////
	    	int base2SimultaniousConnections = 11;//14;//7;//12;//14;//16K
	    	
	    	final int totalUsersCount = 1<<base2SimultaniousConnections;
	    	
	    //	final int totalUsersCount = maxListeners;// + 20;//30;//max users attached from this client, one input and one response pipe for each of these.
	    	
	    	//base2SimultaniousConnections+=7;
	    	//bits needed to hold all the possible users
	    	
	    	
	    	
	    	//TODO: the stages must STAY if there is work to do an NOT return !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	    	
	    	//This must be large enough for both partials and new handshakes.
	    	int maxPartialResponsesServer = 6;//16;//16;//8; //this must be larger for large connections to ensure an open pipe is always available.
	    	gm = defineServer(isTLS, root, gm, maxPartialResponsesServer);     
	        
	        /////////////////
	      	
			
			int maxPartialResponsesClient = 16;  //can and should be less than the number of client users
			
			//client output count of pipes, this is the max count of handshakes from this client since they block all following content.
			final int outputsCount = 8;//should be < client connections,  number of pipes getting wrappers and sent out put stream 
			
			//holds new requests
			Pipe<NetRequestSchema>[] input = new Pipe[totalUsersCount];		//TODO: add one here!!!
			
			Pipe<NetResponseSchema>[] toReactor = defineClient(isTLS, gm, totalUsersCount, base2SimultaniousConnections, outputsCount, maxPartialResponsesClient, input);
			assert(toReactor.length == input.length);
			
			//TODO: test without encryption to find pure latency of framework.
			
			//NOTE: must test with the same or more test size than the users we want to test above.
			
			//2K is optimal? balance between handshake and optimizatios, 256 calls per client ..68 ms per result so 174ms for all + 268 latency , 442ms
			int testSize = totalUsersCount*1; 
			
			///93  268ms
			
			
			RegulatedLoadTestStage client = new RegulatedLoadTestStage(gm, toReactor, input, testSize);
			
			if (base2SimultaniousConnections<=7) {
				GraphManager.exportGraphDotFile(gm, "HTTPSRoundTripTest");			
	        	MonitorConsoleStage.attach(gm); 
			}
	        
	        
			final StageScheduler scheduler = setupScheduler(gm);
			
			long start = System.currentTimeMillis();
			scheduler.startup();
		
	        
	        /////////////////
	        /////////////////
	
			GraphManager.blockUntilStageBeginsShutdown(gm,  client);		
	
		//	hist.outputPercentileDistribution(System.out, 0d);
			
			
		//	System.out.println("total bytes returned:"+cleaner.getTotalBlobCount()+" expected "+expectedData); //434_070  23_930_000
			
			long duration = System.currentTimeMillis()-start;
				
			
			System.out.println("duration: "+duration);
			float msPerCall = duration/(float)testSize;
			System.out.println("ms per call: "+msPerCall);		
			System.out.println("calls per sec: "+(1000f/msPerCall));
			
			scheduler.shutdown();
			scheduler.awaitTermination(3, TimeUnit.SECONDS);
		}
		System.gc();
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


	private GraphManager defineServer(boolean isTLS, String root, GraphManager gm, int maxSimultaniousClients) {
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
    	    	
        gm = NetGraphBuilder.buildHTTPTLSServerGraph(isTLS, gm, groups, maxSimultaniousClients, apps, config, 8443);
		return gm;
	}


	private StageScheduler setupScheduler(GraphManager gm) {

       
        final StageScheduler scheduler = new ThreadPerStageScheduler(gm);
        
      //  final StageScheduler scheduler = new FixedThreadsScheduler(gm, 16); TOOD: fix this to limit threads in use
        
        
                       
        //TODO: add this to scheduler so its done everywehre by default!!  TODO: urgent.
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                    scheduler.shutdown();
                    scheduler.awaitTermination(1, TimeUnit.SECONDS);
            }
        });
		return scheduler;
	}
	


	private Pipe<NetResponseSchema>[] defineClient(boolean isTLS, GraphManager gm, final int inputsCount, int bitsPlusHashRoom,
			final int outputsCount, int maxPartialResponses, Pipe<NetRequestSchema>[] input) {

		GraphManager.addDefaultNota(gm, GraphManager.SCHEDULE_RATE, 20_000);
		
		ClientConnectionManager ccm = new ClientConnectionManager(bitsPlusHashRoom, maxPartialResponses);
		IntHashTable listenerPipeLookup = new IntHashTable(bitsPlusHashRoom);
		
		int i = input.length;
		while (--i>=0) {
			IntHashTable.setItem(listenerPipeLookup, i, i);//put this key on that pipe
			
		}

		
		
		//second pipe which also impacts latency		
		PipeConfig<NetPayloadSchema> clientNetRequestConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance,256,1<<13); 

		//must be large enough for handshake plus this is the primary pipe after the socket so it must be a little larger.
		PipeConfig<NetPayloadSchema> clientNetResponseConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance, 256, 1<<15);	
		
		
		//////////////
		//these 2 are small since we have so many
		/////////////
		
		//out to the server, one of these for every client user
		PipeConfig<NetRequestSchema> netRequestConfig = new PipeConfig<NetRequestSchema>(NetRequestSchema.instance, 4, 1<<4);
		//System.err.println("in "+netRequestConfig);
		//back from server, one of these for every client user.
		PipeConfig<NetResponseSchema> netResponseConfig = new PipeConfig<NetResponseSchema>(NetResponseSchema.instance, 4, 1<<13);
		//System.err.println("out "+netResponseConfig);		
		
		//responses from the server	
		Pipe<NetResponseSchema>[] toReactor = new Pipe[input.length];	
				
		int m = input.length;
		while (--m>=0) {
			toReactor[m] = new Pipe<NetResponseSchema>(netResponseConfig);
			input[m] = new Pipe<NetRequestSchema>(netRequestConfig);	
		}
		
		Pipe<NetPayloadSchema>[] clientRequests = new Pipe[outputsCount];
		int r = outputsCount;
		while (--r>=0) {
			clientRequests[r] = new Pipe<NetPayloadSchema>(clientNetRequestConfig);		
		}
		
		int responseUnwrapUnits = 4;
		int requestWrapUnits = 4;
		
		NetGraphBuilder.buildHTTPClientGraph(isTLS, gm, 
				                             maxPartialResponses, ccm, listenerPipeLookup,
				                             clientNetResponseConfig,
											 clientRequests, 
											 toReactor, 
											 responseUnwrapUnits, requestWrapUnits);

		HTTPClientRequestStage requestStage = new HTTPClientRequestStage(gm, ccm, input, clientRequests);
		
		return toReactor;
		
	}

	
}
