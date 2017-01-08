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

import com.ociweb.pronghorn.network.ClientCoordinator;
import com.ociweb.pronghorn.network.HTTPClientRequestStage;
import com.ociweb.pronghorn.network.ModuleConfig;
import com.ociweb.pronghorn.network.NetGraphBuilder;
import com.ociweb.pronghorn.network.SSLConnection;
import com.ociweb.pronghorn.network.ServerCoordinator;
import com.ociweb.pronghorn.network.config.HTTPContentTypeDefaults;
import com.ociweb.pronghorn.network.config.HTTPHeaderKeyDefaults;
import com.ociweb.pronghorn.network.config.HTTPRevisionDefaults;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.config.HTTPVerbDefaults;
import com.ociweb.pronghorn.network.module.FileReadModuleStage;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ClientHTTPRequestSchema;
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
import com.ociweb.pronghorn.stage.test.ConsoleSummaryStage;
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
    	String bindHost = "127.0.0.1";
		ServerCoordinator serverCoord = new ServerCoordinator(groups, bindHost, 8443, 15, maxListeners,1);//32K simulanious connections on server. 
    	
    	//TODO: the stages must STAY if there is work to do an NOT return !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    	
    	
		 int requestUnwrapUnits = 1;
		 int responseWrapUnits = 4; //only have 4 users now...
		 int pipesPerOutputEngine = 1;
		GraphManager gm1 = gm;
		final String path = root;
		
		
		//using the basic no-fills API
		ModuleConfig config = new ModuleConfig() {
		
		    
		    final PipeConfig<ServerResponseSchema> outgoingDataConfig = new PipeConfig<ServerResponseSchema>(ServerResponseSchema.instance, 2048, 1<<15);//from module to  supervisor
		    final Pipe<ServerResponseSchema> output = new Pipe<ServerResponseSchema>(outgoingDataConfig);
		    
			@Override
			public long addModule(int a, 
					GraphManager graphManager, Pipe<HTTPRequestSchema>[] inputs,
					HTTPSpecification<HTTPContentTypeDefaults, HTTPRevisionDefaults, HTTPVerbDefaults, HTTPHeaderKeyDefaults> spec) {
				
				FileReadModuleStage.newInstance(graphManager, inputs, output, spec, new File(path));
				
				//return needed headers
				return 0;
			}
		
			@Override
			public CharSequence getPathRoute(int a) {
				return "/%b";
			}
		
			@Override
			public Pipe<ServerResponseSchema>[] outputPipes(int a) {
				return new Pipe[]{output};
			}
		
			@Override
			public int moduleCount() {
				return 1;
			}        
		 	
		 };
		
		 int socketWriters = 1;
		 int serverInputBlobs = 1<<11;

		 int serverBlobToEncrypt = 1<<14;
		 int serverBlobToWrite = 1<<16;
		 int routerCount = 4;	
		 
		gm1 = NetGraphBuilder.buildHTTPServerGraph(isTLS, gm1, groups, maxListeners, config, serverCoord, requestUnwrapUnits, 
				    responseWrapUnits, pipesPerOutputEngine, socketWriters, 64, serverInputBlobs, 2048, serverBlobToEncrypt, 1024, serverBlobToWrite, routerCount);
 
    	gm = gm1;     
        
        /////////////////
      	
		final int inputsCount = maxListeners;//4;//also number of max connections		
		int maxPartialResponses = maxListeners;//4;
		
		final int outputsCount = 1;//2;//must be < connections
		
		//holds new requests
		Pipe<ClientHTTPRequestSchema>[] input = new Pipe[inputsCount];
		GraphManager.addDefaultNota(gm, GraphManager.SCHEDULE_RATE, 20_000);
		
		ClientCoordinator clientCoord = new ClientCoordinator(base2SimultaniousConnections, maxPartialResponses, isTLS);		
		
		int responseUnwrapUnits = 1;//To be driven by core count
		int requestWrapUnits = 1;//To be driven by core count

		int clientWriterStages = 2; //writer instances;
		
		
		Pipe<NetResponseSchema>[] toReactor = defineClient(isTLS, gm, base2SimultaniousConnections, outputsCount, maxPartialResponses, input,
				                                           clientCoord, responseUnwrapUnits, requestWrapUnits,
				                                           8, 8, clientWriterStages, 64, 1<<16);
		     
		PipeCleanerStage<NetResponseSchema> cleaner = new PipeCleanerStage<>(gm, toReactor, "Reactor");
		final StageScheduler scheduler = setupScheduler(gm, serverCoord, clientCoord);

		
		long start = System.currentTimeMillis();
		
		scheduler.startup();
		
        runTestData(testFile, testFileSize, maxListeners, input, cleaner, scheduler, start);
        

	}

	private void runTestData(String testFile, int testFileSize, final int maxListeners, Pipe<ClientHTTPRequestSchema>[] input,
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
				
				
				int testSize = 1000;
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
								
					Pipe<ClientHTTPRequestSchema> pipe = input[requests%input.length];
								
					if (PipeWriter.tryWriteFragment(pipe, ClientHTTPRequestSchema.MSG_HTTPGET_100)) {
		
						PipeWriter.writeUTF8(pipe, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_HOST_2, "127.0.0.1");
		
						int user = requests % maxListeners;
						PipeWriter.writeInt(pipe, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_LISTENER_10,  //0);
						                                                                               user);
						
						PipeWriter.writeUTF8(pipe, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_PATH_3, "/"+testFile);
						PipeWriter.writeInt(pipe, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_PORT_1, 8443);
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


	//TODO: require small memory round trip tests for cloudbees
	
	//TODO: URGENT must detect when we get the type wrong but with low level API attempt to use values!!
	
	@Ignore
	//@Test
	public void roundTripTest2() {
				
		{
			
			boolean isTLS = false;//true;
			int port = 8443;
			String host =  //"10.201.200.24";//phi
					      //"10.10.10.244";
					        "127.0.0.1"; // String host = "10.10.10.134";//" "10.10.10.244";/
			
			boolean useLocalServer = true;//

			
			GraphManager gm = new GraphManager();
	    	
	    	//TODO: will big sleeps show the backed up pipes more clearly? TODO: must be tuned for pipe lenghths?
	    	
			
			//NOTE: larger values here allows for more effecient scheculeing and bigger "batches"
			//NOTE: smaller values here will allow for slightly lower latency values
			//NOTE: by sleeping less we get more work done per stage, sometimes
			GraphManager.addDefaultNota(gm, GraphManager.SCHEDULE_RATE, 100_000); //this is in ns
			
	    	//TODO: we need a better test that has each users interaction of 10 then wait for someone else to get in while still connected.
	    	//TODO: urgent need to kill off expired pipe usages.
	    	
	    	//GraphManager.enableBatching(gm);
	    	
	    	final String testFile = "groovySum.json"; 
	    	ServerCoordinator serverCoord = null;
	    	if (useLocalServer) {
	    		serverCoord = exampleServerSetup(isTLS, gm, testFile);
	    	}
	    	
	    	//64 Simultaneous clients with 32 Simultaneous open pipes in server.  TODO: URGENT fix. 
	    	
	    	/////////////////
	        /////////////////
	    	int base2SimultaniousConnections = 3;//6; //TODO: must support multiple simultaninous connections beyond server pipes, Need to share pipes, not enough memory.
	    	int clientCount = 4;
	    		    	
	    	//TODO: this number must be the limit of max simuantious handshakes.
	    	int maxPartialResponsesClient = 1<<base2SimultaniousConnections; //input lines to client (should be large)
	    	final int clientOutputCount = 1<<base2SimultaniousConnections;//should be < client connections,  number of pipes getting wrappers and sent out put stream 
	    	final int clientWriterStages = 2; //writer instances;	
	    	
	    	
	    	/////////////
	    	////////////
	    	
	    	//client output count of pipes, this is the max count of handshakes from this client since they block all following content.
	    	
			
	    	int clientResponseUnwrapUnits = 2;//To be driven by core count,  this is for consuming get responses
	    	int clientRequestWrapUnits = isTLS?4:8;//To be driven by core count, this is for production of post requests, more pipes if we have no wrappers?
	    	int responseQueue = 48;
	    	int requestQueue = 64; 

	    	
	    	//////////////
	    	
	    	//TODO: lower this value until we see pipes get released when completed, if not then we will starve out new requests. NOTE: needs better solution.
	    	int inFlightLimit = 16_000;
	    	final int totalUsersCount = 1<<base2SimultaniousConnections;
	    	final int loadMultiplier = isTLS? 100_000 : 3_000_000;//100_000;//100_000;

	    		    	
			//one of these per unwrap unit and per partial message, there will be many of these normally so they should not be too large
			//however should be deeper if we are sending lots of messages
			int netRespQueue = 42;
			int netRespSize = 1<<12;//must be just larger than the socket buffer
	    	
	    	ClientCoordinator[] clientCoords = new ClientCoordinator[clientCount];
	    	RegulatedLoadTestStage[] clients = new RegulatedLoadTestStage[clientCount];
	    	
	    	int cc = clientCount;
	    	while (--cc>=0) {	    	
		    	singleClientSetup(isTLS, gm, base2SimultaniousConnections, totalUsersCount, loadMultiplier,
						maxPartialResponsesClient, clientOutputCount, clientWriterStages, testFile, port, host,
						clientResponseUnwrapUnits, clientRequestWrapUnits, responseQueue, requestQueue, inFlightLimit,
						clientCoords, clients, cc, netRespQueue, netRespSize);
	    	}
			
			
			if (base2SimultaniousConnections<=6) {
				//GraphManager.exportGraphDotFile(gm, "HTTPSRoundTripTest");			
	        	MonitorConsoleStage.attach(gm); 
			}
			final ServerCoordinator serverCoord1 = serverCoord;
			final ClientCoordinator[] clientCoord = clientCoords;
			final StageScheduler scheduler = new ThreadPerStageScheduler(gm);
			
			//TODO:: fix this to limit threads in use
			//final StageScheduler scheduler = new FixedThreadsScheduler(gm, 16);
			        
			               
			//TODO: add this to scheduler so its done everywehre by default!!  TODO: urgent.
			Runtime.getRuntime().addShutdownHook(new Thread() {
			    public void run() {
			    	scheduler.shutdown();
			    	scheduler.awaitTermination(3, TimeUnit.SECONDS);
			    	    if (null!=serverCoord1) {
			    	    	serverCoord1.shutdown();
			    	    }
			            int i = clientCoord.length;
			            while (--i>=0) {
			            	clientCoord[i].shutdown();
			            }
			    }
			});
	        
			
			long start = System.currentTimeMillis();
			scheduler.startup();
		
	        
	        /////////////////
	        /////////////////
	
			long totalReceived = 0;
			int c = clientCoords.length;
			while (--c>=0) {
				GraphManager.blockUntilStageBeginsShutdown(gm,  clients[c]);	
				clientCoords[c].shutdown();
				totalReceived += clients[c].totalReceived();
			}
			if (null!=serverCoord) {
				serverCoord.shutdown();
			}
			
			//TODO: all the data is back now so the stages should be free to shutdown, 
			//      do we have any which remain in run??	
			
			long duration = System.currentTimeMillis()-start;
	
			scheduler.shutdown();
			scheduler.awaitTermination(2, TimeUnit.SECONDS);
			
		//	GraphManager.validShutdown(gm);
			
		//	hist.outputPercentileDistribution(System.out, 0d);
			
			
		//	System.out.println("total bytes returned:"+cleaner.getTotalBlobCount()+" expected "+expectedData); //434_070  23_930_000
							
			
			System.out.println("duration: "+duration);			
			
			
			float msPerCall = duration/(float)totalReceived;
			System.out.println("ms per call: "+msPerCall);		
			System.out.println("calls per sec: "+(1000f/msPerCall));
			
		}
	}

	private void singleClientSetup(boolean isTLS, GraphManager gm, int base2SimultaniousConnections,
			final int totalUsersCount, final int loadMultiplier, int maxPartialResponsesClient,
			final int clientOutputCount, final int clientWriterStages, final String testFile, int port, String host,
			int clientResponseUnwrapUnits, int clientRequestWrapUnits, int responseQueue, int requestQueue,
			int inFlightLimit, ClientCoordinator[] clientCoords, RegulatedLoadTestStage[] clients, int x, int netRespQueue, int netRespSize) {
		{
			//holds new requests
			Pipe<ClientHTTPRequestSchema>[] input = new Pipe[totalUsersCount];
			
			int usersBits = 0;//this feature does not work.
			int usersPerPipe = 1<<usersBits;  
			ClientCoordinator clientCoord = new ClientCoordinator(base2SimultaniousConnections+usersBits, maxPartialResponsesClient, isTLS);						
			
			int extraHashBits = 2;
			
			Pipe<NetResponseSchema>[] toReactor = defineClient(isTLS, gm, base2SimultaniousConnections+usersBits+extraHashBits, clientOutputCount, maxPartialResponsesClient, 
					                                           input, clientCoord, clientResponseUnwrapUnits, clientRequestWrapUnits,
					                                           requestQueue, responseQueue, clientWriterStages, netRespQueue, netRespSize);
			assert(toReactor.length == input.length);
			clients[x] = new RegulatedLoadTestStage(gm, toReactor, input, totalUsersCount*loadMultiplier, inFlightLimit, "/"+testFile, usersPerPipe, port, host,"reg"+x, clientCoord);
			clientCoords[x]=clientCoord;
		}
	}

	private ServerCoordinator exampleServerSetup(boolean isTLS, GraphManager gm, final String testFile) {
		final String pathRoot = buildStaticFileFolderPath(testFile);
		
		final int maxPartialResponsesServer     = 32; //input lines to server (should be large)
		final int maxConnectionBitsOnServer 	= 13;//8K simulanious connections on server	    	
		final int serverRequestUnwrapUnits 		= 2; //server unwrap units - need more for handshaks and more for posts
		final int serverResponseWrapUnits 		= 8;
		final int serverPipesPerOutputEngine 	= isTLS?1:4;//multiplier against server wrap units for max simultanus user responses.
		final int serverSocketWriters           = 2;
		
		//drives the cached data from the file loader.
		final int messagesToOrderingSuper       = 2048;	    		
		final int messageSizeToOrderingSuper    = 1<<8;	    		
		
		
		 //the typical rec buffer is about 1<<19
		final int serverInputBlobs = 1<<17; //when small THIS has a big slow-down effect and it appears as the client getting backed up.
		final int serverInputMsg = 128;
		 
		final int serverMsgToEncrypt = 256; //only used for TLS
		final int serverBlobToEncrypt = 1<<12;
		 
		final int serverMsgToWrite = 1024; //this pipe gets full in short bursts, 
		final int serverBlobToWrite = 1<<12; //Used for both TLS and NON-TLS
		 
		final int routerCount = 4;
		
		//This must be large enough for both partials and new handshakes.
		String bindHost = "127.0.0.1";
		ServerCoordinator serverCoord = new ServerCoordinator(groups, bindHost, 8443, maxConnectionBitsOnServer, maxPartialResponsesServer, routerCount);
				
		//using the basic no-fills API
		ModuleConfig config = new ModuleConfig() { 
		
		    //this is the cache for the files, so larger is better plus making it longer helps a lot but not sure why.
		    final PipeConfig<ServerResponseSchema> fileServerOutgoingDataConfig = new PipeConfig<ServerResponseSchema>(ServerResponseSchema.instance, messagesToOrderingSuper, messageSizeToOrderingSuper);//from module to  supervisor
		    Pipe<ServerResponseSchema>[] staticFileOutputs;
		    
			@Override
			public long addModule(int a, 
					GraphManager graphManager, Pipe<HTTPRequestSchema>[] inputs,
					HTTPSpecification<HTTPContentTypeDefaults, HTTPRevisionDefaults, HTTPVerbDefaults, HTTPHeaderKeyDefaults> spec) {
								
				boolean stateless = true;
				
				if (stateless) {
					
					
					//the file server is stateless therefore we can build 1 instance for every input pipe
					int instances = inputs.length;
					
					staticFileOutputs = new Pipe[instances];
					
					int i = instances;
					while (--i>=0) {
						staticFileOutputs[i] = new Pipe<ServerResponseSchema>(fileServerOutgoingDataConfig);
						FileReadModuleStage.newInstance(graphManager, inputs[i], staticFileOutputs[i], spec, new File(pathRoot));					
					}
				
				} else {
					staticFileOutputs = new Pipe[]{ new Pipe<ServerResponseSchema>(fileServerOutgoingDataConfig) };					
					FileReadModuleStage.newInstance(graphManager, inputs, staticFileOutputs[0], spec, new File(pathRoot));
				}
				
				//return needed headers
				return 0;
			}
		
			@Override
			public CharSequence getPathRoute(int a) {
				return "/%b";
			}
			
		//TODO: add input pipes to be defined here as well??
			
			@Override
			public Pipe<ServerResponseSchema>[] outputPipes(int a) {
				return staticFileOutputs;
			}
		
			@Override
			public int moduleCount() {
				return 1;
			}        
		 	
		 };
	
		
			
		 NetGraphBuilder.buildHTTPServerGraph(isTLS, gm, groups, maxPartialResponsesServer, config, serverCoord, serverRequestUnwrapUnits, serverResponseWrapUnits, serverPipesPerOutputEngine, 
				                              serverSocketWriters, serverInputMsg, serverInputBlobs, serverMsgToEncrypt, serverBlobToEncrypt, serverMsgToWrite, serverBlobToWrite, routerCount);
		 return serverCoord;
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


	private StageScheduler setupScheduler(GraphManager gm, final ServerCoordinator serverCoord, final ClientCoordinator ... clientCoord) {

       //TODO: determine which stages can support batching
		//GraphManager.enableBatching(gm);
		
        final StageScheduler scheduler = new ThreadPerStageScheduler(gm);
        
		//TODO:: fix this to limit threads in use
        //final StageScheduler scheduler = new FixedThreadsScheduler(gm, 16);
                
                       
        //TODO: add this to scheduler so its done everywehre by default!!  TODO: urgent.
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                    scheduler.shutdown();
                    scheduler.awaitTermination(3, TimeUnit.SECONDS);

                    serverCoord.shutdown();
                    int i = clientCoord.length;
                    while (--i>=0) {
                    	clientCoord[i].shutdown();
                    }
            }
        });
		return scheduler;
	}
	


	private Pipe<NetResponseSchema>[] defineClient(boolean isTLS, GraphManager gm, int bitsPlusHashRoom,
			int outputsCount, int maxPartialResponses, Pipe<ClientHTTPRequestSchema>[] input, ClientCoordinator ccm, int responseUnwrapUnits, int requestWrapUnits,
			int requestQueue, int responseQueue, int clientWriterStages, int netRespQueue, int netRespSize) {
				
		int requestQueueBytes = 1<<4;
		
		int responseQueueBytes = 1<<15;
		
		int httpRequestQueueBytes = 1<<10;
		int httpRequetQueueSize = 64;
		

		//create more pipes if more wrapers were requested.
		if (requestWrapUnits>outputsCount) {
			outputsCount = requestWrapUnits;
		}
		//out to the server, one of these for every client user
		PipeConfig<ClientHTTPRequestSchema> netRequestConfig = new PipeConfig<ClientHTTPRequestSchema>(ClientHTTPRequestSchema.instance, requestQueue, requestQueueBytes);
		//System.err.println("in "+netRequestConfig);
		//back from server, one of these for every client user.
		PipeConfig<NetResponseSchema> netResponseConfig = new PipeConfig<NetResponseSchema>(NetResponseSchema.instance, responseQueue, responseQueueBytes);
		//System.err.println("out "+netResponseConfig);	
		
		System.err.println("Bits for pipe lookup "+bitsPlusHashRoom);
		IntHashTable listenerPipeLookup = new IntHashTable(bitsPlusHashRoom); //bigger for more speed.
		
		
		int i = input.length;//*usersPerPipe;
		while (--i>=0) {
			IntHashTable.setItem(listenerPipeLookup, i, i/*%input.length*/);//put this key on that pipe		

			
		}				
		
		//second pipe which also impacts latency		
		PipeConfig<NetPayloadSchema> httpRequestConfig = new PipeConfig<NetPayloadSchema>(NetPayloadSchema.instance,httpRequetQueueSize,httpRequestQueueBytes); 
		
		//////////////
		//these 2 are small since we have so many
		/////////////
		
		
		//responses from the server	
		Pipe<NetResponseSchema>[] toReactor = new Pipe[input.length];	
				
		int m = input.length;
		while (--m>=0) {
			toReactor[m] = new Pipe<NetResponseSchema>(netResponseConfig);
			input[m] = new Pipe<ClientHTTPRequestSchema>(netRequestConfig);	
		}

		
		Pipe<NetPayloadSchema>[] clientRequests = new Pipe[outputsCount];
		int r = outputsCount;
		while (--r>=0) {
			clientRequests[r] = new Pipe<NetPayloadSchema>(httpRequestConfig);		
		}
		

		NetGraphBuilder.buildHTTPClientGraph(isTLS, gm, 
				                             maxPartialResponses, ccm, listenerPipeLookup,
				                             netRespQueue,netRespSize,
											 clientRequests, 
											 toReactor, 
											 responseUnwrapUnits, requestWrapUnits, clientWriterStages);

		new HTTPClientRequestStage(gm, ccm, input, clientRequests);
		
		//TODO: JUST LIKE GROUPS THESE CAN NOT ACCESS ccm AT THE SAME TIME.
//		Pipe[][] inputs = Pipe.splitPipes(2, input);
//		Pipe[][] requests = Pipe.splitPipes(2, clientRequests);
//				
//		
//		new HTTPClientRequestStage(gm, ccm, inputs[0], requests[0]);
//		new HTTPClientRequestStage(gm, ccm, inputs[1], requests[1]);
//				
		
		return toReactor;
	}

	
	
	
	
}
