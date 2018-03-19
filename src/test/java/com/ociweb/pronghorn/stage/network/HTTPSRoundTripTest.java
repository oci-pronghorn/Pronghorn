package com.ociweb.pronghorn.stage.network;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

import org.junit.Ignore;
import org.junit.Test;

import com.ociweb.pronghorn.network.ClientCoordinator;
import com.ociweb.pronghorn.network.NetGraphBuilder;
import com.ociweb.pronghorn.network.ServerCoordinator;
import com.ociweb.pronghorn.network.ServerPipesConfig;
import com.ociweb.pronghorn.network.TLSCertificates;
import com.ociweb.pronghorn.network.config.HTTPContentTypeDefaults;
import com.ociweb.pronghorn.network.config.HTTPHeaderDefaults;
import com.ociweb.pronghorn.network.config.HTTPRevisionDefaults;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.config.HTTPVerbDefaults;
import com.ociweb.pronghorn.network.http.HTTP1xRouterStageConfig;
import com.ociweb.pronghorn.network.http.ModuleConfig;
import com.ociweb.pronghorn.network.http.RouterStageConfig;
import com.ociweb.pronghorn.network.module.FileReadModuleStage;
import com.ociweb.pronghorn.network.schema.ClientHTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.NetResponseSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ScriptedNonThreadScheduler;
import com.ociweb.pronghorn.stage.test.ConsoleJSONDumpStage;

public class HTTPSRoundTripTest {

        
    @Test
	public void allCertHTTPSTest() {
    
    	int maxPartialResponses=10;
    	int connectionsInBits = 6;		
    	int clientRequestCount = 4;
    	int clientRequestSize = 1<<15;
    	final TLSCertificates tlsCertificates = TLSCertificates.defaultCerts;
    	String bindHost = "127.0.0.1";
    	int port        = 8199;
    	int processors  = 1;
    	String testFile = "groovySum.json";
    	int messagesToOrderingSuper = 1<<12;
    	int messageSizeToOrderingSuper = 1<<12;
    	
    	GraphManager gm = new GraphManager();
    	
    	Pipe<ClientHTTPRequestSchema>[] httpRequestsPipe = new Pipe[]{ClientHTTPRequestSchema.instance.newPipe(10, 1000)};
		Pipe<NetResponseSchema>[] httpResponsePipe = new Pipe[]{NetResponseSchema.instance.newPipe(10, 1000)};

		int fieldDestination = 0;
		int fieldSession = 0;
		CharSequence fieldPath = "/"+testFile;
		CharSequence fieldHeaders = null;
		ClientCoordinator.registerDomain("127.0.0.1");
		
		httpRequestsPipe[0].initBuffers();
		
		ClientHTTPRequestSchema.instance.publishHTTPGet(
														httpRequestsPipe[0], 
														fieldDestination, 
														fieldSession, 
														port, 
														bindHost, 
														fieldPath, 
														fieldHeaders);
			
		NetGraphBuilder.buildHTTPClientGraph(gm, maxPartialResponses, httpResponsePipe, httpRequestsPipe, connectionsInBits,
								clientRequestCount, clientRequestSize, tlsCertificates);
    	
		
		String pathRoot = buildStaticFileFolderPath(testFile);
		ModuleConfig modules = simpleFileServer(pathRoot, messagesToOrderingSuper, messageSizeToOrderingSuper);
	
		StringBuilder results = new StringBuilder();
		ConsoleJSONDumpStage.newInstance(gm, httpResponsePipe[0], results);
		
		NetGraphBuilder.httpServerSetup(tlsCertificates, bindHost, port, gm, processors, modules);
    	
		runRoundTrip(gm, results);
    }
    
    @Test
	public void certMatchHTTPSTest() {
    
    	final TLSCertificates tlsCertificates = new TLSCertificates() {
    		
            @Override
            public String keyStoreResourceName() {
                return "/certificates/testcert.jks";
            }

            @Override
            public String trustStroreResourceName() {
                return "/certificates/testcert.jks";
            }

            @Override
            public String keyStorePassword() {
                return "testcert";
            }

            @Override
            public String keyPassword() {
                return "testcert";
            }

            @Override
            public boolean trustAllCerts() {
                return false;
            }
        };
        
    	
    	int maxPartialResponses=10;
    	int connectionsInBits = 6;		
    	int clientRequestCount = 4;
    	int clientRequestSize = 1<<15;
    	String bindHost = "127.0.0.1";
    	int port        = 8197;
    	int processors  = 1;
    	String testFile = "groovySum.json";
    	int messagesToOrderingSuper = 1<<12;
    	int messageSizeToOrderingSuper = 1<<12;
    	
    	GraphManager gm = new GraphManager();
    	
    	Pipe<ClientHTTPRequestSchema>[] httpRequestsPipe = new Pipe[]{ClientHTTPRequestSchema.instance.newPipe(10, 1000)};
		Pipe<NetResponseSchema>[] httpResponsePipe = new Pipe[]{NetResponseSchema.instance.newPipe(10, 1000)};

		int fieldDestination = 0;
		int fieldSession = 0;
		CharSequence fieldPath = "/"+testFile;
		CharSequence fieldHeaders = null;
		
		httpRequestsPipe[0].initBuffers();
		
		ClientHTTPRequestSchema.instance.publishHTTPGet(
														httpRequestsPipe[0], 
														fieldDestination, 
														fieldSession, 
														port, 
														bindHost, 
														fieldPath, 
														fieldHeaders);
			
		NetGraphBuilder.buildHTTPClientGraph(gm, maxPartialResponses, httpResponsePipe, httpRequestsPipe, connectionsInBits,
								clientRequestCount, clientRequestSize, tlsCertificates);
    	
		String pathRoot = buildStaticFileFolderPath(testFile);
		ModuleConfig modules = simpleFileServer(pathRoot, messagesToOrderingSuper, messageSizeToOrderingSuper);
	
		StringBuilder results = new StringBuilder();
		ConsoleJSONDumpStage.newInstance(gm, httpResponsePipe[0], results);
		
		NetGraphBuilder.httpServerSetup(tlsCertificates, bindHost, port, gm, processors, modules);
    	
		runRoundTrip(gm, results);
    }
    
    @Test
	public void certAuthMatchHTTPSTest() {
    
    	final TLSCertificates tlsCertificates = new TLSCertificates() {
    		
            @Override
            public String keyStoreResourceName() {
                return "/certificates/testcert.jks";
            }

            @Override
            public String trustStroreResourceName() {
                return "/certificates/testcert.jks";
            }

            @Override
            public String keyStorePassword() {
                return "testcert";
            }

            @Override
            public String keyPassword() {
                return "testcert";
            }

            @Override
            public boolean trustAllCerts() {
                return false;
            }
        };
        
    	
    	int maxPartialResponses=10;
    	int connectionsInBits = 6;		
    	int clientRequestCount = 4;
    	int clientRequestSize = 1<<15;
    	String bindHost = "127.0.0.1";
    	int port        = 8198;
    	int processors  = 1;
    	String testFile = "groovySum.json";
    	int messagesToOrderingSuper = 1<<12;
    	int messageSizeToOrderingSuper = 1<<12;
    	
    	GraphManager gm = new GraphManager();
    	
    	Pipe<ClientHTTPRequestSchema>[] httpRequestsPipe = new Pipe[]{ClientHTTPRequestSchema.instance.newPipe(10, 1000)};
		Pipe<NetResponseSchema>[] httpResponsePipe = new Pipe[]{NetResponseSchema.instance.newPipe(10, 1000)};

		int fieldDestination = 0;
		int fieldSession = 0;
		CharSequence fieldPath = "/"+testFile;
		CharSequence fieldHeaders = null;
		
		httpRequestsPipe[0].initBuffers();
		
		ClientHTTPRequestSchema.instance.publishHTTPGet(
														httpRequestsPipe[0], 
														fieldDestination, 
														fieldSession, 
														port, 
														bindHost, 
														fieldPath, 
														fieldHeaders);
			
		NetGraphBuilder.buildHTTPClientGraph(gm, maxPartialResponses, httpResponsePipe, httpRequestsPipe, connectionsInBits,
								clientRequestCount, clientRequestSize, tlsCertificates);
    	
		String pathRoot = buildStaticFileFolderPath(testFile);
		ModuleConfig modules = simpleFileServer(pathRoot, messagesToOrderingSuper, messageSizeToOrderingSuper);
	
		StringBuilder results = new StringBuilder();
		ConsoleJSONDumpStage.newInstance(gm, httpResponsePipe[0], results);
				 
		ServerPipesConfig serverConfig = NetGraphBuilder.simpleServerPipesConfig(tlsCertificates, processors);
		
		ServerCoordinator serverCoord = new ServerCoordinator(tlsCertificates, bindHost, port, 
				   serverConfig.maxConnectionBitsOnServer, 
				   serverConfig.maxConcurrentInputs, 
				   serverConfig.maxConcurrentOutputs,
				   serverConfig.moduleParallelism, true);
		
		NetGraphBuilder.buildHTTPServerGraph(gm, modules, serverCoord, serverConfig);
		
		runRoundTrip(gm, results);
    }
    
    
	public static void runRoundTrip(GraphManager gm, StringBuilder results) {
		ScriptedNonThreadScheduler scheduler 
		= new ScriptedNonThreadScheduler(gm,null,false);
		
		scheduler.startup();
		int i = 3000;
		while ( --i >= 0 && results.length()==0) {
			scheduler.run();
		}
		scheduler.shutdown();
		
		//System.err.println("got "+results);
		assertTrue(results.toString().contains("0xff,0xff,0x7b,0x22,0x78,0x22,0x3a,0x39,0x2c,0x22,0x79,0x22,0x3a,0x31,0x37,0x2c,0x22,0x67,0x72,0x6f,0x6f,0x76,0x79,0x53,0x75,0x6d,0x22,0x3a,0x32,0x36,0x7d,0x0a"));
	}


	public static ModuleConfig simpleFileServer(final String pathRoot, final int messagesToOrderingSuper,
			final int messageSizeToOrderingSuper) {
		//using the basic no-fills API
		ModuleConfig config = new ModuleConfig() { 
		
		    //this is the cache for the files, so larger is better plus making it longer helps a lot but not sure why.
		    final PipeConfig<ServerResponseSchema> fileServerOutgoingDataConfig = new PipeConfig<ServerResponseSchema>(ServerResponseSchema.instance, 
		    		         messagesToOrderingSuper, messageSizeToOrderingSuper);//from module to  supervisor
	
			@Override
			public int moduleCount() {
				return 1;
			}

			@Override
			public Pipe<ServerResponseSchema>[] registerModule(int a,
					GraphManager graphManager, RouterStageConfig routerConfig, 
					Pipe<HTTPRequestSchema>[] inputPipes) {
				

					//the file server is stateless therefore we can build 1 instance for every input pipe
					int instances = inputPipes.length;
					
					Pipe<ServerResponseSchema>[] staticFileOutputs = new Pipe[instances];
					
					int i = instances;
					while (--i>=0) {
						staticFileOutputs[i] = new Pipe<ServerResponseSchema>(fileServerOutgoingDataConfig);
						FileReadModuleStage.newInstance(graphManager, inputPipes[i], staticFileOutputs[i], (HTTPSpecification<HTTPContentTypeDefaults, HTTPRevisionDefaults, HTTPVerbDefaults, HTTPHeaderDefaults>) ((HTTP1xRouterStageConfig)routerConfig).httpSpec, new File(pathRoot));					
					}
						
				
					routerConfig.registerRoute(
                        "/${path}"
						); //no headers requested

				return staticFileOutputs;
			}        
		 	
		 };
		return config;
	}
	
	
	private static String buildStaticFileFolderPath(String testFile) {
		URL dir = ClassLoader.getSystemResource(testFile);
		String root = "";	//file:/home/nate/Pronghorn/target/test-classes/OCILogo.png
			
		try {
		
			String uri = dir.toURI().toString();			
			root = uri.substring("file:".length(), uri.lastIndexOf('/')).replace("%20", " ").replace("/", File.separator).replace("\\", File.separator);
			
		} catch (URISyntaxException e) {						
			e.printStackTrace();
			fail();
		}
		return root;
	}


	
}
