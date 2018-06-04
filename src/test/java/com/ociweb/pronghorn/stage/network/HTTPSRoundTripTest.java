package com.ociweb.pronghorn.stage.network;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ociweb.pronghorn.network.ClientCoordinator;
import com.ociweb.pronghorn.network.HTTPServerConfig;
import com.ociweb.pronghorn.network.HTTPServerConfigImpl;
import com.ociweb.pronghorn.network.NetGraphBuilder;
import com.ociweb.pronghorn.network.ServerConnectionStruct;
import com.ociweb.pronghorn.network.ServerCoordinator;
import com.ociweb.pronghorn.network.ServerPipesConfig;
import com.ociweb.pronghorn.network.TLSCertificates;
import com.ociweb.pronghorn.network.http.ModuleConfig;
import com.ociweb.pronghorn.network.schema.ClientHTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.NetResponseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ScriptedNonThreadScheduler;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.test.ConsoleJSONDumpStage;

public class HTTPSRoundTripTest {

        
    @Test
	public void allCertHTTPSTest() {
    
    	//show the decrypted data which was sent to the client
    	//HTTP1xResponseParserStage.showData = true;
    	
    	
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
			
		NetGraphBuilder.buildHTTPClientGraph(gm, httpResponsePipe, httpRequestsPipe, maxPartialResponses, connectionsInBits,
		 clientRequestCount, clientRequestSize, tlsCertificates);
    	
		
		String pathRoot = buildStaticFileFolderPath(testFile);
		ModuleConfig modules = NetGraphBuilder.simpleFileServer(pathRoot, messagesToOrderingSuper, messageSizeToOrderingSuper);
	
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
    	
    	ClientCoordinator.registerDomain("127.0.0.1");
    	
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
			
		NetGraphBuilder.buildHTTPClientGraph(gm, httpResponsePipe, httpRequestsPipe, maxPartialResponses, connectionsInBits,
		 clientRequestCount, clientRequestSize, tlsCertificates);
		StringBuilder results = new StringBuilder();
		ConsoleJSONDumpStage.newInstance(gm, httpResponsePipe[0], results);
    	
		String pathRoot = buildStaticFileFolderPath(testFile);
		ModuleConfig modules = NetGraphBuilder.simpleFileServer(pathRoot, messagesToOrderingSuper, messageSizeToOrderingSuper);	
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
    	String testFile = "groovySum.json"; // contains: {"x":9,"y":17,"groovySum":26}
    	int messagesToOrderingSuper = 1<<12;
    	int messageSizeToOrderingSuper = 1<<12;
    	
    	ClientCoordinator.registerDomain("127.0.0.1");
    	
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
			
		NetGraphBuilder.buildHTTPClientGraph(gm, httpResponsePipe, httpRequestsPipe, maxPartialResponses, connectionsInBits,
		 clientRequestCount, clientRequestSize, tlsCertificates);
    	
		String pathRoot = buildStaticFileFolderPath(testFile);
		ModuleConfig modules = NetGraphBuilder.simpleFileServer(pathRoot, messagesToOrderingSuper, messageSizeToOrderingSuper);
	
		StringBuilder results = new StringBuilder();
		ConsoleJSONDumpStage.newInstance(gm, httpResponsePipe[0], results);
		
		HTTPServerConfig c = NetGraphBuilder.serverConfig(8080, gm);
		c.setTLS(tlsCertificates);	
		c.setTracks(processors);
		((HTTPServerConfigImpl)c).finalizeDeclareConnections();		
		
		final ServerPipesConfig serverConfig = c.buildServerConfig();
			
		
		ServerConnectionStruct scs = new ServerConnectionStruct(gm.recordTypeData);
		ServerCoordinator serverCoord = new ServerCoordinator(tlsCertificates, bindHost, port, scs,
				    true, "Server", "", serverConfig);
		
		NetGraphBuilder.buildHTTPServerGraph(gm, modules, serverCoord);
		
		runRoundTrip(gm, results);
    }
    
    
	public static void runRoundTrip(GraphManager gm, StringBuilder results) {
		
		StageScheduler s = StageScheduler.defaultScheduler(gm);
		s.startup();

		int i = 20000;
		while (--i>=0 && results.length()==0) {
			
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		try {
			Thread.sleep(3);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		s.shutdown();
		s.awaitTermination(10, TimeUnit.SECONDS);
		
		
		// expecting  {"x":9,"y":17,"groovySum":26} in the payload

		assertTrue(results.toString(),
				results.toString().contains("0xff,0xff,0x7b,0x22,0x78,0x22,0x3a,0x39,0x2c,0x22,0x79,0x22,0x3a,0x31,0x37,0x2c,0x22,0x67,0x72,0x6f,0x6f,0x76,0x79,0x53,0x75,0x6d,0x22,0x3a,0x32,0x36,0x7d,0x0a"));
	
	
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
