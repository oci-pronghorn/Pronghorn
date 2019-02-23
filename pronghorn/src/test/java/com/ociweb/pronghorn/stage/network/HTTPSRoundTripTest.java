package com.ociweb.pronghorn.stage.network;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.ociweb.pronghorn.network.ClientCoordinator;
import com.ociweb.pronghorn.network.HTTPServerConfig;
import com.ociweb.pronghorn.network.HTTPServerConfigImpl;
import com.ociweb.pronghorn.network.NetGraphBuilder;
import com.ociweb.pronghorn.network.SSLUtil;
import com.ociweb.pronghorn.network.TLSCertificates;
import com.ociweb.pronghorn.network.TLSCerts;
import com.ociweb.pronghorn.network.http.ModuleConfig;
import com.ociweb.pronghorn.network.schema.ClientHTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.NetResponseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.test.ConsoleJSONDumpStage;

public class HTTPSRoundTripTest {

    AtomicInteger sessionCounter = new AtomicInteger();    
    
    @Test
    public void demo() {
    	assertTrue(true);
    }
       
    @Test
	public void allCertHTTPSTest() {
        	    	
    	int maxPartialResponses = 1;
    	int connectionsInBits = 6;		
    	int clientRequestCount = 4;
    	int clientRequestSize = SSLUtil.MinTLSBlock;
    	final TLSCertificates tlsCertificates = TLSCerts.define();
    	String bindHost = "127.0.0.1";
    	int port = (int) (3000 + (System.nanoTime()%12000));
    	int processors  = 1;
    	String testFile = "groovySum.json";
    	int messagesToOrderingSuper = 1<<12;
    	int messageSizeToOrderingSuper = 1<<12;
    	
    	GraphManager gm = new GraphManager();
    	
    	Pipe<ClientHTTPRequestSchema>[] httpRequestsPipe = new Pipe[]{ClientHTTPRequestSchema.instance.newPipe(10, 1<<14)};
		Pipe<NetResponseSchema>[] httpResponsePipe = new Pipe[]{NetResponseSchema.instance.newPipe(10, 1<<14)};

		int fieldDestination = 0;
		int fieldSession = sessionCounter.incrementAndGet();
		CharSequence fieldPath = "/"+testFile;
		CharSequence fieldHeaders = null;
		ClientCoordinator.registerDomain("127.0.0.1");
		
		httpRequestsPipe[0].initBuffers();
		Pipe<ClientHTTPRequestSchema> output = httpRequestsPipe[0];
		
		PipeWriter.presumeWriteFragment(output, ClientHTTPRequestSchema.MSG_GET_200);
		PipeWriter.writeInt(output,ClientHTTPRequestSchema.MSG_GET_200_FIELD_SESSION_10, fieldSession);
		PipeWriter.writeInt(output,ClientHTTPRequestSchema.MSG_GET_200_FIELD_PORT_1, port);
		PipeWriter.writeInt(output,ClientHTTPRequestSchema.MSG_GET_200_FIELD_HOSTID_2, ClientCoordinator.registerDomain(bindHost));
		PipeWriter.writeLong(output,ClientHTTPRequestSchema.MSG_GET_200_FIELD_CONNECTIONID_20, (long) -1);
		PipeWriter.writeInt(output,ClientHTTPRequestSchema.MSG_GET_200_FIELD_DESTINATION_11, fieldDestination);
		PipeWriter.writeUTF8(output,ClientHTTPRequestSchema.MSG_GET_200_FIELD_PATH_3, fieldPath);
		PipeWriter.writeUTF8(output,ClientHTTPRequestSchema.MSG_GET_200_FIELD_HEADERS_7, fieldHeaders);
		PipeWriter.publishWrites(output);
			
		NetGraphBuilder.buildHTTPClientGraph(gm, httpResponsePipe, httpRequestsPipe, maxPartialResponses, connectionsInBits,
		 clientRequestCount, clientRequestSize, tlsCertificates);
    	
		
		String pathRoot = buildStaticFileFolderPath(testFile);
		ModuleConfig modules = NetGraphBuilder.simpleFileServer(pathRoot, messagesToOrderingSuper, messageSizeToOrderingSuper);
	
		StringBuilder results = new StringBuilder();
		ConsoleJSONDumpStage.newInstance(gm, httpResponsePipe[0], results);
		HTTPServerConfig c = NetGraphBuilder.serverConfig(port, gm);
		
		c.setDecryptionUnitsPerTrack(2);
		c.setEncryptionUnitsPerTrack(2);
		
		if (null == tlsCertificates) {
			c.useInsecureServer();
		} else {
			c.setTLS(tlsCertificates);
		}
		c.setHost(bindHost);
				
		((HTTPServerConfigImpl)c).setTracks(processors);
		((HTTPServerConfigImpl)c).finalizeDeclareConnections();		

		//gm.enableTelemetry(9099);
		
		NetGraphBuilder.buildHTTPServerGraph(gm, modules, c.buildServerCoordinator());
		
		runRoundTrip(gm, results);
    }
    
    @Test
	public void certMatchHTTPSTest() {
    
    	final TLSCertificates tlsCertificates = TLSCerts.define();
        
    	
    	int maxPartialResponses=1;
    	int connectionsInBits = 6;		
    	int clientRequestCount = 4;
    	int clientRequestSize = SSLUtil.MinTLSBlock;
    	String bindHost = "127.0.0.1";
    	int port = (int) (3000 + (System.nanoTime()%12000));
    	int processors  = 1;
    	String testFile = "groovySum.json";
    	int messagesToOrderingSuper = 1<<12;
    	int messageSizeToOrderingSuper = 1<<12;
    	
    	ClientCoordinator.registerDomain("127.0.0.1");
    	
    	GraphManager gm = new GraphManager();
    	
    	Pipe<ClientHTTPRequestSchema>[] httpRequestsPipe = new Pipe[]{ClientHTTPRequestSchema.instance.newPipe(10, 1<<14)};
		Pipe<NetResponseSchema>[] httpResponsePipe = new Pipe[]{NetResponseSchema.instance.newPipe(10, 1<<14)};

		int fieldDestination = 0;
		int fieldSession = sessionCounter.incrementAndGet();
		CharSequence fieldPath = "/"+testFile;
		CharSequence fieldHeaders = null;
		
		httpRequestsPipe[0].initBuffers();
		Pipe<ClientHTTPRequestSchema> output = httpRequestsPipe[0];
		
		PipeWriter.presumeWriteFragment(output, ClientHTTPRequestSchema.MSG_GET_200);
		PipeWriter.writeInt(output,ClientHTTPRequestSchema.MSG_GET_200_FIELD_SESSION_10, fieldSession);
		PipeWriter.writeInt(output,ClientHTTPRequestSchema.MSG_GET_200_FIELD_PORT_1, port);
		PipeWriter.writeInt(output,ClientHTTPRequestSchema.MSG_GET_200_FIELD_HOSTID_2, ClientCoordinator.registerDomain(bindHost));
		PipeWriter.writeLong(output,ClientHTTPRequestSchema.MSG_GET_200_FIELD_CONNECTIONID_20, (long) -1);
		PipeWriter.writeInt(output,ClientHTTPRequestSchema.MSG_GET_200_FIELD_DESTINATION_11, fieldDestination);
		PipeWriter.writeUTF8(output,ClientHTTPRequestSchema.MSG_GET_200_FIELD_PATH_3, fieldPath);
		PipeWriter.writeUTF8(output,ClientHTTPRequestSchema.MSG_GET_200_FIELD_HEADERS_7, fieldHeaders);
		PipeWriter.publishWrites(output);
			
		NetGraphBuilder.buildHTTPClientGraph(gm, httpResponsePipe, httpRequestsPipe, maxPartialResponses, connectionsInBits,
											clientRequestCount, clientRequestSize, tlsCertificates);
		StringBuilder results = new StringBuilder();
		ConsoleJSONDumpStage.newInstance(gm, httpResponsePipe[0], results);
    	
		String pathRoot = buildStaticFileFolderPath(testFile);
		ModuleConfig modules = NetGraphBuilder.simpleFileServer(pathRoot, 
				messagesToOrderingSuper, messageSizeToOrderingSuper);
		HTTPServerConfig c = NetGraphBuilder.serverConfig(port, gm);
		
		c.setHost(bindHost);
		c.setDecryptionUnitsPerTrack(2);
		c.setEncryptionUnitsPerTrack(2);
		
		if (null == tlsCertificates) {
			c.useInsecureServer();
		} else {
			c.setTLS(tlsCertificates);
		}
		((HTTPServerConfigImpl)c).setTracks(processors);
		((HTTPServerConfigImpl)c).finalizeDeclareConnections();		
		
		NetGraphBuilder.buildHTTPServerGraph(gm, modules, c.buildServerCoordinator());	
		runRoundTrip(gm, results);
		
		
    }
    
    @Test
	public void certAuthMatchHTTPSTest() {
    
    	final TLSCertificates tlsCertificates = TLSCerts.define();
        
    	
    	int maxPartialResponses=1;
    	int connectionsInBits = 6;		
    	int clientRequestCount = 4;
    	int clientRequestSize = SSLUtil.MinTLSBlock;
    	String bindHost = "127.0.0.1";
    	int port = (int) (3000 + (System.nanoTime()%12000));
    	int processors  = 1;
    	String testFile = "groovySum.json"; // contains: {"x":9,"y":17,"groovySum":26}
    	int messagesToOrderingSuper = 1<<12;
    	int messageSizeToOrderingSuper = 1<<12;
    	
    	ClientCoordinator.registerDomain("127.0.0.1");
    	
    	GraphManager gm = new GraphManager();
    	
    	Pipe<ClientHTTPRequestSchema>[] httpRequestsPipe = new Pipe[]{ClientHTTPRequestSchema.instance.newPipe(10, 1<<14)};
		Pipe<NetResponseSchema>[] httpResponsePipe = new Pipe[]{NetResponseSchema.instance.newPipe(10, 1<<14)};

		int fieldDestination = 0;
		int fieldSession = sessionCounter.incrementAndGet();
		CharSequence fieldPath = "/"+testFile;
		CharSequence fieldHeaders = null;
		
		httpRequestsPipe[0].initBuffers();
		Pipe<ClientHTTPRequestSchema> output = httpRequestsPipe[0];
		
		PipeWriter.presumeWriteFragment(output, ClientHTTPRequestSchema.MSG_GET_200);
		PipeWriter.writeInt(output,ClientHTTPRequestSchema.MSG_GET_200_FIELD_SESSION_10, fieldSession);
		PipeWriter.writeInt(output,ClientHTTPRequestSchema.MSG_GET_200_FIELD_PORT_1, port);
		PipeWriter.writeInt(output,ClientHTTPRequestSchema.MSG_GET_200_FIELD_HOSTID_2, ClientCoordinator.registerDomain(bindHost));
		PipeWriter.writeLong(output,ClientHTTPRequestSchema.MSG_GET_200_FIELD_CONNECTIONID_20, (long) -1);
		PipeWriter.writeInt(output,ClientHTTPRequestSchema.MSG_GET_200_FIELD_DESTINATION_11, fieldDestination);
		PipeWriter.writeUTF8(output,ClientHTTPRequestSchema.MSG_GET_200_FIELD_PATH_3, fieldPath);
		PipeWriter.writeUTF8(output,ClientHTTPRequestSchema.MSG_GET_200_FIELD_HEADERS_7, fieldHeaders);
		PipeWriter.publishWrites(output);
			
		NetGraphBuilder.buildHTTPClientGraph(gm, httpResponsePipe, httpRequestsPipe, maxPartialResponses, connectionsInBits,
		 clientRequestCount, clientRequestSize, tlsCertificates);
    	
		String pathRoot = buildStaticFileFolderPath(testFile);
		ModuleConfig modules = NetGraphBuilder.simpleFileServer(pathRoot, messagesToOrderingSuper, messageSizeToOrderingSuper);
	
		StringBuilder results = new StringBuilder();
		ConsoleJSONDumpStage.newInstance(gm, httpResponsePipe[0], results);
		
		HTTPServerConfig c = NetGraphBuilder.serverConfig(port, gm);
		
		c.setHost(bindHost);
		c.setDecryptionUnitsPerTrack(4);
		c.setEncryptionUnitsPerTrack(4);
				
		c.setTLS(tlsCertificates);	
		((HTTPServerConfigImpl)c).setTracks(processors);
		((HTTPServerConfigImpl)c).finalizeDeclareConnections();		

		NetGraphBuilder.buildHTTPServerGraph(gm, modules, c.buildServerCoordinator());
		
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
