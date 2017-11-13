package com.ociweb.pronghorn.stage.network.mqtt;

import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.junit.Ignore;

import com.ociweb.pronghorn.network.TLSCertificates;
import com.ociweb.pronghorn.network.mqtt.MQTTClientGraphBuilder;
import com.ociweb.pronghorn.network.mqtt.MQTTEncoder;
import com.ociweb.pronghorn.network.schema.MQTTClientRequestSchema;
import com.ociweb.pronghorn.network.schema.MQTTClientResponseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.NonThreadScheduler;
import com.ociweb.pronghorn.stage.test.ConsoleJSONDumpStage;

public class ClientTest {

	@Ignore //Dave please look into this one
    public void simpleClientTest() {
        
        GraphManager gm = new GraphManager();
       
		final boolean isTLS = false;
		
		int maxInFlight = 40;
		int maximumLenghOfVariableLengthFields = 4096;
		long rate = 1_200;
		
		CharSequence fieldHost = "127.0.0.1";
		int fieldPort = isTLS ? 8883 : 1883;

		String user = "user";
		String pass = "pass";
		
		Pipe<MQTTClientRequestSchema> clientRequest = MQTTClientRequestSchema.instance.newPipe(maxInFlight, maximumLenghOfVariableLengthFields);
		clientRequest.initBuffers();
		
		Pipe<MQTTClientResponseSchema> clientResponse = MQTTClientResponseSchema.instance.newPipe(maxInFlight, maximumLenghOfVariableLengthFields);
		
		short responseConnections = (short)1;

		TLSCertificates certs = isTLS ? TLSCertificates.defaultCerts : null;
		MQTTClientGraphBuilder.buildMQTTClientGraph(gm, certs,
				                                    maxInFlight, maximumLenghOfVariableLengthFields, 
				                                    clientRequest, clientResponse, rate,
													(byte)3, responseConnections,
													user, pass);       
       
        ConsoleJSONDumpStage.newInstance(gm, clientResponse);
       
       // gm.enableTelemetry(8098);
        
        NonThreadScheduler scheduler = new NonThreadScheduler(gm);
       
        scheduler.startup();
       
       int j  = 100; //build up some Ids
       while (--j>=0) {
           scheduler.run();
       }
       
       boolean okHost = PipeWriter.tryWriteFragment(clientRequest, MQTTClientRequestSchema.MSG_BROKERCONFIG_100);
       assertTrue(okHost);
       
	   PipeWriter.writeUTF8(clientRequest,MQTTClientRequestSchema.MSG_BROKERCONFIG_100_FIELD_HOST_26, fieldHost);
	   PipeWriter.writeInt(clientRequest,MQTTClientRequestSchema.MSG_BROKERCONFIG_100_FIELD_PORT_27, fieldPort);
	   PipeWriter.publishWrites(clientRequest);
	  
       
       
       int fieldKeepAliveSec = 4;
              
       //If CleanSession is set to 0, the Server MUST resume communications with the Client based on state 
       //from the current Session (as identified by the Client identifier). If there is no Session associated 
       //with the Client identifier the Server MUST create a new Session. The Client and Server MUST store the 
       //Session after the Client and Server are disconnected [MQTT-3.1.2-4]. After the disconnection of a Session 
       //that had CleanSession set to 0, the Server MUST store further QoS 1 and QoS 2 messages that match any 
       //subscriptions that the client had at the time of disconnection as part of the Session state [MQTT-3.1.2-5]. 
       //It MAY also store QoS 0 messages that meet the same criteria.
       
       //If CleanSession is set to 1, the Client and Server MUST discard any previous Session and start a new one. 
       //This Session lasts as long as the Network Connection. State data associated with this Session MUST NOT be 
       //reused in any subsequent Session [MQTT-3.1.2-6].
       
       //TELL THE SERVER NOT TO REMEMBER PREVIOUS DISCUSSIONS
       int fieldFlags = MQTTEncoder.CONNECT_FLAG_CLEAN_SESSION_1; 
       
       boolean okCon = PipeWriter.tryWriteFragment(clientRequest, MQTTClientRequestSchema.MSG_CONNECT_1);
       assertTrue(okCon);
        
	   PipeWriter.writeInt(clientRequest,MQTTClientRequestSchema.MSG_CONNECT_1_FIELD_KEEPALIVESEC_28, fieldKeepAliveSec);
	   PipeWriter.writeInt(clientRequest,MQTTClientRequestSchema.MSG_CONNECT_1_FIELD_FLAGS_29, fieldFlags);
	   PipeWriter.writeUTF8(clientRequest,MQTTClientRequestSchema.MSG_CONNECT_1_FIELD_CLIENTID_30, "testClient");
	   PipeWriter.writeUTF8(clientRequest,MQTTClientRequestSchema.MSG_CONNECT_1_FIELD_WILLTOPIC_31, "");
	   PipeWriter.writeBytes(clientRequest,MQTTClientRequestSchema.MSG_CONNECT_1_FIELD_WILLPAYLOAD_32, ByteBuffer.allocate(0));
	   PipeWriter.writeUTF8(clientRequest,MQTTClientRequestSchema.MSG_CONNECT_1_FIELD_USER_33, "");
	   PipeWriter.writeUTF8(clientRequest,MQTTClientRequestSchema.MSG_CONNECT_1_FIELD_PASS_34, "");
	   PipeWriter.publishWrites(clientRequest);

              
              
       //need an instance of broker running somewhere?? mosquitto already running on my dev box.
       // watch at prompt mosquitto_sub -t /# -q 1

	   ////////////////////////////////
	   //a single QoS 1 causes issues.
	   //////////////////////////////
	   int fieldQOS = 1;
       byte[] payload = ("hello world "+fieldQOS).getBytes();
       
       boolean okPub = PipeWriter.tryWriteFragment(clientRequest, MQTTClientRequestSchema.MSG_PUBLISH_3);
       assertTrue(okPub);
	   PipeWriter.writeInt(clientRequest,MQTTClientRequestSchema.MSG_PUBLISH_3_FIELD_QOS_21, fieldQOS);
	   PipeWriter.writeInt(clientRequest,MQTTClientRequestSchema.MSG_PUBLISH_3_FIELD_RETAIN_22, 0);
	   PipeWriter.writeUTF8(clientRequest,MQTTClientRequestSchema.MSG_PUBLISH_3_FIELD_TOPIC_23, "/sensors/temprature");
	   PipeWriter.writeBytes(clientRequest,MQTTClientRequestSchema.MSG_PUBLISH_3_FIELD_PAYLOAD_25, payload, 0, payload.length);
	   PipeWriter.publishWrites(clientRequest);

       PipeWriter.publishEOF(clientRequest);       
	   
	   
       
//       while (!NonThreadScheduler.isShutdownRequested(scheduler)) {
//    	   scheduler.run();
//       }
//       scheduler.shutdown();
       
	   long target = System.currentTimeMillis()+2_000;
       
       while (System.currentTimeMillis()<target) {
  //  	   System.err.println("run loop pass.......................");
           scheduler.run();
           try {
        	   Thread.sleep(1);
			} catch (InterruptedException e) {
				return;
			}
       }
       
       scheduler.shutdown();
       
       
    }
    
    
}
