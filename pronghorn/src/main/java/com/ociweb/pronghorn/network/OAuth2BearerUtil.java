package com.ociweb.pronghorn.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.ClientHTTPRequestSchema;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.util.Appendables;

public class OAuth2BearerUtil {

	private static final CharSequence path = "/oauth2/token";
	
	private static final Logger logger = LoggerFactory.getLogger(OAuth2BearerUtil.class);
	
	public static void bearerRequest(Pipe<ClientHTTPRequestSchema> pipe, String ck, String cs, String host, int port, int httpRequestResponseId) {
		
		//logger.info("requested new bearer on port {}",port);
		
		PipeWriter.tryWriteFragment(pipe, ClientHTTPRequestSchema.MSG_POST_201);
		PipeWriter.writeInt(pipe, ClientHTTPRequestSchema.MSG_POST_201_FIELD_SESSION_10, 0);
		PipeWriter.writeInt(pipe, ClientHTTPRequestSchema.MSG_POST_201_FIELD_PORT_1, port);
		PipeWriter.writeInt(pipe, ClientHTTPRequestSchema.MSG_POST_201_FIELD_HOSTID_2, ClientCoordinator.registerDomain(host));
		PipeWriter.writeLong(pipe, ClientHTTPRequestSchema.MSG_POST_201_FIELD_CONNECTIONID_20, -1);
		PipeWriter.writeInt(pipe, ClientHTTPRequestSchema.MSG_POST_201_FIELD_DESTINATION_11, httpRequestResponseId);
				
		PipeWriter.writeUTF8(pipe, ClientHTTPRequestSchema.MSG_POST_201_FIELD_PATH_3, path);
		
		
		//builder.append("Accept-Encoding: gzip\r\n");//Accept: */*\r\n");	
		//
		//Content-Type: application/x-www-form-urlencoded;charset=UTF-8
		//Authorization: Basic <64 ENCODED bearerTokenCred>
		//Accept-Encoding: gzip
		//User-Agent: My Twitter App v1.0.23
		
		
		DataOutputBlobWriter<ClientHTTPRequestSchema> stream = PipeWriter.outputStream(pipe);
		DataOutputBlobWriter.openField(stream);		
		writeHeaders(ck, cs, stream);
		DataOutputBlobWriter.closeHighLevelField(stream, ClientHTTPRequestSchema.MSG_POST_201_FIELD_HEADERS_7);
		
		String payload = "grant_type=client_credentials";
		PipeWriter.writeUTF8(pipe, ClientHTTPRequestSchema.MSG_POST_201_FIELD_PAYLOAD_5, payload);		
		PipeWriter.publishWrites(pipe);
				
	}


	private static void writeHeaders(String ck, String cs, DataOutputBlobWriter<ClientHTTPRequestSchema> stream) {
	//	byte[] btc = encodeKeys(ck,cs).getBytes();//
		byte[] btc = (ck+':'+cs).getBytes();  //TODO: not GC free...
		//Authorization: Basic UW5KTlFsWnlkemRXVDNSV2RrWTVaSFo2TWs1Tk5EbEZZanBvTW5VNU5UQlpOVUZFZVdOa1JqSlNXV1p1V0daNE5sUkJaRVpaTlZZeE4xZDVSazFsTVVSS2JrSlBSMVZqZVVNM1lRPT0=

		stream.append("Authorization: Basic ");
		Appendables.appendBase64Encoded(stream, btc, 0, btc.length, Integer.MAX_VALUE);
		stream.append("\r\nContent-Type: application/x-www-form-urlencoded;charset=UTF-8\r\n");
	}



//	private static String encodeKeys(String consumerKey, String consumerSecret) {
//		try {
//			String encodedConsumerKey = URLEncoder.encode(consumerKey, "UTF-8");
//			String encodedConsumerSecret = URLEncoder.encode(consumerSecret, "UTF-8");
//			
//			String fullKey = encodedConsumerKey + ":" + encodedConsumerSecret;
//			byte[] encodedBytes = Base64.getEncoder().encode(fullKey.getBytes());
//			return new String(encodedBytes);  
//		}
//		catch (UnsupportedEncodingException e) {
//			return new String();
//		}
//	}
	


//	
//	// Writes a request to a connection
//	private static boolean writeRequest(HttpsURLConnection connection, String textBody) {
//		try {
//			BufferedWriter wr = new BufferedWriter(new OutputStreamWriter(connection.getOutputStream()));
//			wr.write(textBody);
//			wr.flush();
//			wr.close();
//				
//			return true;
//		}
//		catch (IOException e) { return false; }
//	}
//		
//		
//	// Reads a response for a given connection and returns it as a string.
//	private static String readResponse(HttpsURLConnection connection) {
//		try {
//			StringBuilder str = new StringBuilder();
//				
//			BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
//			String line = "";
//			while((line = br.readLine()) != null) {
//				str.append(line + System.getProperty("line.separator"));
//			}
//			return str.toString();
//		}
//		catch (IOException e) { return new String(); }
//	}
	
}
