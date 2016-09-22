package com.ociweb.pronghorn.stage.network;

import java.io.IOException;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.network.schema.ServerRequestSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class ClientHTTPSocketRequestGeneratorStage extends PronghornStage {

    private Logger logger = LoggerFactory.getLogger(ClientHTTPSocketRequestGeneratorStage.class);
    
    private ByteBuffer rawData;
    private int rawDataLength;
    private int[] rawDataSizes;
    
    private int chunkCount;
    private int pos;
    private final int iterations;
    private int iterationCount;    
    private final CharSequence[] paths;
    
    private final String verb;
    
    private final String targetAddr = "127.0.0.1";
    private final int portNumber;
    
    private Socket socket;
    private SocketChannel socketChannel;

    private final int batchSize;
    
    private long realCount = 0;
    
    
    
    public static ClientHTTPSocketRequestGeneratorStage newInstance(GraphManager gm, int iterations, CharSequence[] paths, int port) {
        return new ClientHTTPSocketRequestGeneratorStage(gm, iterations, paths, port);
    }
     
    public ClientHTTPSocketRequestGeneratorStage(GraphManager gm, int iterations, CharSequence[] paths, int port) {
        super(gm, NONE, NONE);
        
        this.verb = "GET ";
        //= "GET "; // .6M msg/sec
        //= "HEAD "; //1.8M msg/sec
                
        this.iterations = iterations;
        this.iterationCount = iterations;
        this.pos = 0;
        this.paths = paths;        
        this.batchSize = paths.length;
                
      //  System.out.println("batching "+batchSize);
        
        this.portNumber = port;
        GraphManager.addNota(gm, GraphManager.PRODUCER, GraphManager.PRODUCER,  this);
                      
    }
    
    @Override
    public void startup() {        
        StringBuilder builder = new StringBuilder();
                        
        rawDataSizes = new int[paths.length];
               
        for(int i = 0; i<paths.length; i++) {
            int startPos = builder.length();
            httpRequestBuilder(verb, builder, paths[i]);
            rawDataSizes[i]=builder.length()-startPos;
        }        
        
        rawData = ByteBuffer.wrap(builder.toString().getBytes());
        rawDataLength = rawData.limit();
        rawData.position(0);
        rawData.limit(0);
        
        long totalRequestBytes = rawDataLength*(long)iterations;
        //System.out.println("total request bytes "+totalRequestBytes+" bits "+(8L*totalRequestBytes)); 
                
        
    }

    protected boolean ensureOpenSocket() {
        if (socket==null || socketChannel==null || !socketChannel.isOpen()) {
            
            //open write socket.
            try {
                socket = new Socket(targetAddr, portNumber);
                socketChannel = SocketChannel.open(socket.getRemoteSocketAddress());
                
                socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
       //         socketChannel.setOption(StandardSocketOptions.SO_LINGER, 3);
                socketChannel.setOption(StandardSocketOptions.SO_SNDBUF, 65536);
                               
                logger.trace("opened socket to {}",socket.getLocalAddress());
            } catch (UnknownHostException e) {
                socket = null;
                socketChannel = null;
                throw new RuntimeException(e);
            } catch (IOException e) {
                socket = null;
                socketChannel = null;
                return false;
            }
        }
        return true;
    }
    
    private void httpRequestBuilder(String verb, Appendable target, CharSequence path) {
               
        //request    
        //  GET /hello/myfile HTTP/1.1
        //  Host: 127.0.0.1:8081
        //  Connection: keep-alive
        //  Cache-Control: max-age=0
        //  Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8
        //  Upgrade-Insecure-Requests: 1
        //  User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/47.0.2526.106 Chrome/47.0.2526.106 Safari/537.36
        //  DNT: 1
        //  Accept-Encoding: gzip, deflate, sdch
        //  Accept-Language: en-US,en;q=0.8
        
        //connection must apper with upgrade and upgrade should appear in first couple
//        Upgrade: websocket   h2c    -- what are the valid values?   HTTP/2.0, SHTTP/1.3, IRC/6.9, RTA/x11 protocols the server may upgrade to
//        Connection: Upgrade -- what are the valid values?  keep-alive
        
        try {
            target.append(verb);
            target.append(path);
            target.append(" HTTP/1.1\r\n"); //MUST have white space before the path or this does not work.
            target.append("Host: 127.0.0.1:8081\r\n");
            target.append("Connection: keep-alive\r\n");
            target.append("Cache-Control: max-age=0\r\n");
     //       target.append("Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\r\n");
   
            //longer values may be picked up and inerprited wrongly.
            
    //        target.append("Upgrade-Insecure-Requests: 1\r\n");
            
            target.append("User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/47.0.2526.106 Chrome/47.0.2526.106 Safari/537.36\r\n");
            //TODO: duplicate the above and profile again.
            
            
            target.append("DNT: 1\r\n");
            target.append("Accept-Encoding: gzip, deflate, sdch\r\n");
            target.append("Accept-Language: en-US,en;q=0.8\r\n");
            target.append("\r\n");//marks the end
            
            
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
    }

    @Override
    public void run() {        
        
        if (!ensureOpenSocket()) {
            Thread.yield();
            return;//try later
        }
        
        assert(null!=socketChannel);
        
        
        while (pos < rawDataLength ) {
                        
            if (!rawData.hasRemaining()) {
                int length = 0;
                int i = batchSize;
                while (--i>=0) {
                    length += rawDataSizes[chunkCount++];    
                }
                rawData.limit(pos+length);
                rawData.position(pos);
            }
            
            try {
                
                do {
                    int len = socketChannel.write(rawData);
                 //   System.out.println("wrote "+len);
                    pos += len;
                    Thread.yield();
                } while (rawData.hasRemaining());//WARING this is a blocking loop but its part of the load testing test
                realCount += batchSize;
                                
            } catch (IOException e) {
                //reconnect and continue, we have hit some back pressure
                socket = null;
                socketChannel = null;
                Thread.yield();
                return;
            }
            
            if (pos>=rawDataLength) {
                
                if (--iterationCount >=0 ) {
                    pos = 0;
                    chunkCount = 0;
                } else {
                    requestShutdown();
                }
            }
        }
        
    }
  

    @Override
    public void shutdown() {
        try {
            if (null!=socketChannel) {
                socketChannel.close();
            }
            if (null!=socket) {
                socket.close();
            }
        } catch (IOException e) {
           //quiet shutodwn
        }        
    }


    
}
