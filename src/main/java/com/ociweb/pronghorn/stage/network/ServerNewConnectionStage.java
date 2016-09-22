package com.ociweb.pronghorn.stage.network;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.network.schema.ServerConnectionSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

/**
 * General base class for server construction.
 * Server should minimize garbage but unlike client may not be possible to remove it.
 * 
 * No protocol specifics are found in this class only socket usage logic
 * 
 * @author Nathan Tippy
 *
 */
public class ServerNewConnectionStage extends PronghornStage{
        
    private static Logger log = LoggerFactory.getLogger(ServerNewConnectionStage.class);
    
    private Selector selector;
    private int selectionKeysAllowedToWait = 0;//NOTE: should test what happens when we make this bigger.
    private ServerSocketChannel server;
    
    static final int connectMessageSize = ServerConnectionSchema.FROM.fragScriptSize[ServerConnectionSchema.MSG_SERVERCONNECTION_100];
    private ServerCoordinator coordinator;
    private Pipe<ServerConnectionSchema> newClientConnections;
    
    private SSLSocketFactory sf;
    
    private String[] STRONG_PROTOCOLS = new String[] {"SSLv3",
                                                    "TLSv1",
                                                    "TLSv1.1",
                                                    "SSLv2Hello"};
    private String[] STRONG_CIPHER_SUITES = new String[] { 
                                            "TLS_RSA_WITH_DES_CBC_SHA",
                                            "TLS_DHE_DSS_WITH_DES_CBC_SHA",
                                            "TLS_DHE_RSA_WITH_DES_CBC_SHA",
                                       //     "TLS_DHE_DSS_EXPORT1024_WITH_DES_CBC_SHA",
                                            "TLS_RSA_WITH_3DES_EDE_CBC_SHA",
                                            "TLS_RSA_WITH_RC4_128_SHA",
                                            "TLS_RSA_WITH_RC4_128_MD5",
                                            "TLS_DHE_DSS_WITH_3DES_EDE_CBC_SHA",
                                            "TLS_DHE_DSS_WITH_RC4_128_SHA",
                                            "TLS_DHE_RSA_WITH_3DES_EDE_CBC_SHA",
                                            ///
                                            "SSL_DHE_DSS_WITH_3DES_EDE_CBC_SHA",
                                            "SSL_DHE_RSA_WITH_3DES_EDE_CBC_SHA",
                                            "TLS_RSA_WITH_AES_128_CBC_SHA",
                                            "TLS_RSA_WITH_AES_256_CBC_SHA",
                                            "SSL_RSA_WITH_3DES_EDE_CBC_SHA",
                                            "SSL_RSA_WITH_RC4_128_MD5",
                                            "SSL_RSA_WITH_RC4_128_SHA"
    
    };
    
 //   private String[] protocolsInUse = 
    
    
    public ServerNewConnectionStage(GraphManager graphManager, ServerCoordinator coordinator, Pipe<ServerConnectionSchema> newClientConnections) {
        super(graphManager, NONE, newClientConnections);
        this.coordinator = coordinator;
        this.newClientConnections = newClientConnections;
    }
    

    
    
    @Override
    public void startup() {

        try {
            sf = ((SSLSocketFactory) SSLSocketFactory.getDefault());
            SocketAddress endPoint = coordinator.getAddress();
                              
            
            //channel is not used until connected
            //once channel is closed it can not be opened and a new one must be created.
            server = ServerSocketChannel.open();
            server.socket().bind(endPoint);
            
            ServerSocketChannel channel = (ServerSocketChannel)server.configureBlocking(false);

            //this stage accepts connections as fast as possible
            selector = Selector.open();
            channel.register(selector, SelectionKey.OP_ACCEPT); 

            System.out.println("ServerNewConnectionStage is now ready on  "+endPoint);
        } catch (BindException be) {
            String msg = be.getMessage();
            if (msg.contains("already in use")) {
                System.out.println(msg);
                System.out.println("shutting down");
                System.exit(0);
            }
            throw new RuntimeException(be);
        } catch (IOException e) {
            
           throw new RuntimeException(e);
           
        }
        
    }

    @Override
    public void run() {
//        try {
//            Thread.sleep(3);
//        } catch (InterruptedException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
        
        try {
           if (selector.selectNow() > selectionKeysAllowedToWait) {
                //we know that there is an interesting (non zero positive) number of keys waiting.
                                
                Iterator<SelectionKey> keyIterator = selector.selectedKeys().iterator();
                while (keyIterator.hasNext()) {
                
                  SelectionKey key = keyIterator.next();
                  keyIterator.remove();
                  int readyOps = key.readyOps();
                                    
                  if (0 != (SelectionKey.OP_ACCEPT & readyOps)) {
                      
                      if (!Pipe.hasRoomForWrite(newClientConnections, ServerNewConnectionStage.connectMessageSize)) {
                          return;
                      }
                      
                      int targetPipeIdx = ServerCoordinator.scanForOptimalPipe(coordinator,Integer.MAX_VALUE, -1);
                      //if -1 we can not open any new connection on any pipeline.
                      if (targetPipeIdx<0) {
                          return;//try again later if the client is still waiting.
                      }
                      
                      SocketChannel channel = server.accept();
                      
                      boolean useSecureTLSConnection = false; //TOOD: "The default behavior is secure" then allow for the developer to downgrade as needed.
                      
                      if (useSecureTLSConnection) {
                          
                          Socket socket = channel.socket();                      
                          InetSocketAddress remoteAddress = (InetSocketAddress) socket.getRemoteSocketAddress();
                          SSLSocket s = (SSLSocket) (sf.createSocket(socket, remoteAddress.getHostName(), socket.getPort(), true)); 
                          
                          s.setUseClientMode(false);
                          
                          s.setEnabledProtocols(intersection(s.getSupportedProtocols(),STRONG_PROTOCOLS)); //creates garbage and is slow must fix.
                          s.setEnabledCipherSuites(intersection(s.getSupportedCipherSuites(),STRONG_CIPHER_SUITES));
                          s.startHandshake();
       
                      }
                      
                      
                      try {                          
                          channel.configureBlocking(false);
                          channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
                          
                   //       System.out.println("buffer size:"+        channel.socket().getReceiveBufferSize());// channel.getOption(StandardSocketOptions.SO_RCVBUF) );
                          
                      //    System.out.println(     channel.supportedOptions()  );
                          
                      //    channel.setOption(StandardSocketOptions.SO_LINGER, 3);     
                                                    
                          long channelId = ServerCoordinator.getSocketChannelHolder(coordinator, targetPipeIdx).add(channel); //TODO: confirm this returns -1 if nothing is found.
                          if (channelId<0) {
                              //error this should have been detected in the scanForOptimalPipe method
                              return;
                          }
                          
                          //NOTE: for servers that do not require an upgrade we can set this to the needed pipe right now.
                          ServerCoordinator.setTargetUpgradePipeIdx(coordinator, targetPipeIdx, channelId, 0); //default for all
                                                                                                          
                          channel.register(ServerCoordinator.getSelector(coordinator, targetPipeIdx), SelectionKey.OP_READ, ServerCoordinator.selectorKeyContext(coordinator, targetPipeIdx, channelId));
                                                                              
                          //the pipe selected has already been checked to ensure room for the connect message                      
                          Pipe<ServerConnectionSchema> targetPipe = newClientConnections;
                          
                          Pipe.addMsgIdx(targetPipe, ServerConnectionSchema.MSG_SERVERCONNECTION_100);
                          Pipe.addIntValue(targetPipeIdx,targetPipe);
                          Pipe.addLongValue(channelId, targetPipe);
                          Pipe.publishWrites(targetPipe);
                          Pipe.confirmLowLevelWrite(targetPipe, connectMessageSize);
                          
                      } catch (IOException e) {
                          log.trace("Unable to accept connection",e);
                      } 

                  } else {
                      log.error("should this be run at all?");
                      assert(0 != (SelectionKey.OP_CONNECT & readyOps)) : "only expected connect";
                      ((SocketChannel)key.channel()).finishConnect(); //TODO: if this does not scale we should move it to the IOStage.
                  }
                                   
                }
            }
        } catch (IOException e) {
            log.trace("Unable to open new incoming connection",e);
        }
    }



    
    private static String[] intersection(String[] a, String[] b) {
        return a;
    }

}
