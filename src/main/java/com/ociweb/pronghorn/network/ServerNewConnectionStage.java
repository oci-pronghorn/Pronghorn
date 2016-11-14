package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.net.BindException;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.ServerConnectionSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.ServiceObjectHolder;

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
        
    private static Logger logger = LoggerFactory.getLogger(ServerNewConnectionStage.class);
    
    private Selector selector;
    private int selectionKeysAllowedToWait = 0;//NOTE: should test what happens when we make this bigger.
    private ServerSocketChannel server;
    
    static final int connectMessageSize = ServerConnectionSchema.FROM.fragScriptSize[ServerConnectionSchema.MSG_SERVERCONNECTION_100];
    private ServerCoordinator coordinator;
    private Pipe<ServerConnectionSchema> newClientConnections;
    
    
    //TODO: not sure these are right at all.
    private String host="localhost";
	private int port=8443;
	  
    
    public ServerNewConnectionStage(GraphManager graphManager, ServerCoordinator coordinator, Pipe<ServerConnectionSchema> newClientConnections) {
        super(graphManager, NONE, newClientConnections);
        this.coordinator = coordinator;
        this.newClientConnections = newClientConnections;
    }
    

    
    
    @Override
    public void startup() {

        try {
            SocketAddress endPoint = coordinator.getAddress();
                              
            
            //channel is not used until connected
            //once channel is closed it can not be opened and a new one must be created.
            server = ServerSocketChannel.open();
            
           // server.setOption(StandardSocketOptions.SO_REUSEADDR, Boolean.TRUE);            
            server.socket().bind(endPoint);
            
            ServerSocketChannel channel = (ServerSocketChannel)server.configureBlocking(false);

            //this stage accepts connections as fast as possible
            selector = Selector.open();
            channel.register(selector, SelectionKey.OP_ACCEPT); 
            
            
            System.out.println("ServerNewConnectionStage is now ready on  https:/"+endPoint+"/index.html");
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
                      
                      try {                          
                          channel.configureBlocking(false);
                          channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
                          
                   //       System.out.println("buffer size:"+        channel.socket().getReceiveBufferSize());// channel.getOption(StandardSocketOptions.SO_RCVBUF) );
                          
                      //    System.out.println(     channel.supportedOptions()  );
                          
                      //    channel.setOption(StandardSocketOptions.SO_LINGER, 3);     
                                                    
                          ServiceObjectHolder<ServerConnection> holder = ServerCoordinator.getSocketChannelHolder(coordinator, targetPipeIdx);
                          
                          final long channelId = holder.lookupInsertPosition();
                          
                          //long channelId = ServerCoordinator.getSocketChannelHolder(coordinator, targetPipeIdx).add(channel); //TODO: confirm this returns -1 if nothing is found.
                          if (channelId<0) {
                              //error this should have been detected in the scanForOptimalPipe method
                        	  logger.info("no channel, dropping data");
                              return;
                          }
						  
						  SSLEngine sslEngine = SSLEngineFactory.createSSLEngine();//// not needed for server? host, port);
						  sslEngine.setUseClientMode(false); //here just to be complete and clear
						//  sslEngine.setNeedClientAuth(true); //only if the auth is required to have a connection
						  sslEngine.setWantClientAuth(false); //the auth is optional
						  sslEngine.setNeedClientAuth(false);
						  
						  
						  sslEngine.beginHandshake();
						  
						  holder.setValue(channelId, new ServerConnection(sslEngine, channel, channelId));
                          
						  
//						  HandshakeStatus handshakeStatus = sslEngine.getHandshakeStatus();
//						  logger.info("external handshake check is now {} NEED TO SEND HANDSHAKE FROM HERE?",handshakeStatus);
						  
                          
                          //NOTE: for servers that do not require an upgrade we can set this to the needed pipe right now.
                          ServerCoordinator.setTargetUpgradePipeIdx(coordinator, targetPipeIdx, channelId, 0); //default for all
                                                                                                  
                         // logger.info("register new data to selector for pipe {}",targetPipeIdx);
                          Selector selector2 = ServerCoordinator.getSelector(coordinator, targetPipeIdx);
						  channel.register(selector2, SelectionKey.OP_READ, ServerCoordinator.selectorKeyContext(coordinator, targetPipeIdx, channelId));
    						 
                          //the pipe selected has already been checked to ensure room for the connect message                      
                          Pipe<ServerConnectionSchema> targetPipe = newClientConnections;
                          
                          int msgSize = Pipe.addMsgIdx(targetPipe, ServerConnectionSchema.MSG_SERVERCONNECTION_100);
                          
                          Pipe.addLongValue(targetPipeIdx,targetPipe);
                          Pipe.addLongValue(channelId, targetPipe);
                          
                          Pipe.confirmLowLevelWrite(targetPipe, msgSize);
                          Pipe.publishWrites(targetPipe);
                          
                      } catch (IOException e) {
                    	  logger.trace("Unable to accept connection",e);
                      } 

                  } else {
                	  logger.error("should this be run at all?");
                      assert(0 != (SelectionKey.OP_CONNECT & readyOps)) : "only expected connect";
                      ((SocketChannel)key.channel()).finishConnect(); //TODO: if this does not scale we should move it to the IOStage.
                  }
                                   
                }
            }
        } catch (IOException e) {
        	logger.trace("Unable to open new incoming connection",e);
        }
    }



    
    private static String[] intersection(String[] a, String[] b) {
        return a;
    }

}
