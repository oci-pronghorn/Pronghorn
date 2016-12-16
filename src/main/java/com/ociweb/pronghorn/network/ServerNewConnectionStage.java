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
    private final boolean isTLS;

	      
	public static ServerNewConnectionStage newIntance(GraphManager graphManager, ServerCoordinator coordinator, Pipe<ServerConnectionSchema> newClientConnections, boolean isTLS) {
		return new ServerNewConnectionStage(graphManager,coordinator,newClientConnections,isTLS);
	}
	
    public ServerNewConnectionStage(GraphManager graphManager, ServerCoordinator coordinator, Pipe<ServerConnectionSchema> newClientConnections, boolean isTLS) {
        super(graphManager, NONE, newClientConnections);
        this.coordinator = coordinator;
        this.newClientConnections = newClientConnections;
        this.isTLS = isTLS;
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
            
            
            System.out.println("Server is now ready on  http"+(isTLS?"s":"")+":/"+endPoint+"/");
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
                          //channel.setOption(StandardSocketOptions.TCP_NODELAY, true);  
                         // channel.setOption(StandardSocketOptions.SO_SNDBUF, 1<<16); //for heavy testing we avoid overloading client by making this smaller.
                                     
                          channel.socket().setTcpNoDelay(true);
                          
                         // logger.info("send buffer size {} ",  channel.getOption(StandardSocketOptions.SO_SNDBUF));
                          
                          ServiceObjectHolder<ServerConnection> holder = ServerCoordinator.getSocketChannelHolder(coordinator, targetPipeIdx);
                          
                          final long channelId = holder.lookupInsertPosition();                          
                          
                          //System.err.println("lookup insert channel position:" +channelId+" out of "+holder.size());
                          
                          
                          //long channelId = ServerCoordinator.getSocketChannelHolder(coordinator, targetPipeIdx).add(channel); //TODO: confirm this returns -1 if nothing is found.
                          if (channelId<0) {
                              //error this should have been detected in the scanForOptimalPipe method
                        	  logger.info("no channel, dropping data");
                              return;
                          }
						  
                          SSLEngine sslEngine = null;
                          if (isTLS) {
							  sslEngine = SSLEngineFactory.createSSLEngine();//// not needed for server? host, port);
							  sslEngine.setUseClientMode(false); //here just to be complete and clear
							//  sslEngine.setNeedClientAuth(true); //only if the auth is required to have a connection
							 // sslEngine.setWantClientAuth(true); //the auth is optional
							  sslEngine.setNeedClientAuth(false); //required for openSSL/boringSSL
							  sslEngine.beginHandshake();
                          }
						  
						  
						  //logger.debug("server new connection attached for new id {} ",channelId);
						  
						  holder.setValue(channelId, new ServerConnection(sslEngine, channel, channelId));
                                                                                                                            
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
