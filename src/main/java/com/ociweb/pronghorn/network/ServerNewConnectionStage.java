package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

import javax.net.ssl.SSLEngine;

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
        
    private static final int CONNECTION_TIMEOUT = 7_000; 

	private static Logger logger = LoggerFactory.getLogger(ServerNewConnectionStage.class);
    
    private Selector selector;
    private int selectionKeysAllowedToWait = 0;//NOTE: should test what happens when we make this bigger.
    private ServerSocketChannel server;
    
    static final int connectMessageSize = ServerConnectionSchema.FROM.fragScriptSize[ServerConnectionSchema.MSG_SERVERCONNECTION_100];
    private ServerCoordinator coordinator;
    private Pipe<ServerConnectionSchema> newClientConnections;

	public static ServerNewConnectionStage newIntance(GraphManager graphManager, ServerCoordinator coordinator, Pipe<ServerConnectionSchema> newClientConnections, boolean isTLS) {
		return new ServerNewConnectionStage(graphManager,coordinator,newClientConnections);
	}
	
    public ServerNewConnectionStage(GraphManager graphManager, ServerCoordinator coordinator, Pipe<ServerConnectionSchema> newClientConnections) {
        super(graphManager, NONE, newClientConnections);
        this.coordinator = coordinator;
        this.newClientConnections = newClientConnections;
    }
    

    
    
    @Override
    public void startup() {

    	SocketAddress endPoint = null;

    	try {
            logger.info("startup of new server");
    		//channel is not used until connected
    		//once channel is closed it can not be opened and a new one must be created.
    		server = ServerSocketChannel.open();
    		
    		//to ensure that this port can be re-used quickly for testing and other reasons
    		server.setOption(StandardSocketOptions.SO_REUSEADDR, Boolean.TRUE);
    		
    
    		endPoint = bindAddressPort(coordinator.host(), coordinator.port());
            
            ServerSocketChannel channel = (ServerSocketChannel)server.configureBlocking(false);

            //this stage accepts connections as fast as possible
            selector = Selector.open();
            
            channel.register(selector, SelectionKey.OP_ACCEPT); 
            
            //trim of local domain name when present.
            String host = endPoint.toString();

            int hidx = host.indexOf('/');
            if (hidx==0) {
            	host = host.substring(hidx+1,host.length());
            } else if (hidx>0) {
            	int colidx = host.indexOf(':');
            	if (colidx<0) {
            		host = host.substring(0,hidx);
            	} else {
            		host = host.substring(0,hidx)+host.substring(colidx,host.length());
            	}
            }
            
            System.out.println(coordinator.serviceName()+" is now ready on  http"+(coordinator.isTLS?"s":"")+":/"+host+"/"+coordinator.defaultPath());
            
            
        } catch (SocketException se) {
         
	    	if (se.getMessage().contains("Permission denied")) {
	    		logger.warn("\nUnable to open {} due to {}",endPoint,se.getMessage());
	    		coordinator.shutdown();
	    		return;
	    	} else {
	        	if (se.getMessage().contains("already in use")) {
	                logger.warn("{}",endPoint,se.getMessage());
	                coordinator.shutdown();
	                return;
	            }
	    	}
            throw new RuntimeException(se);
        } catch (IOException e) {
           if (e.getMessage().contains("Unresolved address")) {
        	   logger.warn("\nUnresolved host address  http"+(coordinator.isTLS?"s":""));
        	   coordinator.shutdown();
               return;
           }
        	
           throw new RuntimeException(e);
           
        }
        
    }

	private SocketAddress bindAddressPort(String host, int port) throws IOException, BindException {
		
		InetSocketAddress endPoint = null;
		
		long timeout = System.currentTimeMillis()+CONNECTION_TIMEOUT;
		boolean notConnected = true;
		int printWarningCountdown = 20;
		do {
		    try{
		    	if (null == endPoint) {
		    		endPoint = new InetSocketAddress(host, port);		
		    		logger.info("bind to {} ",endPoint);
		    	}
		    	server.socket().bind(endPoint);
		    	notConnected = false;
		    } catch (BindException se) {
		    	if (System.currentTimeMillis() > timeout) {
		    		logger.warn("Timeout attempting to open open {}",endPoint,se.getMessage());
		    		coordinator.shutdown();
		    		throw se;
		    	} else {
		    		//small pause before retry
		    		try {
		    			if (0 == printWarningCountdown--) {
		    				logger.warn("Unable to open {} this port appears to already be in use.",endPoint);
		    			}
						Thread.sleep( printWarningCountdown>=0 ? 5 : 20);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						break;
					}
		    	}
		    }
		} while (notConnected);
		return endPoint;
	}

    @Override
    public void run() {
  
        try {//selector may be null if shutdown was called on startup.
           if (null!=selector && selector.selectNow() > selectionKeysAllowedToWait) {
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

                      if (!ServerCoordinator.scanForOptimalPipe(coordinator)) {                    	  
                    	  return;//try again later if the client is still waiting.
                      }
                      int targetPipeIdx = 0;//NOTE: this will be needed for rolling out new sites and features atomicly
                      
                      
                      SocketChannel channel = server.accept();
                      
                      try {                          
                          channel.configureBlocking(false);
                          
                          //TCP_NODELAY is requried for HTTP/2 get used to to being on.
                          //channel.setOption(StandardSocketOptions.TCP_NODELAY, true);  
                          
                          
                          //channel.setOption(StandardSocketOptions.SO_RCVBUF, 1<<19);
                          //channel.setOption(StandardSocketOptions.SO_SNDBUF, 1<<19); //for heavy testing we avoid overloading client by making this smaller.
                          
                          //	logger.info("server recv buffer size {} ",  channel.getOption(StandardSocketOptions.SO_RCVBUF)); //default  531000
                          //	logger.info("server send buffer size {} ",  channel.getOption(StandardSocketOptions.SO_SNDBUF)); //default 1313280
                          //    logger.info("send buffer size {} ",  channel.getOption(StandardSocketOptions.SO_SNDBUF));
                          
                          ServiceObjectHolder<ServerConnection> holder = ServerCoordinator.getSocketChannelHolder(coordinator);
                          
                          final long channelId = holder.lookupInsertPosition();
                          if (channelId<0) {
                              //error this should have been detected in the scanForOptimalPipe method
                        	  logger.info("no channel, dropping data");
                              return;
                          }
						  
                          SSLEngine sslEngine = null;
                          if (coordinator.isTLS) {
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
                          Selector selector2 = ServerCoordinator.getSelector(coordinator);
						  channel.register(selector2, SelectionKey.OP_READ, ServerCoordinator.selectorKeyContext(coordinator, channelId));
    						 
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
