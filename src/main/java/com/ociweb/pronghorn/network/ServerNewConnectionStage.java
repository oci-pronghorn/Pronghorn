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

import com.ociweb.pronghorn.network.config.HTTPHeader;
import com.ociweb.pronghorn.network.schema.ServerConnectionSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;
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

	private static final Logger logger = LoggerFactory.getLogger(ServerNewConnectionStage.class);
    
    private Selector selector;
    private int selectionKeysAllowedToWait = 0;//NOTE: should test what happens when we make this bigger.
    private ServerSocketChannel server;
    private String host;
    private final long startupTimeNS;
    
    static final int connectMessageSize = ServerConnectionSchema.FROM.fragScriptSize[ServerConnectionSchema.MSG_SERVERCONNECTION_100];

	private static final long CONNECTION_TTL_MS = 4_000; //may shut off any connections unused for 4 sec
    private ServerCoordinator coordinator;
    private Pipe<ServerConnectionSchema> newClientConnections;
    private final String label;
    private boolean needsToNotifyStartup;
    
	public static ServerNewConnectionStage newIntance(GraphManager graphManager, ServerCoordinator coordinator, Pipe<ServerConnectionSchema> newClientConnections, boolean isTLS) {
		return new ServerNewConnectionStage(graphManager,coordinator,newClientConnections);
	}
	
    public ServerNewConnectionStage(GraphManager graphManager, ServerCoordinator coordinator, Pipe<ServerConnectionSchema> newClientConnections) {
        super(graphManager, NONE, newClientConnections);
        this.coordinator = coordinator;
        this.startupTimeNS = graphManager.startupTimeNS;        
        this.label = coordinator.host()+":"+coordinator.port();
        
        this.newClientConnections = newClientConnections;
        
        GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lemonchiffon3", this);
        
 //       GraphManager.addNota(graphManager, GraphManager.ISOLATE, GraphManager.ISOLATE, this);
        //much larger limit since nothing needs this thread back.
        GraphManager.addNota(graphManager, GraphManager.SLA_LATENCY, 100_000_000, this);
        
        
    }
    
	public static ServerNewConnectionStage newIntance(GraphManager graphManager, ServerCoordinator coordinator, boolean isTLS) {
		return new ServerNewConnectionStage(graphManager,coordinator);
	}
	
    public ServerNewConnectionStage(GraphManager graphManager, ServerCoordinator coordinator) {
        super(graphManager, NONE, NONE);
        this.coordinator = coordinator;
        this.startupTimeNS = graphManager.startupTimeNS;  
        this.label = coordinator.host()+":"+coordinator.port();
        
        this.newClientConnections = null;
        GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lemonchiffon3", this);
        GraphManager.addNota(graphManager, GraphManager.ISOLATE, GraphManager.ISOLATE, this);
  
        GraphManager.addNota(graphManager, GraphManager.SLA_LATENCY, 100_000_000, this);
         
    }
    
    @Override
    public String toString() {
    	String root = super.toString();
    	return root+"\n"+label+"\n";
    }
    
    @Override
    public void startup() {

    	/////////////////////////////////////////////////////////////////////////////////////
    	//The trust manager MUST be established before any TLS connection work begins
    	//If this is not done there can be race conditions as to which certs are trusted...
    	if (coordinator.isTLS) {
    		coordinator.engineFactory.initTLSService();
    	}
    	logger.trace("init of Server TLS called {}",coordinator.isTLS);
    	/////////////////////////////////////////////////////////////////////////////////////
    	

    	SocketAddress endPoint = null;

    	try {
    		
            //logger.info("startup of new server");
    		//channel is not used until connected
    		//once channel is closed it can not be opened and a new one must be created.
    		server = ServerSocketChannel.open();
    		
    		//to ensure that this port can be re-used quickly for testing and other reasons
    		server.setOption(StandardSocketOptions.SO_REUSEADDR, Boolean.TRUE);
    		server.socket().setPerformancePreferences(1, 2, 0);
    		server.socket().setSoTimeout(0);
    		    		
    		endPoint = bindAddressPort(coordinator.host(), coordinator.port());
            
            ServerSocketChannel channel = (ServerSocketChannel)server.configureBlocking(false);

            //this stage accepts connections as fast as possible
            selector = Selector.open();
            
            channel.register(selector, SelectionKey.OP_ACCEPT); 
            
            extractHostString(endPoint);
            needsToNotifyStartup=true;          
            
        } catch (SocketException se) {
         
	    	if (se.getMessage().contains("Permission denied")) {
	    		logger.warn("\nUnable to open {} due to {}",coordinator.port(),se.getMessage());
	    		coordinator.shutdown();
	    		return;
	    	} else {
	        	if (se.getMessage().contains("already in use")) {
	                logger.warn("Already in use: {} {}",coordinator.host(), coordinator.port());
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

	private void extractHostString(SocketAddress endPoint) {
		//trim of local domain name when present.
		host = endPoint.toString();

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
	}

	private void reportServerIsRunning(String host) {
		//ensure reporting is done together
		synchronized(logger) {
			System.out.println();
			
			System.out.append(coordinator.serviceName()).append(" is now ready on http");
			if (coordinator.isTLS) {
				System.out.append("s");
			}
			System.out.append("://").append(host).append("/").append(coordinator.defaultPath()).append("\n");
			
			Appendables.appendValue(
				System.out.append(coordinator.serviceName())
				.append(" max connections: ")
				,coordinator.channelBitsSize).append("\n");
			
			Appendables.appendValue(
				System.out.append(coordinator.serviceName())
				.append(" max concurrent inputs: ")
				,coordinator.maxConcurrentInputs).append("\n");  
			
			Appendables.appendValue(
					System.out.append(coordinator.serviceName())
					.append(" concurrent tracks: ")
					,coordinator.moduleParallelism()).append("\n");
			
			Appendables.appendValue(
					System.out.append(coordinator.serviceName())
					.append(" max concurrent outputs: ")
					,coordinator.maxConcurrentOutputs).append("\n");
		    
		    Appendables.appendNearestTimeUnit(System.out, System.nanoTime() - this.startupTimeNS).append(" total startup time.\n");
		    
		    System.out.println();
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
		    		logger.trace("bind to {} ",endPoint);
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
						Thread.sleep( printWarningCountdown>=0 ? 4 : 20);
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
  
    	if (!needsToNotifyStartup) {	  
	    	//long now = System.nanoTime();
	    	
	        try {//selector may be null if shutdown was called on startup.
	           if (null!=selector && selector.selectNow() > selectionKeysAllowedToWait) {
	                //we know that there is an interesting (non zero positive) number of keys waiting.
	        
	        	    //we have no pipes to monitor so this must be done explicity
	        	    if (null != this.didWorkMonitor) {
	        	    	this.didWorkMonitor.published();
	        	    }
	        	    
	                Iterator<SelectionKey> keyIterator = selector.selectedKeys().iterator();
	                while (keyIterator.hasNext()) {
	                
	                  SelectionKey key = keyIterator.next();
	                  keyIterator.remove();
	                  int readyOps = key.readyOps();
	                                    
	                  if (0 != (SelectionKey.OP_ACCEPT & readyOps)) {
	                     
	                	  //ServerCoordinator.acceptConnectionStart = now;
	                	  
	                      if (null!=newClientConnections && !Pipe.hasRoomForWrite(newClientConnections, ServerNewConnectionStage.connectMessageSize)) {
	                          return;
	                      }
	
	                      ServiceObjectHolder<ServerConnection> holder = ServerCoordinator.getSocketChannelHolder(coordinator);
	                                            
	                      long channelId = holder.lookupInsertPosition();
	                      if (channelId<0) {
	                    	  
	                    	  long leastUsedConnectionId = (-channelId);
	                    	  ServerConnection tempConnection = holder.get(leastUsedConnectionId);
	                    	  long now = System.currentTimeMillis();
	                    	  if ( (tempConnection!=null)
	                    		  && (tempConnection.isValid)
	                    		  && (now-tempConnection.getLastUsedTime() < CONNECTION_TTL_MS )
	                    			  ) {
	                    		  //We have no connections we can replace so do not accept this
	                    		  return;//try again later if the client is still waiting.	                    		  
	                    	  } else {
	                    		  if (null!=tempConnection) {
	                    			  tempConnection.close();
	                    		  }
	                    		  
	                    		  //reuse old index for this new connection
	                    		  channelId = leastUsedConnectionId;	                    		  
	                    	  }
	                      }
	                      
	                      int targetPipeIdx = 0;//NOTE: this will be needed for rolling out new sites and features atomicly
	                                            
	                      SocketChannel channel = server.accept();
	                      
	                      try {                          
	                          channel.configureBlocking(false);
	                          
	                          //TCP_NODELAY is requried for HTTP/2 get used to it being on now.
	                          channel.setOption(StandardSocketOptions.TCP_NODELAY, Boolean.TRUE);  
	                          channel.socket().setPerformancePreferences(1, 0, 2);
	                 
	                          SSLEngine sslEngine = null;
	                          if (coordinator.isTLS) {
								  sslEngine = coordinator.engineFactory.createSSLEngine();//// not needed for server? host, port);
								  sslEngine.setUseClientMode(false); //here just to be complete and clear
								  // sslEngine.setNeedClientAuth(true); //only if the auth is required to have a connection
								  // sslEngine.setWantClientAuth(true); //the auth is optional
								  sslEngine.setNeedClientAuth(coordinator.requireClientAuth); //required for openSSL/boringSSL
								  
								  sslEngine.beginHandshake();
	                          }
							  
							  
							//  logger.info("new server connection attached for new id {} ",channelId);
							  
	                          holder.setValue(channelId, 
	                        		  new ServerConnection(sslEngine, 
	                        				  channel, channelId,
	                        				  coordinator));
	                          
	                         
	                                                                                                                            
	                         // logger.info("register new data to selector for pipe {}",targetPipeIdx);
	                          Selector selector2 = ServerCoordinator.getSelector(coordinator);
							  channel.register(selector2, SelectionKey.OP_READ, ServerCoordinator.selectorKeyContext(coordinator, channelId));
	    						
							  if (null!=newClientConnections) {
								  
		                          publishNotificationOFNewConnection(targetPipeIdx, channelId);
							  }
							  
	                          
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
	        
		} else {
			reportServerIsRunning(host);
			needsToNotifyStartup=false;
		}
	
    }

	private void publishNotificationOFNewConnection(int targetPipeIdx, final long channelId) {
		//the pipe selected has already been checked to ensure room for the connect message                      
		  Pipe<ServerConnectionSchema> targetPipe = newClientConnections;
		  
		  int msgSize = Pipe.addMsgIdx(targetPipe, ServerConnectionSchema.MSG_SERVERCONNECTION_100);
		  
		  Pipe.addLongValue(targetPipeIdx,targetPipe);
		  Pipe.addLongValue(channelId, targetPipe);
		  
		  Pipe.confirmLowLevelWrite(targetPipe, msgSize);
		  Pipe.publishWrites(targetPipe);
	}



    
    private static String[] intersection(String[] a, String[] b) {
        return a;
    }

}
