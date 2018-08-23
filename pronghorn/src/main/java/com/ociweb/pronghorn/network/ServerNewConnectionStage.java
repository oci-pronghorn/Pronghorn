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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Consumer;

import javax.net.ssl.SSLEngine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * No protocol specifics are found in this class, only socket usage logic.
 * 
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class ServerNewConnectionStage extends PronghornStage{
        
    private static final int CONNECTION_TIMEOUT = 7_000; 

	private static final Logger logger = LoggerFactory.getLogger(ServerNewConnectionStage.class);
    
    private Set<SelectionKey> selectedKeys;	
    private ArrayList<SelectionKey> doneSelectors = new ArrayList<SelectionKey>(100);
	
    private Selector selector;
    private ServerSocketChannel server;
    private String host;
    private final long startupTimeNS;
    
    static final int connectMessageSize = ServerConnectionSchema.FROM.fragScriptSize[ServerConnectionSchema.MSG_SERVERCONNECTION_100];

    private ServerCoordinator coordinator;
    private Pipe<ServerConnectionSchema> newClientConnections;
    private final String label;
    private boolean needsToNotifyStartup;
    
	public static ServerNewConnectionStage newIntance(GraphManager graphManager, ServerCoordinator coordinator, Pipe<ServerConnectionSchema> newClientConnections, boolean isTLS) {
		return new ServerNewConnectionStage(graphManager,coordinator,newClientConnections);
	}

	/**
	 *
	 * @param graphManager
	 * @param coordinator
	 * @param newClientConnections _out_ The ServerConnectionSchema containing the newest client information.
	 */
    public ServerNewConnectionStage(GraphManager graphManager, ServerCoordinator coordinator, Pipe<ServerConnectionSchema> newClientConnections) {
        super(graphManager, NONE, newClientConnections);
        this.coordinator = coordinator;
        this.startupTimeNS = graphManager.startupTimeNS;        
        this.label = coordinator.host()+":"+coordinator.port();
        
        this.newClientConnections = newClientConnections;
        
        GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lemonchiffon3", this);
        GraphManager.addNota(graphManager, GraphManager.SLA_LATENCY, 100_000_000, this);

    }
    
    public final Consumer<SelectionKey> selectionKeyAction = new Consumer<SelectionKey>(){
		@Override
		public void accept(SelectionKey selection) {
			processSelection(selection); 
		}
    };   

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

    private boolean hasNewDataToRead() {
    	
    	if (null!=selectedKeys && !selectedKeys.isEmpty()) {
    		return true;
    	}
    		
        try {
        	////////////
        	//CAUTION - select now clears pevious count and only returns the additional I/O opeation counts which have become avail since the last time SelectNow was called
        	////////////        	
            if (null!=selector && selector.selectNow() > 0) {
            	selectedKeys = selector.selectedKeys();
            	return true;
            } else {
            	return false;
            }

            //    logger.info("pending new selections {} ",pendingSelections);
        } catch (IOException e) {
        	logger.warn("new connections",e);
            return false;
        }
    }
    
    @Override
    public void run() {
  
    	//we always do this because we always poll for new data from the OS
    	if (null != this.didWorkMonitor) {
    		this.didWorkMonitor.published();
    	}
    	
    	if (!needsToNotifyStartup) {	  
	    	//long now = System.nanoTime();
	    	    		  
    		   int maxIterations = 1000;
	           while (hasNewDataToRead() && --maxIterations>=0) {
	                //we know that there is an interesting (non zero positive) number of keys waiting.
	        	    doneSelectors.clear();
	        	    selectedKeys.forEach(selectionKeyAction);	                
	                removeDoneKeys(selectedKeys);
	            }
	 	        
		} else {
			reportServerIsRunning(host);
			needsToNotifyStartup=false;
		}
	
    }


	private void removeDoneKeys(Set<SelectionKey> selectedKeys) {
		//sad but this is the best way to remove these without allocating a new iterator
		// the selectedKeys.removeAll(doneSelectors); will produce garbage upon every call
		ArrayList<SelectionKey> doneSelectors2 = doneSelectors;
		int c = doneSelectors2.size();
		while (--c>=0) {
		    	selectedKeys.remove(doneSelectors2.get(c));
		}

	}
	
	private void processSelection(SelectionKey key) {
		
		if (null!=newClientConnections 
			&& !Pipe.hasRoomForWrite(newClientConnections, ServerNewConnectionStage.connectMessageSize)) {
			return;
		}

		int readyOps = key.readyOps();
		                    
		  if (0 != (SelectionKey.OP_ACCEPT & readyOps)) {
		      ServiceObjectHolder<ServerConnection> holder = ServerCoordinator.getSocketChannelHolder(coordinator);

		      //if we use an old position the old object will be decomposed to ensure there is no leak.
		      long channelId = holder.lookupInsertPosition();	
		      
		      //NOTE: warning this can accept more connections than we have open pipes, these connections will pile up in the socket reader by design.
		      	                      
		      if (channelId>=0) {		                    
		     
		    	  //TODO: remove this...
		          int targetPipeIdx = 0;//NOTE: this will be needed for rolling out new sites and features atomicly
		                                		          
		          try {                          
		        	  SocketChannel channel = server.accept();
		              channel.configureBlocking(false);
		              
		              //TCP_NODELAY is required for HTTP/2 get used to it being on now.
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
					  							  
					  
		              ServerConnection old = holder.setValue(channelId, 
		            		  		  new ServerConnection(sslEngine, 
		            		  				  		       channel, channelId,
		            				                       coordinator)
		            		  		  );
		              if (null!=old) {
							old.close();
							old.decompose();
						}
		              
		            // logger.info("\naccepting new connection {} registered data selector", channelId); 
		        		           
		                          
		              
		              channel.register(ServerCoordinator.getSelector(coordinator), 
							           SelectionKey.OP_READ, 
							           ServerCoordinator.selectorKeyContext(coordinator, channelId));
						
					//  logger.info("\nnew server connection attached for new id {} ",channelId);
					  if (null!=newClientConnections) {								  
		                  publishNotificationOFNewConnection(targetPipeIdx, channelId);
					  }
					  
		              
		          } catch (IOException e) {
		        	  logger.error("\nUnable to accept connection",e);
		          } 
		          doneSelectors.add(key);		          
		      } else {
		    	  
		    	  // TODO: find old connections and recycle them if they are no longer used..
		    	  // channelId holds the negative least used channel in case we want to close it
		    	  // any connection object with a pool reservation of -1 is also a good candidate...
		    	  //use this method to check
		    	//  coordinator.connectionForSessionId(channelId).getPoolReservation()
		    	  
		    	  logger.error("\n****max connections achieved, no more connections.****");
		      }

		  } else {
		      assert(0 != (SelectionKey.OP_CONNECT & readyOps)) : "only expected connect";
		      try {
				((SocketChannel)key.channel()).finishConnect();
				doneSelectors.add(key);
			} catch (IOException e) {
				logger.error("Unable to finish connect",e);
			} 
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



}
