package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.StandardSocketOptions;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Set;
import java.util.function.Consumer;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;

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
        
    private static final int CONNECTION_TIMEOUT = 12_000; 

	private static final Logger logger = LoggerFactory.getLogger(ServerNewConnectionStage.class);
    
    private Set<SelectionKey> selectedKeys;	
    private ArrayList<SelectionKey> doneSelectors = new ArrayList<SelectionKey>(100);
	
    private Selector selector;
    private ServerSocketChannel serverSocketChannel;
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
        GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, 500_000, this);
        
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
        GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, 500_000, this);
        
        
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
    		serverSocketChannel = ServerSocketChannel.open();
       	    		
    		//by design we set this in both places
    		serverSocketChannel.socket().setReceiveBufferSize(1<<19);
      	  
    		//to ensure that this port can be re-used quickly for testing and other reasons
    		serverSocketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, Boolean.TRUE);
    		serverSocketChannel.socket().setPerformancePreferences(0,1,2);  //(1, 2, 0);
    		serverSocketChannel.socket().setSoTimeout(0);
    		    		
    		endPoint = bindAddressPort(coordinator.host(), coordinator.port());
            
            ServerSocketChannel channel = (ServerSocketChannel)serverSocketChannel.configureBlocking(false);

            //this stage accepts connections as fast as possible
            selector = Selector.open();
            
            channel.register(selector, SelectionKey.OP_ACCEPT); 
            
            extractHostString(endPoint);
            needsToNotifyStartup=true;          
            
        } catch (SocketException se) {
         
	    	if (se.getMessage().contains("Permission denied")) {
	    		logger.warn("\n{} Unable to open {} due to {}",coordinator.serviceName(),coordinator.port(),se.getMessage());
	    		coordinator.shutdown();
	    		throw new RuntimeException(se);//server can not run at this point so we must have a hard stop

	    	} else {
	        	if (se.getMessage().contains("already in use")) {
	                logger.warn("\n{} Already in use {}:{}",coordinator.serviceName(),coordinator.host(), coordinator.port());
	                coordinator.shutdown();
	                throw new RuntimeException(se);//server can not run at this point so we must have a hard stop

	            }
	    	}
            throw new RuntimeException(se);
        } catch (IOException e) {
           if (e.getMessage().contains("Unresolved address")) {
        	   logger.warn("\n{} Unresolved host address  http"+(coordinator.isTLS?"s":""),coordinator.serviceName());
        	   coordinator.shutdown();
        	   throw new RuntimeException(e);//server can not run at this point so we must have a hard stop
 
           }
        	
           throw new RuntimeException(e);
           
        }
        //logger.info("startup done");
    }
    
    @Override
    public void shutdown() {
        //must shut down or follow on tests may end up blocked.
    	try {
    		if (null!=selector) {
    			selector.close();
    		}
    		if (null!=serverSocketChannel) {
    			serverSocketChannel.close();
    		}
		} catch (IOException e) {
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
		if ("0.0.0.0".equals(host)) {
			logger.warn("Developer used 0.0.0.0 as the host so this server is running on all known networks,\n              It is much more secure to define an explicit network binding like this eg. '10.*.*.*' ");
			endPoint = new InetSocketAddress(port);
			serverSocketChannel.socket().bind(endPoint);
			return endPoint;
		} else {
			
			long timeout = System.currentTimeMillis()+CONNECTION_TIMEOUT;
			boolean notConnected = true;
			int printWarningCountdown = 20;
			do {
			    try{
			    	if (null == endPoint) {
			    		endPoint = new InetSocketAddress(host, port);		
			    		logger.trace("bind to {} ",endPoint);
			    	}
			    	serverSocketChannel.socket().bind(endPoint);
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
	}

    private boolean hasNewDataToRead() {
    	
    	assert(null==selectedKeys || selectedKeys.isEmpty()) : "All selections should be processed";
    	    		
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

        } catch (ClosedSelectorException cse) {
        	return false;
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

    	assert(null==selectedKeys || selectedKeys.isEmpty()) : selectedKeys.size()+" How is this even possible? All selections should be processed";
    	
    	if (!needsToNotifyStartup) {	  
	    	//long now = System.nanoTime();
	    	    		  
	           while (hasNewDataToRead()) {
	        	    doWork = true;
	                //we know that there is an interesting (non zero positive) number of keys waiting.
	        	    doneSelectors.clear();
	        	    selectedKeys.forEach(selectionKeyAction);	                
	                removeDoneKeys(selectedKeys);
	                
	                assert(null==selectedKeys || selectedKeys.isEmpty()) : "All selections should be processed";
	                if (!doWork) {
	                	break;
	                }
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
	
	
	public final static boolean limitHandshakeConcurrency = false; //great for testing
	
	public boolean doWork;	
	private SSLEngine currentHandshake = null;
	
	private void processSelection(SelectionKey key) {
		
		
		doneSelectors.add(key);	
		
		if (null!=newClientConnections 	&& !Pipe.hasRoomForWrite(newClientConnections, ServerNewConnectionStage.connectMessageSize)) {
			doWork = false;
			return;
		}
		
		
		int readyOps = key.readyOps();
		                    
		if (0 != (SelectionKey.OP_ACCEPT & readyOps)) {
		
	        //for testing only:  this block limits the servder so only 1 client may handshake at a time.
			if (limitHandshakeConcurrency && coordinator.isTLS && null!=currentHandshake) {					
				HandshakeStatus status = currentHandshake.getHandshakeStatus();					
				if (status == HandshakeStatus.FINISHED || status== HandshakeStatus.NOT_HANDSHAKING) {
					currentHandshake = null;
					//logger.trace("\nfinished one more handshake");
					Thread.yield();
				} else {
					doWork = false;
					return;//do not accept new connections until this handshake is complete.
				}					
			}
			
			
			ServiceObjectHolder<ServerConnection> holder = ServerCoordinator.getSocketChannelHolder(coordinator);
			
			//if we use an old position the old object will be decomposed to ensure there is no leak.
			long channelId = holder.lookupInsertPosition();	
			
		      
		      //NOTE: warning this can accept more connections than we have open pipes, these connections will pile up in the socket reader by design.
		      	            
		      if (channelId>=0) {		                    
		    	  Selector sel = coordinator.getSelector();
		          if (sel!=null) {      		          
			          try {         
			        	 
			        	  SocketChannel channel = serverSocketChannel.accept();
			        			        	  
			        	  //by design we set this in both places
			        	  channel.socket().setReceiveBufferSize(1<<19);
			              channel.configureBlocking(false);
			    			            
			              //TCP_NODELAY is required for HTTP/2 get used to it being on now.
			              channel.setOption(StandardSocketOptions.TCP_NODELAY, Boolean.TRUE); //NOTE: may need to turn off for high volume..  
			              channel.socket().setPerformancePreferences(0,1,2);//(1, 0, 2);
			           			              
			              SSLEngine sslEngine = null;
			              if (coordinator.isTLS) {
							  sslEngine = coordinator.engineFactory.createSSLEngine();//// not needed for server? host, port);
							  sslEngine.setUseClientMode(false); //here just to be complete and clear
							  // sslEngine.setNeedClientAuth(true); //only if the auth is required to have a connection
							  // sslEngine.setWantClientAuth(true); //the auth is optional
							  sslEngine.setNeedClientAuth(coordinator.requireClientAuth); //required for openSSL/boringSSL
							 
							  sslEngine.beginHandshake();
							  currentHandshake = sslEngine;
			              }
						  							  
						  
			              ServerConnection old = holder.setValue(channelId, 
			            		  		  new ServerConnection(sslEngine, 
			            		  				  		       channel, channelId,
			            				                       coordinator)
			            		  		  );
			              if (null!=old) {
			            	//  logger.info("\nclosing an old connection");
								old.close();
								old.decompose();
						  }
	  
			        //  logger.info("\naccepting new connection {} registered data selector {}", channelId, sel); 
	
			              channel.register(sel,SelectionKey.OP_READ, 
								           ServerCoordinator.selectorKeyContext(coordinator, channelId));
							
						//  logger.info("\nnew server connection attached for new id {} ",channelId);
						  if (null!=newClientConnections) {								  
			                  int targetPipeIdx = 0;
							  publishNotificationOFNewConnection(targetPipeIdx, channelId);
						  }
						  
			              
			          } catch (IOException e) {
			        	  logger.error("\nUnable to accept connection",e);
			          } 
			      } else {
			    	  doWork = false;
			      }
		          
		      } else {
		    	  
		    	  // TODO: find old connections and recycle them if they are no longer used..
		    	  // channelId holds the negative least used channel in case we want to close it
		    	  // any connection object with a pool reservation of -1 is also a good candidate...
		    	  //use this method to check
		    	//  coordinator.connectionForSessionId(channelId).getPoolReservation()
		    	  
		    	  logger.error("\n****max connections achieved, no more connections.****");
		    	  doWork = false;
		      }

		  } else {
		      assert(0 != (SelectionKey.OP_CONNECT & readyOps)) : "only expected connect";
		      try {
				((SocketChannel)key.channel()).finishConnect();

			} catch (IOException e) {
				logger.error("Unable to finish connect",e);
			} 
		  }
	}


//	private void growSendBuf(SocketChannel socketChannel) {
//		try {
//			final int baseSize =socketChannel.getOption(StandardSocketOptions.SO_SNDBUF);
//			//try to set a larger send buffer size...
//			
//			System.out.println("base: "+baseSize);
//			
//		    int lastKnown = baseSize;
//		    do {
//		    	socketChannel.setOption(StandardSocketOptions.SO_SNDBUF, 1<<21);//lastKnown+baseSize);
//		    	
//		    	int newValue = socketChannel.getOption(StandardSocketOptions.SO_SNDBUF);
//		    	System.out.println("updated to: "+newValue);
//		    	
//		    	if (newValue!=(lastKnown+baseSize)) {
//		    		break;
//		    	} else {
//		    		lastKnown = newValue;
//		    	}
//		    
//		    	
//		    	
//		    } while (true);
//			
//			
//			
//			
//			
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		
//	}
	
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
