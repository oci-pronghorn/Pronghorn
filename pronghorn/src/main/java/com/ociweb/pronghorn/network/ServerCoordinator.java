package com.ociweb.pronghorn.network;

import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.config.HTTPContentTypeDefaults;
import com.ociweb.pronghorn.network.config.HTTPHeaderDefaults;
import com.ociweb.pronghorn.network.config.HTTPRevisionDefaults;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.config.HTTPVerbDefaults;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.PipeConfigManager;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.PronghornStageProcessor;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.ArrayGrow;
import com.ociweb.pronghorn.util.MemberHolder;
import com.ociweb.pronghorn.util.ServiceObjectHolder;
import com.ociweb.pronghorn.util.ServiceObjectValidator;

public class ServerCoordinator extends SSLConnectionHolder {
	
	private final static Logger logger = LoggerFactory.getLogger(ServerCoordinator.class);
    
	private ServiceObjectHolder<ServerConnection> socketHolder;
    
	private Selector[]                            selectors;
	private int                                   selectedSelector = -1; //init so we hit zero first
    
    
    private MemberHolder                          subscriptions;
    private int[]                                 upgradePipeLookup;
    private ConnectionContext[]                   connectionContext; //NOTE: ObjectArrays would work very well here!!

    public final int                                  channelBits;
    public final int                                  channelBitsSize;
    public final int                                  channelBitsMask;

    private final int                                  port;
    private final String                               bindHost;

    public final static int BEGIN_RESPONSE_SHIFT         = 27;
    public final static int INCOMPLETE_RESPONSE_SHIFT    = 28;
    public final static int END_RESPONSE_SHIFT           = 29;//for multi message send this high bit marks the end
    public final static int CLOSE_CONNECTION_SHIFT       = 30;
    public final static int UPGRADE_CONNECTION_SHIFT     = 31;
    
    public final static int UPGRADE_TARGET_PIPE_MASK     = (1<<21)-1; 

    public final static int BEGIN_RESPONSE_MASK          = 1<<BEGIN_RESPONSE_SHIFT;	
    public final static int INCOMPLETE_RESPONSE_MASK     = 1<<INCOMPLETE_RESPONSE_SHIFT;
	public final static int END_RESPONSE_MASK            = 1<<END_RESPONSE_SHIFT;
	public final static int CLOSE_CONNECTION_MASK        = 1<<CLOSE_CONNECTION_SHIFT;
	public final static int UPGRADE_MASK                 = 1<<UPGRADE_CONNECTION_SHIFT;

	public final int moduleParallelism;

	
	public final int maxConcurrentInputs;
	public final int maxConcurrentOutputs;
		
    private final String serviceName;
    private final String defaultPath;
    
    private final LogFileConfig logFile; //generate a set of files to rotate..
   
	public final boolean requireClientAuth;//clients must send their cert to connect
	private final ServerConnectionStruct scs; 

	//Need to inject this?
	public final HTTPSpecification<HTTPContentTypeDefaults,HTTPRevisionDefaults,HTTPVerbDefaults,HTTPHeaderDefaults> spec = HTTPSpecification.defaultSpec();
	
	///////////////////////////////////////
		
//	public static long acceptConnectionStart;
//	public static long acceptConnectionRespond;
//	
//	public static long orderSuperStart;
//	public static long newDotRequestStart;
	
	//this is only used for building stages and adding notas
	private PronghornStageProcessor optionalStageProcessor = null;

	public final PipeConfigManager pcm;

	public final PipeConfig<NetPayloadSchema> incomingDataConfig;
	public final int serverRequestUnwrapUnits;
	public final int serverPipesPerOutputEngine;
	public final int serverResponseWrapUnitsAndOutputs;
	public final int serverSocketWriters;
	
    public ServerCoordinator(TLSCertificates tlsCertificates,
    		                 String bindHost, int port,
    		                 ServerConnectionStruct scs,    		                
							 boolean requireClientAuth,
							 String serviceName, 
							 String defaultPath, 
							 ServerPipesConfig serverPipesConfig){

		super(tlsCertificates);
		
		this.channelBits = serverPipesConfig.maxConnectionBitsOnServer;
		this.maxConcurrentInputs = serverPipesConfig.maxConcurrentInputs;
		this.maxConcurrentOutputs = serverPipesConfig.maxConcurrentOutputs;
		this.moduleParallelism = serverPipesConfig.moduleParallelism;
		this.logFile = serverPipesConfig.logFile;
		 
		this.incomingDataConfig       = serverPipesConfig.incomingDataConfig;
		this.serverRequestUnwrapUnits = serverPipesConfig.serverRequestUnwrapUnits;
		this.serverPipesPerOutputEngine = serverPipesConfig.serverPipesPerOutputEngine;
		this.serverResponseWrapUnitsAndOutputs = serverPipesConfig.serverResponseWrapUnitsAndOutputs;
		this.serverSocketWriters = serverPipesConfig.serverSocketWriters;
		
		this.requireClientAuth = requireClientAuth;
		this.scs = scs;
        this.port              = port;
        this.channelBitsSize   = 1<<channelBits;
        this.channelBitsMask   = channelBitsSize-1;
        this.bindHost          = bindHost;
          
        this.serviceName       = serviceName;
        this.defaultPath       = defaultPath.startsWith("/") ? defaultPath.substring(1) : defaultPath;
        
 
   
    	//  0 0 0 0 1 1 1 1 
    	// 	logger.info("processorLookup to bind connections to tracks {}",Arrays.toString(processorLookup));
    	
        this.socketHolder = new ServiceObjectHolder<ServerConnection>(
        		channelBits, 
        		ServerConnection.class, 
        		new SocketValidator(), false/*Do not grow*/);

        serverPipesConfig.pcm.ensureSize(HTTPRequestSchema.class, 
        		Math.min(serverPipesConfig.fromRouterToModuleCount, 1<<12), //4K max queue length 
        		this.connectionStruct().inFlightPayloadSize());
        
        serverPipesConfig.pcm.addConfig(serverPipesConfig.orderWrapConfig()); 

		pcm = serverPipesConfig.pcm;
        
    }
    
    public void setStageNotaProcessor(PronghornStageProcessor p) {
    	optionalStageProcessor = p;
    }
    public void processNota(GraphManager gm, PronghornStage stage) {
    	if (null != optionalStageProcessor) {
    		optionalStageProcessor.process(gm, stage);
    	}
    }
            
    private PronghornStage firstStage;
    
    public void shutdown() {
    	
    	if (null!=firstStage) {
    		firstStage.requestShutdown();
    		firstStage = null;
    	}
    	socketHolder = null;
    	optionalStageProcessor = null;
    	selectors = null;
    }
    
    public boolean isLogFilesEnabled() {
    	return null!=logFile;
    }
    
    public LogFileConfig getLogFilesConfig() {
    	return logFile;
    }
    
	@Override
	public int maxConnections() {
		return socketHolder.size();		
	}
    
	public void setStart(PronghornStage startStage) {
		this.firstStage = startStage;
	}
	
	public long[] routeSLALimits = new long[0];
	

	@Override
	public <B extends BaseConnection> B lookupConnectionById(long id) {
		return (B)socketHolder.get(id);		
	}

    public int port() {
    	return port;
    }
    
    public String host() {
    	return bindHost;
    }
    
    
    public static ServiceObjectHolder<ServerConnection> newSocketChannelHolder(ServerCoordinator that) {
        that.connectionContext = new ConnectionContext[that.channelBitsSize];
        //must also create these long lived instances, this would be a good use case for StructuredArray and ObjectLayout
        int i = that.channelBitsSize;
        while (--i >= 0) {
            that.connectionContext[i] = new ConnectionContext();
        }
        
        that.upgradePipeLookup = new int[that.channelBitsSize];
        Arrays.fill(that.upgradePipeLookup, -1);//if not upgraded it remains -1
        
        return that.socketHolder;
        		
    }
    
    public static ServiceObjectHolder<ServerConnection> getSocketChannelHolder(ServerCoordinator that) {
        return that.socketHolder;
    }
    
    public MemberHolder newMemberHolder() {
        return subscriptions = new MemberHolder(100);
    }
    

    
    public int moduleParallelism() {
    	return moduleParallelism;
    }

	public static class SocketValidator implements ServiceObjectValidator<ServerConnection> {

        @Override
        public boolean isValid(ServerConnection serviceObject) { 

        	SocketChannel socketChannel = serviceObject.getSocketChannel();
        	if (null!=socketChannel) {
        		//TODO: if disconnecting return false??
				if (socketChannel.isConnected()) {
	        		if (serviceObject.getPoolReservation()>=0) {
	        			return true;
	        		} else {

	        			//this connection was attempted but could not be completed
	        			//now that it is discovered we must release any resources it was using
	        			//serviceObject.close();
	        			//serviceObject.decompose();        			
	        			return false;
	        		}
	        	}
	        	if (socketChannel.isConnectionPending()) {
	        		return true;
	        	}        	
        	}
        	return false;
        }

        @Override
        public void dispose(ServerConnection t) {

        		if (t.getSocketChannel().isOpen()) {
                    t.close();
                }
        }
        
    }

	@Deprecated
    public static int[] getUpgradedPipeLookupArray(ServerCoordinator that) {
        return that.upgradePipeLookup;
    }
    
    public static void setUpgradePipe(ServerCoordinator that, long channelId, int targetPipe) {
    	that.upgradePipeLookup[(int)(that.channelBitsMask & channelId)] = targetPipe;
    }
    
	public static boolean isWebSocketUpgraded(ServerCoordinator that, long channelId) {
		return that.upgradePipeLookup[(int)(that.channelBitsMask & channelId)]>=0;
	}
	public static int getWebSocketPipeIdx(ServerCoordinator that, long channelId) {
		return that.upgradePipeLookup[(int)(that.channelBitsMask & channelId)];
	}
	
	public static ConnectionContext selectorKeyContext(ServerCoordinator that, long channelId) {
		ConnectionContext context = that.connectionContext[(int)(that.channelBitsMask & channelId)];
		context.setChannelId(channelId);
		context.skPosition(-1);//clear in case this is reused.
		//logger.info("\nnew selector conext for connection {}",channelId);
		return context;
	}
	
    
    public Selector getSelector() {
    	//round robin selectors, we have no better way to balance the load
    	
    	if (null!=selectors) {
	
	    	if (++selectedSelector >= selectors.length) {
	    		selectedSelector = 0;
	    	}
	    	
	    	return selectors[selectedSelector];
    	} else {
    		return null;
    	}
    }

    public void registerSelector(Selector selector) {

    	//confirm it is not already listed
      	int i = (null==selectors) ? 0 : selectors.length;
    	while (--i>=0) {
    		if (selectors[i]==selector) {
    			return;//do not add, it is already there.
    		}
    	}    	
    	//grow the array
    	selectors = ArrayGrow.appendToArray(selectors, selector);
    	assert(null!=selectors);
    }

	public String serviceName() {
		return serviceName;
	}
	
	public String defaultPath() {
		return defaultPath;
	}

	public ServerConnectionStruct connectionStruct() {
		return scs;
	}


}
