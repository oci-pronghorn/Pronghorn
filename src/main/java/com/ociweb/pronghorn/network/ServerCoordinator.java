package com.ociweb.pronghorn.network;

import java.nio.channels.Selector;
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
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.PipeConfigManager;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.PronghornStageProcessor;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.MemberHolder;
import com.ociweb.pronghorn.util.PoolIdx;
import com.ociweb.pronghorn.util.PoolIdxPredicate;
import com.ociweb.pronghorn.util.ServiceObjectHolder;
import com.ociweb.pronghorn.util.ServiceObjectValidator;

public class ServerCoordinator extends SSLConnectionHolder {
	
	private final static Logger logger = LoggerFactory.getLogger(ServerCoordinator.class);
    
	private final ServiceObjectHolder<ServerConnection> socketHolder;
    private Selector                              selectors;
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

	private final PoolIdx responsePipeLinePool;
	private final int[] processorLookup;
	private final int moduleParallelism;
	private final int concurrentPerModules;
	
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
    	this.responsePipeLinePool = new PoolIdx(maxConcurrentInputs, moduleParallelism); 
    	
    	this.processorLookup = Pipe.splitGroups(moduleParallelism, maxConcurrentInputs);
        this.concurrentPerModules = maxConcurrentInputs/moduleParallelism;
    	//  0 0 0 0 1 1 1 1 
    	// 	logger.info("processorLookup to bind connections to tracks {}",Arrays.toString(processorLookup));
    	
        this.socketHolder = new ServiceObjectHolder<ServerConnection>(
        		channelBits, 
        		ServerConnection.class, 
        		new SocketValidator(), false/*Do not grow*/);

        serverPipesConfig.pcm.addConfig(NetGraphBuilder.buildRoutertoModulePipeConfig(this, serverPipesConfig));
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
   
    	
    //	logger.trace("Server pipe pool:\n {}",responsePipeLinePool);
    	    	
    }
    
    public boolean isLogFilesEnabled() {
    	return null!=logFile;
    }
    
    public LogFileConfig getLogFilesConfig() {
    	return logFile;
    }
    
	public void setStart(PronghornStage startStage) {
		this.firstStage = startStage;
	}
	
	
	public void debugResponsePipeLine() {
		logger.info(responsePipeLinePool.toString());
	}
	
	private PipeLineFilter isOk = new PipeLineFilter();

	public long[] routeSLALimits = new long[0];


	
	//NOT thread safe only called by ServerSocketReaderStage
	public int responsePipeLineIdx(final long ccId) {
	
		isOk.setId(ccId); //object resuse prevents CG here
		return responsePipeLinePool.get(ccId, isOk);

	}
	
	public int checkForResponsePipeLineIdx(long ccId) {
		return PoolIdx.getIfReserved(responsePipeLinePool,ccId);
	}	
	
	public void releaseResponsePipeLineIdx(long ccId) {		
		responsePipeLinePool.release(ccId);	
		//logger.info("after release we have {} locks",responsePipeLinePool.locks());
	}
	
	public int resposePoolSize() {
		return responsePipeLinePool.length();
	}
    
	public void setFirstUsage(Runnable run) {
		responsePipeLinePool.setFirstUsageCallback(run);
	}
	
	public void setLastUsage(Runnable run) {
		responsePipeLinePool.setNoLocksCallback(run);
	}

	@Override
	public <B extends BaseConnection> B connectionForSessionId(long id) {
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
    
    private final class PipeLineFilter implements PoolIdxPredicate {
		
    	private int idx;
		private int validValue;

		private PipeLineFilter() {
			
		}

		public void setId(long ccId) {
			assert(maxConcurrentInputs == processorLookup.length);

			//each connection should be in the next modules group of input pipes.
			this.idx = ((int)ccId*concurrentPerModules)%maxConcurrentInputs;			
			this.validValue = processorLookup[idx];
			
			///logger.info("PipeLineFilter set ccId {} idx {} validValue {}", ccId, idx, validValue);
			
		}

		@Override
		public boolean isOk(final int i) {
			return validValue == processorLookup[i]; 
		}
	}
    
    public int moduleParallelism() {
    	return moduleParallelism;
    }

	public static class SocketValidator implements ServiceObjectValidator<ServerConnection> {

        @Override
        public boolean isValid(ServerConnection serviceObject) { 
        	
            return serviceObject.getSocketChannel().isConnectionPending() || 
                   serviceObject.getSocketChannel().isConnected();
        }

        @Override
        public void dispose(ServerConnection t) {

        		if (t.getSocketChannel().isOpen()) {
                    t.close();
                }
        }
        
    }

//	public static AtomicInteger inServerCount = new AtomicInteger(0);
//    public static long start;
    
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
	
	
    
    public static Selector getSelector(ServerCoordinator that) {
        return that.selectors;
    }
    
    public static ConnectionContext selectorKeyContext(ServerCoordinator that, long channelId) {
        ConnectionContext context = that.connectionContext[(int)(that.channelBitsMask & channelId)];
        context.setChannelId(channelId);
        context.skPosition(-1);//clear in case this is reused.
        //logger.info("\nnew selector conext for connection {}",channelId);
        return context;
    }


    public void registerSelector(Selector selector) {
    	assert(null==selectors) : "Should not already have a value";
        selectors = selector;
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
