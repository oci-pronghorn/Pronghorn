package com.ociweb.pronghorn.network;

import java.net.InetSocketAddress;
import java.nio.channels.Selector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.Pipe;
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
    
	private ServiceObjectHolder<ServerConnection> socketHolder;
    private Selector                              selectors;
    private MemberHolder                          subscriptions;
    private int[]                                 upgradePipeLookup;
    private ConnectionContext[]                   connectionContext; //NOTE: ObjectArrays would work very well here!!
            
    
    public final int                                  channelBits;
    public final int                                  channelBitsSize;
    public final int                                  channelBitsMask;

    private final int                                  port;
    private final InetSocketAddress                    address;

    public final static int INCOMPLETE_RESPONSE_SHIFT    = 28;
    public final static int END_RESPONSE_SHIFT           = 29;//for multi message send this high bit marks the end
    public final static int CLOSE_CONNECTION_SHIFT       = 30;
    public final static int UPGRADE_CONNECTION_SHIFT     = 31;
    public final static int UPGRADE_TARGET_PIPE_MASK     = (1<<21)-1; 

    public final static int INCOMPLETE_RESPONSE_MASK     = 1<<INCOMPLETE_RESPONSE_SHIFT;
	public final static int END_RESPONSE_MASK            = 1<<END_RESPONSE_SHIFT;
	public final static int CLOSE_CONNECTION_MASK        = 1<<CLOSE_CONNECTION_SHIFT;
	public final static int UPGRADE_MASK                 = 1<<UPGRADE_CONNECTION_SHIFT;

	private final PoolIdx responsePipeLinePool;
	public final int maxPartialResponses;
	private final int[] processorLookup;
	public final boolean isTLS;
	private final int processorsCount;
	
	public static boolean TEST_RECORDS = false;
	static {
		
	}
	
	//this is only used for building stages and adding notas
	private PronghornStageProcessor optionalStageProcessor = null;
	
	public ServerCoordinator(boolean isTLS, String bindHost, int port, ServerPipesConfig serverConfig) {
		this(isTLS,bindHost,port, serverConfig.maxConnectionBitsOnServer, serverConfig.maxPartialResponsesServer, serverConfig.processorCount );
	}
	
    public ServerCoordinator(boolean isTLS, String bindHost, int port, int maxConnectionsBits, int maxPartialResponses, int processorsCount) {
        
    	this.isTLS 			   = isTLS;
        this.port              = port;
        this.channelBits       = maxConnectionsBits;
        this.channelBitsSize   = 1<<channelBits;
        this.channelBitsMask   = channelBitsSize-1;
        this.address           = new InetSocketAddress(bindHost,port);
        
    	this.responsePipeLinePool = new PoolIdx(maxPartialResponses); 
    	
    	this.maxPartialResponses = maxPartialResponses;

    	this.processorsCount = processorsCount;
    	this.processorLookup = Pipe.splitGroups(processorsCount, maxPartialResponses);
    	
    	
    	SSLEngineFactory.init();
    	
        
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

	public void setStart(PronghornStage startStage) {
		this.firstStage = startStage;
	}
	
	
	public void debugResponsePipeLine() {
		logger.info(responsePipeLinePool.toString());
	}
	
	private PipeLineFilter isOk = new PipeLineFilter();
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
	public SSLConnection get(long id) {
		return socketHolder.get(id);		
	}
    
    public InetSocketAddress getAddress() {
        return address;
    }

    
    public static ServiceObjectHolder<ServerConnection> newSocketChannelHolder(ServerCoordinator that) {
        that.connectionContext = new ConnectionContext[that.channelBitsSize];
        //must also create these long lived instances, this would be a good use case for StructuredArray and ObjectLayout
        int i = that.channelBitsSize;
        while (--i >= 0) {
            that.connectionContext[i] = new ConnectionContext();
        }
        
        that.upgradePipeLookup = new int[that.channelBitsSize];
        return that.socketHolder = new ServiceObjectHolder<ServerConnection>(that.channelBits, ServerConnection.class, new SocketValidator(), false/*Do not grow*/);
        
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
			assert(maxPartialResponses == processorLookup.length);
			this.idx = ((int)ccId)%maxPartialResponses;
			this.validValue = processorLookup[idx];
		}

		@Override
		public boolean isOk(final int i) {
			return validValue == processorLookup[i]; 
		}
	}
    
    public int processorCount() {
    	return processorsCount;
    }

	public static class SocketValidator implements ServiceObjectValidator<ServerConnection> {

        @Override
        public boolean isValid(ServerConnection serviceObject) {            
            return serviceObject.socketChannel.isConnectionPending() || 
                   serviceObject.socketChannel.isConnected();
        }

        @Override
        public void dispose(ServerConnection t) {
                if (t.socketChannel.isOpen()) {
                    t.close();
                }
        }
        
    }


    
    public static int[] getUpgradedPipeLookupArray(ServerCoordinator that) {
        return that.upgradePipeLookup;
    }
    
    public static Selector getSelector(ServerCoordinator that) {
        return that.selectors;
    }
    
    public static ConnectionContext selectorKeyContext(ServerCoordinator that, long channelId) {
        ConnectionContext context = that.connectionContext[(int)(that.channelBitsMask & channelId)];
        context.setChannelId(channelId);        
        return context;
    }

    static boolean scanForOptimalPipe(ServerCoordinator that) {
             
        	ServiceObjectHolder<ServerConnection> holder = that.socketHolder;    
             if (null!=holder) {
	             return ((int)(ServiceObjectHolder.getSequenceCount(holder) - ServiceObjectHolder.getRemovalCount(holder))) < holder.size(); 
             }
     
          return false;
    }

    public void registerSelector(Selector selector) {
    	assert(null==selectors) : "Should not already have a value";
        selectors = selector;
    }


}
