package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.util.MemberHolder;
import com.ociweb.pronghorn.util.PoolIdx;
import com.ociweb.pronghorn.util.PoolIdxPredicate;
import com.ociweb.pronghorn.util.ServiceObjectHolder;
import com.ociweb.pronghorn.util.ServiceObjectValidator;

public class ServerCoordinator extends SSLConnectionHolder {

    //TODO: replace to hold ServerConnection
	private final ServiceObjectHolder<ServerConnection>[] socketHolder;
	
	private final static Logger logger = LoggerFactory.getLogger(ServerCoordinator.class);
    
    private final Selector[]                           selectors;
    private final MemberHolder[]                       subscriptions;
    private final int[][]                              upgradePipeLookup;
    private final ConnectionContext[][]                connectionContext; //NOTE: ObjectArrays would work very well here!!
            
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
	private final int maxPartialResponses;
	private final int routerCount;

	private final int[] routerLookup;
    
    public ServerCoordinator(int socketGroups, String bindHost, int port, int maxConnectionsBits, int maxPartialResponses, int routerCount) {
        this.socketHolder      = new ServiceObjectHolder[socketGroups];    
        this.selectors         = new Selector[socketGroups];
        
        
        this.subscriptions     = new MemberHolder[socketGroups];
        this.port              = port;
        this.upgradePipeLookup        = new int[socketGroups][];
        this.connectionContext = new ConnectionContext[socketGroups][];
        this.channelBits       = maxConnectionsBits;
        this.channelBitsSize   = 1<<channelBits;
        this.channelBitsMask   = channelBitsSize-1;
        this.address           = new InetSocketAddress(bindHost,port);
        
    	this.responsePipeLinePool = new PoolIdx(maxPartialResponses); 
    	
    	this.maxPartialResponses = maxPartialResponses;
    	this.routerCount = routerCount;
    	this.routerLookup = Pipe.splitGroups(routerCount, maxPartialResponses);
    	
    	
    	SSLEngineFactory.init();
    	
        
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
	
	public int responsePipeLineIdx(long ccId) {
		//SSLConnection cc = get(ccId, 0);
		
		
		final int expectedRoute =  ((int)ccId)%routerCount;	
		final int expected = ((int)ccId)%maxPartialResponses;

		PoolIdxPredicate isOk = new PoolIdxPredicate() {
			
			@Override
			public boolean isOk(final int i) {

				//System.err.println(Arrays.toString(routerLookup )+"  "+i+" expected route "+routerLookup[i]);
				
				//this approach is better however it is causing an error
				// looking for 322791 but found lower 319786 for connection 31
				//return expectedRoute == routerLookup[i]; //TODO: need unit test on splitter to ensure match first.
				
				return i == expected;
			}
		};
		
		//this forces the same connection to take the same path every time which enforces order HOWEVER.
		// we need only restrict it to the same Router but how to know that?
		
		int result =  responsePipeLinePool.get(ccId, isOk); //TODO: must get the connection to remember this router and stay here, unless we have writting ot this point....


		//TODO: these two values must match, teh result should not be zero.
		
//		int expectedR = 31 & (int)ccId;
//		
//		
//		if (expectedR != result) {
//			System.err.println(ccId+"  should match "+result+" "+expectedR);
//			System.err.println("FORCE EXIT ZERO DETECTED");
//			System.exit(-1);
//			
//		}
		
	//	assert(result==expected);
		
		
//		System.err.println("   "+result+"  "+expected+"  "+expectedRoute);
		
		return result;
	}
	
	public int checkForResponsePipeLineIdx(long ccId) {
		return responsePipeLinePool.getIfReserved(ccId);
	}	
	
	public void releaseResponsePipeLineIdx(long ccId) {		
		responsePipeLinePool.release(ccId);	
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
	public SSLConnection get(long id, int groupId) {
		return socketHolder[groupId].get(id);		
	}
    
    public InetSocketAddress getAddress() {
        return address;
    }

    
    public static ServiceObjectHolder<ServerConnection> newSocketChannelHolder(ServerCoordinator that, int idx) {
        that.connectionContext[idx] = new ConnectionContext[that.channelBitsSize];
        //must also create these long lived instances, this would be a good use case for StructuredArray and ObjectLayout
        int i = that.channelBitsSize;
        while (--i >= 0) {
            that.connectionContext[idx][i] = new ConnectionContext();
        }
        
        that.upgradePipeLookup[idx] = new int[that.channelBitsSize];
        return that.socketHolder[idx] = new ServiceObjectHolder<ServerConnection>(that.channelBits, ServerConnection.class, new SocketValidator(), false/*Do not grow*/);
        
    }
    
    public static ServiceObjectHolder<ServerConnection> getSocketChannelHolder(ServerCoordinator that, int idx) {
        return that.socketHolder[idx];
    }
    
    public MemberHolder newMemberHolder(int idx) {
        return subscriptions[idx] = new MemberHolder(100);
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


    
    public static int[] getUpgradedPipeLookupArray(ServerCoordinator that, int idx) {
        return that.upgradePipeLookup[idx];
    }
    
    @Deprecated//Not needed bad idea
    public static void setTargetUpgradePipeIdx(ServerCoordinator coordinator, int groupId, long channelId, int idxValue) {
        coordinator.upgradePipeLookup[groupId][(int)(coordinator.channelBitsMask & channelId)] = idxValue;
    }
    
    
    @Deprecated
    public static int getTargetUpgradePipeIdx(ServerCoordinator coordinator, int groupId, long channelId) {
        return coordinator.upgradePipeLookup[groupId][(int)(coordinator.channelBitsMask & channelId)];
    }
    
    public static Selector getSelector(ServerCoordinator that, int idx) {
        return that.selectors[idx];
    }
    
    public static ConnectionContext selectorKeyContext(ServerCoordinator that, int idx, long channelId) {
        ConnectionContext context = that.connectionContext[idx][(int)(that.channelBitsMask & channelId)];
        context.setChannelId(channelId);        
        return context;
    }

    static int scanForOptimalPipe(ServerCoordinator that, int minValue, int minIdx) {
        int i = that.socketHolder.length;
        ServiceObjectHolder<ServerConnection>[] localSocketHolder=that.socketHolder;
        while (--i>=0) {
             ServiceObjectHolder<ServerConnection> holder = localSocketHolder[i];    
             if (null!=holder) {
	             int openConnections = (int)(ServiceObjectHolder.getSequenceCount(holder) - ServiceObjectHolder.getRemovalCount(holder));
	             if (openConnections<minValue /*&& Pipe.hasRoomForWrite(localOutputs[i])*/ ) {
	                 minValue = openConnections;
	                 minIdx = i;
	             } 
             }
          }
          return minIdx;
    }

    public void registerSelector(int pipeIdx, Selector selector) {
    	assert(null==selectors[pipeIdx]) : "Should not already have a value";
        selectors[pipeIdx] = selector;
    }


}
