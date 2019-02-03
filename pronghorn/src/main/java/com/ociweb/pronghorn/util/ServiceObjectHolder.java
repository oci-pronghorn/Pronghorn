package com.ociweb.pronghorn.util;

import java.lang.reflect.Array;
import java.util.Arrays;

import com.ociweb.pronghorn.network.ServerConnection;

/**
 * Small footprint manager/holder of service objects. 
 * 
 * When a service object is added a long index value is returned so the caller can 
 * request this object again later.  Objects are validated by the ServiceObjectValidator so they 
 * can be expired.  Once expired they will no longer be returned.  This way if an async part of the
 * code still holds an old index it will not get the old object or any new object.
 * 
 * @author Nathan Tippy
 * 
 * Sample usage:
 * 
 *        ServiceObjectValidator validator = new ServiceObjectValidator() {
 *              public Channel isValid(Channel serviceObject) {
 *                   return serviceObject.isOpen() ? serviceObject : null;
 *               }
 *        };
 *             
 *        ServiceObjectHolder channelHolder = new ServiceObjectHolder(initialBits, Channel.class, validator);
 * 
 *        //save channel so it can be looked up later
 *        long channelIndex = channelHolder.add(channel);
 *       
 *        //look up a channel from the channelIndex
 *        Channel myChannel = channelHolder.get(channelIndex);
 * 
 */


public class ServiceObjectHolder<T> {

   
    static class ServiceObjectData<T> {
        public int size;
        public int mask;
        public final long[] serviceObjectKeys;
        public final T[] serviceObjectValues;
        public final long[] serviceObjectLookupCounts; //Used for sequence counts or usage counts
        public final long[] addTimeMS;
   
        public final Class<T> clazz; 

        @SuppressWarnings("unchecked")
        public ServiceObjectData(int initialBits, Class<T> clazz) {
            this.clazz = clazz;
            this.size = 1 << initialBits;
            this.mask = size-1;
            this.serviceObjectKeys = new long[size];
            
//            if (size > (1<<13)) {
//            	logger.trace(this.getClass().getSimpleName()+" is allocating long arrays of "+size);
//            }
            
            this.serviceObjectValues = (T[]) Array.newInstance(clazz, size);
            this.serviceObjectLookupCounts = new long[size];
            this.addTimeMS = new long[size];
         }

        @SuppressWarnings("unchecked")
        public ServiceObjectData(ServiceObjectData<T> data, int growthFactor) {
            
            this.clazz = data.clazz;
            this.size = data.size*growthFactor;
            this.mask = this.size-1;
            this.serviceObjectKeys = new long[size];
            this.serviceObjectValues = (T[]) Array.newInstance(clazz, size);
            this.serviceObjectLookupCounts = new long[size];
            this.addTimeMS = new long[size];
            
            int index = data.size;
            int mask = this.mask;
            while (--index >= 0) {
                int modIdx = mask & index;                        
                T value = data.serviceObjectValues[modIdx];
                if (null != value) { 
                    serviceObjectValues[modIdx] = value;
                    serviceObjectKeys[modIdx] = data.serviceObjectKeys[modIdx];
                }
            }            
        }
    }
    private static final int DEFAULT_BITS = 5;

    private ServiceObjectData<T> data;
    private final ServiceObjectValidator<T> validator;
    
    private long sequenceCounter;//goes up on every add
    
    //for support of forever loop around the valid Objects
    private int loopPos = -1;
    
    private final boolean shouldGrow;
    
    public String toString() {
    	return Arrays.toString(data.serviceObjectKeys)+" "+Arrays.toString(data.serviceObjectValues);
    	
    }
    
    /**
     * Do not use this constructor unless you want to start out with internal arrays more near the desired size.
     */
    public ServiceObjectHolder(int initialBits, Class<T> clazz, ServiceObjectValidator<T> validator, boolean shouldGrow) {
        this.validator = validator;
        this.data = new ServiceObjectData<T>(initialBits, clazz);
        this.shouldGrow = shouldGrow;
        
        //new Exception("new").printStackTrace();
    }    
    
    public ServiceObjectHolder(Class<T> clazz, ServiceObjectValidator<T> validator, boolean shouldGrow) {
        this.validator = validator;
        this.data = new ServiceObjectData<T>(DEFAULT_BITS, clazz);
        this.shouldGrow = shouldGrow;
        
        //new Exception("new").printStackTrace();
    }  
    
    
    public void disposeAll() {
    	int j = data.serviceObjectValues.length;
    	while (--j>=0) {
    		if (null != data.serviceObjectValues[j]) {
    			
    			validator.dispose(data.serviceObjectValues[j]);
    			
    		}
    		
    	}
    	
    	
    }
    
    
    /**
     * Add new service object and return the index to retrieve the object later.
     * 
     * This is not thread safe and must not be called concurrently.
     * 
     * @param serviceObject
     */
    public long add(T serviceObject) {
        //Not thread safe, must be called by one thread or sequentially    
        long index = -1;
        int modIdx =-1;
        
        long localSequenceCount = sequenceCounter;
        long hardStop = localSequenceCount + data.size;
        
        long maxPeriod = 0;
        long maxPeriodIdx = -1;
        long now = System.currentTimeMillis();
        do {
            //if we end up passing over all the members find which is the least used.
            if (-1 != index) {
                long lookupCounts = data.serviceObjectLookupCounts[modIdx];                
                long liveTime = now - data.addTimeMS[modIdx];
                long callPeriod = liveTime/(lookupCounts+1);
                //we want to pick the one with the largest call period
                //and with a live time > 1
                //this gives us the one least frequently used over time
                
            
                	if (liveTime > maxPeriod) {
                		maxPeriod = liveTime;
                		maxPeriodIdx = index;
                	}                	
              
            }
            
            index = ++localSequenceCount;
            modIdx = data.mask & (int)index;
                    
            if (index==hardStop) {
               
                //three choices - build 3 different add methods
                // + grow - not good for production and should be avoided     addGrow
                // + replace least frequently used                            addReplace
                // + return -1 to signal can not insert                       addTry
                
                if (shouldGrow) {
                   //copy and grow the data space, done locally then it is all replaced at once to not break concurrent callers
                    data = new ServiceObjectData<T>(data, 2);   
                } else {
                    //do not grow instead replace the least used member                    
                    index = maxPeriodIdx;
                    modIdx = data.mask & (int)maxPeriodIdx;
                    validator.dispose(data.serviceObjectValues[modIdx]);
                    break;
                }
            }
            
            //keep going if we have looped around and hit a bucket which is already occupied with something valid.
        } while (keepLooking(data.serviceObjectValues[modIdx]));
        
        
        data.addTimeMS[modIdx] = System.currentTimeMillis();
        data.serviceObjectKeys[modIdx] = index;
        data.serviceObjectValues[modIdx] = serviceObject;
        
        sequenceCounter = Math.max(sequenceCounter,index);
        
        //Never resets the usage count, that field is use case specific and should not always be cleared.
        return index;
    }

    
    /**
     * 
     * @return negative value of least used or the new value of the new service ID?
     */
    public long lookupInsertPosition() {
        //Not thread safe, must be called by one thread or sequentially    
        long index = -1;
        int modIdx =-1;
         
        long localSequenceCount = sequenceCounter;
        long hardStop = localSequenceCount + data.size;
        
        //System.err.println("scanning from "+localSequenceCount+" "+hardStop);
        
        
        long maxPeriod = 0;
        long maxPeriodIdx = -1;

       // System.err.println("service holder mask "+data.mask+" from "+localSequenceCount+" to "+hardStop);
        
        final long now = System.currentTimeMillis();
        top:
        do {

            //if we end up passing over all the members find which is the least used.
            if (-1 != index) {
                long lookupCounts = data.serviceObjectLookupCounts[modIdx];                
                long liveTime = now-data.addTimeMS[modIdx];
                long callPeriod = liveTime/(lookupCounts+1);
                //we want to pick the one with the largest call period
                //and with a live time > 1
                //this gives us the one least frequently used over time
                                
            	if (liveTime >= maxPeriod) {
            		maxPeriod = liveTime;
            		maxPeriodIdx = index;
            	}
            }
            
            index = ++localSequenceCount;
            modIdx = data.mask & (int)index;
          //  System.err.println("checking "+modIdx+"  "+index+"  "+data.serviceObjectValues[modIdx]);
        
            if (data.clazz != ServerConnection.class) {
            if (index==hardStop && keepLooking(data.serviceObjectValues[modIdx])) {
            	assert(-1!=maxPeriodIdx);            	
            	//do not grow instead return the negative value of the least used object
            	//if (data.clazz == ServerConnection.class) {
            	//	System.out.println("found connection to kill "+maxPeriodIdx);
            	//}
            	return -maxPeriodIdx;
            }            
            }
            
//            if (data.clazz == ServerConnection.class) {
//            	ServerConnection sc = (ServerConnection)data.serviceObjectValues[modIdx];
//            	if (null != sc) {
//            		if (sc.getSequenceNo() > 1) {
//            			
//            			System.out.println(modIdx+" should be skipped "+index+" with sequence "+sc.getSequenceNo()+" con "+sc.getId()+" data mask "+data.mask+" data size "+data.size);
//            			
//            			continue top;//skip over this one            			
//            		} else {
//            			//if seq is 1 then we may not be shipping data yet... we have gone around and nothing happened.
//            			System.out.println(modIdx+" should be NOT? skipped "+index+" with sequence "+sc.getSequenceNo()+" con "+sc.getId()+" data mask "+data.mask+" data size "+data.size);
//            			sc.decompose();
//            			//TODO:we keep looking for a new connnection but the 1 of 4 which was closed is still here!!
//            			//     how do we close that connection and remove it from the pool?
//            			break;
//            		}
//            	}            	
//            }
            //keep going if we have looped around and hit a bucket which is already occupied with something valid.
        } while (keepLooking(data.serviceObjectValues[modIdx]));

        sequenceCounter = (data.mask &localSequenceCount);
        //Never resets the usage count, that field is use case specific and should not always be cleared.
        
//        if (data.clazz == ServerConnection.class) {
//        	ServerConnection sc = (ServerConnection)data.serviceObjectValues[modIdx];
//        	
//        	System.out.println("lookupInsertPosition returned:"+index+" out of "+(data.size)+"  seq:"+((null==sc)?"null":sc.getSequenceNo()));
//        	
//        }
        
        //this value always goes up and can be mod to find the exact position of an object
        return index;
    }

	private final boolean keepLooking(T obj) {
		return (null!=obj) && validator.isValid(obj);
	}
    
    /*
     * Only used with the above lookupInsertPosition for setting the object, this allows for the late construction when the object is needed.
     * This can also be used to replace this value if it is still valid and not expired.
     * 
     */
    public T setValue(long index, T object) {
    	int modIdx = data.mask & (int)index;
    	data.addTimeMS[modIdx] = System.currentTimeMillis();
    	data.serviceObjectKeys[modIdx] = index;//must be stored to make this a valid object
    	T old = data.serviceObjectValues[modIdx];
    	data.serviceObjectValues[modIdx] = (T)object;    	
    	sequenceCounter = Math.max(sequenceCounter,index);
    	return old;
    }
    
    
    /**
     * Given the index value return the valid value object or null.
     * 
     * Side effect, if the value is invalid it is set to null to release the resources sooner.
     * 
     * @param index
     */
    public T getValid(final long index) {  
        //must ensure we use the same instance for the work
        ServiceObjectData<T> localData = data;
        
        int modIdx = localData.mask & (int)index;
        if (index == localData.serviceObjectKeys[modIdx] && 
        	validator.isValid(localData.serviceObjectValues[modIdx])) {          
            return localData.serviceObjectValues[modIdx];
        }
        return null;        
    }
    
    public void visitValid(ServerObjectHolderVisitor<T> v) {
    	ServiceObjectData<T> localData = data;
    	int i = localData.serviceObjectValues.length;
    	while (--i>=0) {
    		final T t = localData.serviceObjectValues[i];
    		if ((null!=t) && validator.isValid(t)) {
    			v.visit(i, t);
    		}
    	}
    }
    
    public void visitAll(ServerObjectHolderVisitor<T> v) {
    	ServiceObjectData<T> localData = data;
    	int i = localData.serviceObjectValues.length;
    	while (--i>=0) {
    		final T t = localData.serviceObjectValues[i];
    		if (null!=t) {
    			v.visit(i, t);
    		}
    	}
    }
    
    public void resetUsageCount(final long index) {
        data.serviceObjectLookupCounts[data.mask & (int)index] = 0;
    }
    
    public long incUsageCount(final long index) {
        return data.serviceObjectLookupCounts[data.mask & (int)index]++;
    }

    /**
     * Given the index value return the value object or null.
     * 
     * @param key
     */
    public T get(final long key) {
        //must ensure we use the same instance for the work
        ServiceObjectData<T> localData = data;
        int modIdx = localData.mask & (int)key;
        return key == localData.serviceObjectKeys[modIdx] ? localData.serviceObjectValues[modIdx] : null;

    }
    
    public T remove(final long key) {  
        //must ensure we use the same instance for the work
        ServiceObjectData<T> localData = data;
        
        int modIdx = localData.mask & (int)key;
        
        if (key == localData.serviceObjectKeys[modIdx]) {
        	T result = localData.serviceObjectValues[modIdx];
        	localData.serviceObjectKeys[modIdx] = 0;
        	localData.serviceObjectValues[modIdx] = null; //wipe out old value
        	//System.err.println("now cleared "+key+" at buckedt "+modIdx);
        	return result;
        } else {
        	return null;
        }
    }
    
    public int size() {
    	return data.size;
    }
    
    public T getByPosition(int position) {    	
    	ServiceObjectData<T> localData = data;
        T result = localData.serviceObjectValues[position];
        if (null!=result && validator.isValid(result)) {
        	return result;
        } else {
        	return null;
        }
    }

    public static long getSequenceCount(ServiceObjectHolder sho) {
        return sho.sequenceCounter; 
    }

    
}
