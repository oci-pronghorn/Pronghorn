package com.ociweb.pronghorn.util;

import java.lang.reflect.Array;
import java.util.Arrays;

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
 * 
 * Sample usage:
 * 
 *        ServiceObjectValidator<Channel> validator = new ServiceObjectValidator<Channel>() {
 *               @Override
 *              public Channel isValid(Channel serviceObject) {
 *                   return serviceObject.isOpen() ? serviceObject : null;
 *               }
 *        };
 *             
 *        ServiceObjectHolder<Channel> channelHolder = new ServiceObjectHolder<Channel>(initialBits, Channel.class, validator);
 * 
 *        //save channel so it can be looked up later
 *        long channelIndex = channelHolder.add(channel);
 *       
 *        //look up a channel from the channelIndex
 *        Channel myChannel = channelHolder.get(channelIndex);
 * 
 *
 * @param <T>
 */


public class ServiceObjectHolder<T> {

   
    static class ServiceObjectData<T> {
        public int size;
        public int mask;
        public long[] serviceObjectKeys;
        public T[] serviceObjectValues;
        public final Class<T> clazz; 

        @SuppressWarnings("unchecked")
        public ServiceObjectData(int initialBits, Class<T> clazz) {
            this.clazz = clazz;
            this.size = 1 << initialBits;
            this.mask = size-1;
            this.serviceObjectKeys = new long[size];
            this.serviceObjectValues = (T[]) Array.newInstance(clazz, size);
        }

        @SuppressWarnings("unchecked")
        public ServiceObjectData(ServiceObjectData<T> data, int growthFactor) {
            
            this.clazz = data.clazz;
            this.size = data.size*2;
            this.mask = this.size-1;
            this.serviceObjectKeys = new long[size];
            this.serviceObjectValues = (T[]) Array.newInstance(clazz, size);
            
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
    private long removalCounter;//goes down on every remove
    
    //for support of forever loop around the valid Objects
    private int loopPos = -1;
    
    /**
     * Do not use this constructor unless you want to start out with internal arrays more near the desired size.
     * 
     * @param initialBits, The initial size of the internal array as defined by 1<<initialBits 
     * @param clazz, The class to be held 
     * @param validator, Function to validate held values 
     */
    public ServiceObjectHolder(int initialBits, Class<T> clazz, ServiceObjectValidator<T> validator) {
        this.validator = validator;
        this.data = new ServiceObjectData<T>(initialBits, clazz);
    }    
    
    public ServiceObjectHolder(Class<T> clazz, ServiceObjectValidator<T> validator) {
        this.validator = validator;
        this.data = new ServiceObjectData<T>(DEFAULT_BITS, clazz);
    }  
    
    /**
     * Add new service object and return the index to retrieve the object later.
     * 
     * This is not thread safe and must not be called concurrently.
     * 
     * @param serviceObject
     * @return
     */
    public long add(T serviceObject) {
        //Not thread safe, must be called by one thread or sequentially    
        long index;
        int modIdx;
        
        long hardStop = sequenceCounter+data.size;
        do {
            index = ++sequenceCounter;
            modIdx = data.mask & (int)index;
        
            if (index==hardStop) {
                //copy and grow the data space, done locally then it is all replaced at once to not break concurrent callers
                data = new ServiceObjectData<T>(data, 2);                  
            }
                        
            //keep going if we have looped around and hit a bucket which is already occupied with something valid.
        } while (null != data.serviceObjectValues[modIdx] && validator.isValid(data.serviceObjectValues[modIdx]));
        
        data.serviceObjectKeys[modIdx] = index;
        data.serviceObjectValues[modIdx] = serviceObject;
        
        return index;
    }
    
    /**
     * Given the index value return the valid value object or null.
     * 
     * Side effect, if the value is invalid it is set to null to release the resources sooner.
     * 
     * @param index
     * @return
     */
    public T getValid(final long index) {  
        //must ensure we use the same instance for the work
        ServiceObjectData<T> localData = data;
        
        int modIdx = localData.mask & (int)index;
        return (index != localData.serviceObjectKeys[modIdx] ? null : (validator.isValid(localData.serviceObjectValues[modIdx]) ? localData.serviceObjectValues[modIdx] : null ));
    }

    /**
     * Given the index value return the value object or null.
     * 
     * @param index
     * @return
     */
    public T get(final long index) {  
        //must ensure we use the same instance for the work
        ServiceObjectData<T> localData = data;
        
        int modIdx = localData.mask & (int)index;
        return (index != localData.serviceObjectKeys[modIdx] ? null : localData.serviceObjectValues[modIdx]);
    }
    
    public long size() {
        return sequenceCounter;
    }
    

    /**
     * Loop forever around all valid objects.
     * Only returns null when there are no valid items to loop over.
     * @return
     */
    public T next() {
        
        ServiceObjectData<T> localData = data;
        int index = loopPos;
        int modIdx;
        int hardStop = index+localData.size;
        T result = null;
        do {
            modIdx = (int)(++index) & localData.mask;
            result = localData.serviceObjectValues[modIdx];                        
        } while ((null == result || !validator.isValid(result) ) && index<=hardStop);
        
        loopPos = modIdx;
        return index<=hardStop ? result : null;    
        
    }

    public static long getSequenceCount(ServiceObjectHolder sho) {
        return sho.sequenceCounter; 
    }
    
    public static long getRemovalCount(ServiceObjectHolder sho) {
        return sho.removalCounter; 
    }
    
    
}
