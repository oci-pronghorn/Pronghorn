package com.ociweb.pronghorn.pipe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeConfigManager {

	private PipeConfig[] configs;
	private int configCount;
	private int defaultMinimumFragmentsOnPipe;
	private int defaultMaximumLengthOfVariableLengthFields;
	private Logger logger = LoggerFactory.getLogger(PipeConfigManager.class);
			
	public PipeConfigManager() {
		this(4, 2, 512);
	}
	
	public PipeConfigManager(int initialCount, int defaultMinimumFragmentsOnPipe, int defaultMaximumLengthOfVariableLengthFields) {
		this.configs = new PipeConfig[initialCount];
		this.configCount = 0;
		this.defaultMinimumFragmentsOnPipe = defaultMinimumFragmentsOnPipe;
		this.defaultMaximumLengthOfVariableLengthFields = defaultMaximumLengthOfVariableLengthFields;
		
	}
	
	public <S extends MessageSchema<S>> PipeConfig<S> addConfig(int minimumFragmentsOnPipe,final int maximumLengthOfVariableLengthFields, Class<S> clazz) {
		
		PipeConfig<S> newConfig = MessageSchema
								.findInstance(clazz)
								.newPipeConfig(minimumFragmentsOnPipe, maximumLengthOfVariableLengthFields);
		
		return addConfig(newConfig);
	}

	public <S extends MessageSchema<S>> PipeConfig<S> addConfig(PipeConfig<S> newConfig) {
		int idx = findIndex(newConfig.schema);   
		if (idx<0) {

			if (configCount >= configs.length) {
				//grow, we are out of room
				PipeConfig[] newConfigs = new PipeConfig[configs.length*2];
				System.arraycopy(configs, 0, newConfigs, 0, configs.length);
				configs = newConfigs;
			}			
			configs[configCount++] = newConfig;
			
		} else {
			//if (configs[idx].minimumFragmentsOnPipe()>newConfig.minimumFragmentsOnPipe()) {
			//	throw new UnsupportedOperationException("Already ensured size larger than new assignment, use ensure not add.");
			//}
			configs[idx] = newConfig;
		}
		return newConfig;
	}
	

	public <S extends MessageSchema<S>> void ensureSize(Class<S> clazz, final int queueLength, final int maxMessageSize) {
		
		int oldQueueLen = 0;
		int oldMaxVarLenSize = 0;
		int idx = 0;
		try {
			idx = findIndex(clazz);
			if (idx>=0) {
				//we found it 
				PipeConfig<S> oldConfig = (PipeConfig<S>)configs[idx];
				
				oldQueueLen = oldConfig.minimumFragmentsOnPipe();
				oldMaxVarLenSize = oldConfig.maxVarLenSize();
	
				if (queueLength>oldQueueLen || maxMessageSize>oldMaxVarLenSize) {
					addConfig(Math.max(oldQueueLen,queueLength), Math.max(oldMaxVarLenSize, maxMessageSize), clazz);
				}
			} else {
				//add it was not found
				addConfig(Math.max(queueLength,defaultMinimumFragmentsOnPipe),
						  Math.max(maxMessageSize, defaultMaximumLengthOfVariableLengthFields),clazz);
			}
		} catch (UnsupportedOperationException t) {
			//report where these values came from
			if (idx >= 0) {
				logger.warn("Max of len from old:{} new:{} ", oldQueueLen, queueLength);
				logger.warn("Max of payload from old:{} new:{} ", oldMaxVarLenSize, maxMessageSize);
			} else {
				logger.warn("Max of len from default:{} new:{} ", defaultMinimumFragmentsOnPipe, queueLength);
				logger.warn("Max of payload from default:{} new:{} ", defaultMaximumLengthOfVariableLengthFields, maxMessageSize);
			}

        	throw(t);
        }
		
	}	
	
    public <S extends MessageSchema<S>> PipeConfig<S> getConfig(Class<S> clazz) {
    	
    	S instance = MessageSchema.findInstance(clazz);
		final int idx = findIndex(instance);    	
    	if (idx>=0) {
    		return (PipeConfig<S>)configs[idx];
    	}
		return buildNewConfig(instance);    	
    }

	private <S extends MessageSchema<S>> PipeConfig<S> buildNewConfig(S instance) {
		final int maximumLengthOfVariableLengthFields = defaultMaximumLengthOfVariableLengthFields;
    	//when undefined build store and return the default
    	PipeConfig<S> newConfig = instance.newPipeConfig(defaultMinimumFragmentsOnPipe, maximumLengthOfVariableLengthFields);

		if (configCount >= configs.length) {
			//grow, we are out of room
			PipeConfig[] newConfigs = new PipeConfig[configs.length*2];
			System.arraycopy(configs, 0, newConfigs, 0, configs.length);
			configs = newConfigs;
		}			
		configs[configCount++] = newConfig;
		
		return newConfig;
	}

	private <S extends MessageSchema<S>> int findIndex(Class<S> clazz) {
		S goal = MessageSchema.findInstance(clazz);
    	return findIndex(goal);
	}

	private <S extends MessageSchema<S>> int findIndex(S goal) {
		//this is a simple linear search, this code is normally called with
    	//  1. short lists of configs
    	//  2. on startup
    	//so this is not going to be a problem.
    	int i = configCount;
    	while (--i>=0) {
    		if (configs[i].schema == goal) {
    			break;
    		}
    	}
    	return i;
	}

	public <S extends MessageSchema<S>> Pipe<S> newPipe(Class<S> clazz) {		
		return new Pipe<S>(getConfig(clazz));
	}

}
