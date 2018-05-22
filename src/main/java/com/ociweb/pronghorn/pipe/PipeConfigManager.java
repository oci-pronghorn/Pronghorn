package com.ociweb.pronghorn.pipe;

public class PipeConfigManager {

	private PipeConfig[] configs;
	private int configCount;
	private int defaultMinimumFragmentsOnPipe;
	private int defaultMaximumLengthOfVariableLengthFields;
	
	public PipeConfigManager() {
		this(4, 32, 512);
	}
	
	public PipeConfigManager(int initialCount, int defaultMinimumFragmentsOnPipe, int defaultMaximumLengthOfVariableLengthFields) {
		this.configs = new PipeConfig[initialCount];
		this.configCount = 0;
		if (defaultMinimumFragmentsOnPipe>1024) {
			throw new UnsupportedOperationException("Why is this value "+defaultMinimumFragmentsOnPipe+" soo large?");
		}
		this.defaultMinimumFragmentsOnPipe = defaultMinimumFragmentsOnPipe;
		this.defaultMaximumLengthOfVariableLengthFields = defaultMaximumLengthOfVariableLengthFields;
		
	}
	
	public <S extends MessageSchema<S>> PipeConfig<S> addConfig(int minimumFragmentsOnPipe, int maximumLengthOfVariableLengthFields, Class<S> clazz) {
		
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
			configs[idx] = newConfig;
		}
		return newConfig;
	}
	

	public <S extends MessageSchema<S>> void ensureSize(Class<S> clazz, int queueLength, int maxMessageSize) {
		int idx = findIndex(clazz);
		if (idx>=0) {
			//we found it 
			PipeConfig<S> oldConfig = (PipeConfig<S>)configs[idx];
			
			int oldQueueLen = oldConfig.minimumFragmentsOnPipe();
			int oldMaxVarLenSize = oldConfig.maxVarLenSize();
			if (queueLength>oldQueueLen || maxMessageSize>oldMaxVarLenSize) {
				addConfig(Math.max(oldQueueLen,queueLength), Math.max(oldMaxVarLenSize, maxMessageSize), clazz);
			}
		} else {
			//add it was not found
			addConfig(Math.max(queueLength,defaultMinimumFragmentsOnPipe),Math.max(maxMessageSize, defaultMaximumLengthOfVariableLengthFields),clazz);
		}
	}	
	
    public <S extends MessageSchema<S>> PipeConfig<S> getConfig(Class<S> clazz) {
    	
    	int idx = findIndex(clazz);    	
    	if (idx>=0) {
    		return (PipeConfig<S>)configs[idx];
    	}
    	//when undefined build store and return the default
    	return addConfig(defaultMinimumFragmentsOnPipe, defaultMaximumLengthOfVariableLengthFields, clazz);
    	
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
    	int idx = i;
		return idx;
	}

	public <S extends MessageSchema<S>> Pipe<S> newPipe(Class<S> clazz) {		
		return new Pipe<S>(getConfig(clazz));
	}

}
