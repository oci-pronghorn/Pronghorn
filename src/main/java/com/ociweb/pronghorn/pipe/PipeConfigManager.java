package com.ociweb.pronghorn.pipe;

public class PipeConfigManager {

	private PipeConfig[] configs;
	private int configCount;
	private int defaultMinimumFragmentsOnPipe;
	private int defaultMaximumLenghOfVariableLengthFields;
	
	public PipeConfigManager() {
		this(4, 16, 512);
	}
	
	public PipeConfigManager(int initialCount, int defaultMinimumFragmentsOnPipe, int defaultMaximumLenghOfVariableLengthFields) {		
		this.configs = new PipeConfig[initialCount];
		this.configCount = 0;
		this.defaultMinimumFragmentsOnPipe = defaultMinimumFragmentsOnPipe;
		this.defaultMaximumLenghOfVariableLengthFields = defaultMaximumLenghOfVariableLengthFields;
		
	}
	
	public <S extends MessageSchema<S>> PipeConfig<S> addConfig(int minimumFragmentsOnPipe, int maximumLenghOfVariableLengthFields, Class<S> clazz) {
		
		PipeConfig<S> newConfig = MessageSchema
								.findInstance(clazz)
								.newPipeConfig(minimumFragmentsOnPipe, maximumLenghOfVariableLengthFields);
		
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
	
    public <S extends MessageSchema<S>> PipeConfig<S> getConfig(Class<S> clazz) {
    	
    	int idx = findIndex(clazz);    	
    	if (idx>=0) {
    		return (PipeConfig<S>)configs[idx];
    	}
    	//when undefined build store and return the default
    	return addConfig(defaultMinimumFragmentsOnPipe,defaultMaximumLenghOfVariableLengthFields,clazz);
    	
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
