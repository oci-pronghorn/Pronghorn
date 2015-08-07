package org.slf4j.impl;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;

public class RingBufferLoggerFactory implements ILoggerFactory {
	private Map<String, RingBufferLogger> loggerMap;
	
	public RingBufferLoggerFactory() {
		loggerMap = new HashMap<String, RingBufferLogger>();
	}
	
	@Override
	public Logger getLogger(String name) {
        synchronized (loggerMap) {
	        if (!loggerMap.containsKey(name)) {
               loggerMap.put(name, new RingBufferLogger(name));
            }
            return loggerMap.get(name);
        }
	}

}
