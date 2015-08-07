package org.slf4j.impl;

public interface RingBufferLoggerMessageConsumer {
	void consumeMessage(StringBuffer message);
}
