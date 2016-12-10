package com.ociweb.pronghorn.network;

public class ConnectionContext {

    private long channelId;
    
    public ConnectionContext() {
        this.channelId = (long) -1;
    }

	public long getChannelId() {
		return channelId;
	}

	public void setChannelId(long channelId) {
		this.channelId = channelId;
	}

    
}
