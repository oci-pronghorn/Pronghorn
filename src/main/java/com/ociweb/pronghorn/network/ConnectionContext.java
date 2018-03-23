package com.ociweb.pronghorn.network;

public final class ConnectionContext implements SelectionKeyHashMappable {

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
	
	private int skPos = -1;
	
	@Override
	public void skPosition(int position) {
		skPos=position;
	}

	@Override
	public int skPosition() {
		return skPos;
	}


}
