package com.ociweb.json.appendable;

import com.ociweb.pronghorn.pipe.ChannelReader;
import com.ociweb.pronghorn.pipe.ChannelWriter;

public interface ChannelExternalizable {
    void writeExternal(ChannelWriter out);
    void readExternal(ChannelReader in);
}
