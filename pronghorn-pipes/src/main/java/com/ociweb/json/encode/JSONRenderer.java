package com.ociweb.json.encode;

import com.ociweb.pronghorn.pipe.ChannelWriter;
import com.ociweb.pronghorn.util.AppendableByteWriter;

public class JSONRenderer<T> extends JSONRoot<T, T, JSONRenderer<T>> {
    private boolean locked = false;

    public JSONRenderer() {
        super(new JSONBuilder<T, T>());
    }

    public JSONRenderer(JSONKeywords keywords) {
        super(new JSONBuilder<T, T>(keywords));
    }

    public boolean isLocked() {
        return locked;
    }

    public void render(AppendableByteWriter<?> writer, T source) {
        assert(locked) : "JSONRenderers can only be rendered once locked";
        builder.render(writer, source);
    }

    @Override
    JSONRenderer<T> rootEnded() {
        locked = true;
        return this;
    }

    //render and prefix the field with a short length;
	public void renderWithLengthPrefix(T source, ChannelWriter target) {
	
		int startPos = target.absolutePosition();
		target.writeShort(-1);//hold these 2 for later			
		int startTextPos = target.absolutePosition();			
				
		render(target, source);
		
		int stopTextPos = target.absolutePosition();
		int length = stopTextPos-startTextPos;		
		if (length>Short.MAX_VALUE) {
			throw new ArrayIndexOutOfBoundsException();
		}
		
		target.absolutePosition(startPos);
		target.writeShort(length); //set the actual length now that we know.
		target.absolutePosition(stopTextPos);
	}

}
