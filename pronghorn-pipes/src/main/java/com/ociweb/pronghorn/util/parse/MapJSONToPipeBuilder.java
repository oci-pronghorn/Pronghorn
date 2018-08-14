package com.ociweb.pronghorn.util.parse;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.token.TypeMask;
import com.ociweb.pronghorn.util.TrieKeyable;

public abstract class MapJSONToPipeBuilder<M extends MessageSchema<M>, K extends Enum<K> & TrieKeyable> {

	protected final K[] allEnums;
	protected final int msgIdx;
	protected final FieldReferenceOffsetManager from;
	protected final int[] bitFields;
	
	protected MapJSONToPipeBuilder(M schema, Class<K> enums, int msgIdx, int ... bitFields) {
		this.allEnums = enums.getEnumConstants();
		this.msgIdx = msgIdx;
		this.from = MessageSchema.from(schema);
		this.bitFields = bitFields;
	}	
	
	protected long buildUniqueId(K ... path) {
		
		long accum = 0;
		for(int i = 0; i<path.length; i++) {
			accum = (accum*allEnums.length)+path[i].ordinal();
		}
		return accum;
	}	
		
	public abstract int bitMask(long id);
	public abstract boolean usesBitMask(long id);
	public abstract int getLoc(long id);
	
	public int[] bitFields() {
		return bitFields;
	}

	public boolean isInteger(int loc) {
		return TypeMask.isInt(FieldReferenceOffsetManager.extractTypeFromLoc(loc));		
	}

	public boolean isLong(int loc) {
		return TypeMask.isLong(FieldReferenceOffsetManager.extractTypeFromLoc(loc));	
	}

	public boolean isNumeric(int loc) {
		int type = FieldReferenceOffsetManager.extractTypeFromLoc(loc);	
		return TypeMask.isLong(type) | TypeMask.isInt(type);
	}
	

	public int messageId() {
		return msgIdx;
	}

	
}
