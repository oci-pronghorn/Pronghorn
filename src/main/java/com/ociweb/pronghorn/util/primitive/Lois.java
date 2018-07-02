package com.ociweb.pronghorn.util.primitive;

import com.ociweb.pronghorn.pipe.ChannelReader;
import com.ociweb.pronghorn.pipe.ChannelWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.RawDataSchemaUtil;

/**
 * ListOfIntSets aka Lois      
 * @author Nathan Tippy
 *
 */
public class Lois {
	
	/////////////////////////////////
	//first int is jump, zero is the end of the line
	//top 3 bits of second int is operator
	//lower 29 bits are custom for operator
	/////////////////////////////////
	static int LOISOpSimpleList = 0;
	static int LOISOpBitMap = 1;
	//static int LOISOpRLE = 2;
	static LoisOperator[] operatorIndex = new LoisOperator[] {
			new LoisOpSimpleList(LOISOpSimpleList),
			new LoisOpBitMap(LOISOpBitMap)//,
			//new LoisOpRLE(LOISOpRLE)
	};
	////////
	final int blockSize; //must be at least 4 and a power of 2 is best
	////////
	private transient int     savePosition = -1;	
	private transient int     loadPosition = -1;
	////////
    //Internally this class behaves like an array of sets of int values 
	//The implementation however keeps the int values ordered to support optimizations to minimize memory
	
	boolean supportBitMaps = true; //needed for testing
	boolean supportRLE = false; //needed for testing
	
	int[] data; //this array contains [<block data><recycled blocks list> gap <list entries>]
	private int blockLimit;
	//starting at block limit we store the locations of recycled objects
	private int recycledCount;
	//list is stored from data.length-1, this is the count
	private int listCount;	
	
	public Lois() {
		this(32, 1<<8);
	}
	
	public Lois(int blockSize, int initBlocks) {
		assert(blockSize>=4) : "block size must be at least 4";
		assert(initBlocks>=16) : "initBlocks must be at least 16";
		this.blockSize = blockSize;
		this.data = new int[blockSize*initBlocks];
	}

	/**
	 * 
	 * @param pipe source
	 * @return true when the load is complete otherwise false it needs to be called again.
	 */
	public boolean load(Pipe<RawDataSchema> pipe) {
		
		while (Pipe.hasContentToRead(pipe)) {
			
			boolean isEnd = RawDataSchemaUtil.accumulateInputStream(pipe);
			
			
			ChannelReader reader = Pipe.inputStream(pipe);			
			int startingAvailable = reader.available();
			if (isEnd && startingAvailable==0) {
				return true;//no data, no file
			}
		
			if (!readLoadData(pipe, isEnd, reader)) {
				//did no reads, needs more data
				return false;
			};
			RawDataSchemaUtil.releaseConsumed(pipe, reader, startingAvailable);
			
			if (loadPosition==data.length) {
				loadPosition = -1;
				//System.err.println("done with load..");
				return true;
			}
			
		}
		
		return false;
	}

	private boolean readLoadData(Pipe<RawDataSchema> pipe, boolean isEnd, ChannelReader reader) {
		if (loadPosition==-1) {
			//note this value here forces us to keep init at 16 and min block at 4
			if (reader.available()<((5 * ChannelReader.PACKED_INT_SIZE)
					               +(2 * ChannelReader.BOOLEAN_SIZE))) {
				return false;//not enough data yet to read header cleanly
			}
			
			int loadedBlockSize = reader.readPackedInt();
			assert(loadedBlockSize==blockSize) : "Can not load, not same block  size";
			if (loadedBlockSize!=blockSize) {
				throw new UnsupportedOperationException("Unable to load due to block size mismatch");
			}			
			supportBitMaps = reader.readBoolean();
			supportRLE = reader.readBoolean();		
			blockLimit = reader.readPackedInt();
			recycledCount = reader.readPackedInt();
			listCount = reader.readPackedInt();
			
			final int dataLength = reader.readPackedInt();
			if (data==null || data.length!=dataLength) {
				data = new int[dataLength];
			}
			//System.err.println("load data len of  "+data.length);
			
			loadPosition = 0;
		}
		
		//while we have lots of data or we have encountered the end of the data.
		while (((reader.available()>=ChannelReader.PACKED_INT_SIZE) || isEnd) 
				&& loadPosition<data.length) {
			data[loadPosition++] = reader.readPackedInt();
			
			//System.err.println(data[loadPosition-1]+" at pos "+(loadPosition-1));				
		}
		//System.err.println("is end "+isEnd+"  "+reader.available()+" pipe "+pipe);
		return true;
	}

	/**
	 * Save this to the output stream, Must continue to call save until it returns
	 * false.  This allows very large storage without requiring the pipe be be so large.
	 * 
	 * @param pipe target
	 * @return true when save is done, false when we need to call again
	 */
	public boolean save(Pipe<RawDataSchema> pipe) {
		assert (pipe.maxVarLen > ((ChannelReader.PACKED_INT_SIZE*5) + (ChannelReader.BOOLEAN_SIZE*2))) : "Pipes must hold longer messages to write this content";
				
		while (Pipe.hasRoomForWrite(pipe)) {					
			int size = Pipe.addMsgIdx(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1);
			ChannelWriter writer = Pipe.openOutputStream(pipe);
			if (savePosition==-1) { //new file
				writer.writePackedInt(blockSize);
				writer.writeBoolean(supportBitMaps);
				writer.writeBoolean(supportRLE);				
				writer.writePackedInt(blockLimit);
				writer.writePackedInt(recycledCount);
				writer.writePackedInt(listCount);
				writer.writePackedInt(data.length);
				//System.err.println("save len of  "+data.length);
				savePosition = 0;
			}
			while (savePosition<data.length && writer.remaining()>=ChannelReader.PACKED_INT_SIZE) {
				//System.err.println("save "+data[savePosition]+" at "+savePosition);
				writer.writePackedInt(data[savePosition++]);				
			}			
			writer.closeLowLevelField();
			Pipe.confirmLowLevelWrite(pipe, size);
			Pipe.publishWrites(pipe);
		
			if (savePosition==data.length) {
				savePosition = -1;
				return true;
			}
		}
		return false;
	}
	
	
	LoisOperator operator(int setId) {
		return operatorIndex[0x3&data[setId+1]>>>29];
	}
	
	int next(int id) {
		return data[id];
	}
	
	/**
	 * Find index to new empty block, this will grow data if required
	 */
	int newBlock() {
		if (recycledCount==0) {
			
			blockLimit += blockSize;
			if (blockLimit <= data.length-listCount) {
				
			} else {
				growDataSpace();
			}
			return blockLimit;
		} else {
			System.err.println("used a recycled block");
			//use this old block we want to recycle which was stored after the block limit
			return data[blockLimit+(--recycledCount)];
		}
	}

	private void growDataSpace() {
		
		//must grow data first, must not grow larger than 29 bits
		if (data.length > (1<<30)) {
			throw new UnsupportedOperationException("LOIS data structure can not be larger than 8G of data.");
		}
		
		int[] newData = new int[data.length*2];
		//copy data & recycled values
		System.arraycopy(data, 0, newData, 0, data.length-listCount);
		//copy list data
		System.arraycopy(   data,    data.length-listCount, 
				         newData, newData.length-listCount, listCount);
		
		data = newData;
	}


	/**
	 * Check if any of the values starting with startValue and less than
	 * end value are stored in the set.
	 * 
	 * @param setId set to check
	 * @param startValue int first value to confirm
	 * @param endValue int exclusive stop value
	 * @return true if any value in the range is in the list
	 */
	public boolean containsAny(int setId, int startValue, int endValue) {
		
		int id = setId;
		int next = 0;
		
		//////////////////////////////////
		//skip over all the blocks where this value comes after the full block
		////////////////////////////////
		while ((next = data[id])!=0 && operator(id).isAfter(id, startValue, this)) {			
			id = next;
		}

		//scan from here for value in range, once we are out of range then stop looking
        //keep going until the end value is not after this position		
		while (!operator(id).isBefore(id, endValue, this)) {
			if (operator(id).containsAny(id, startValue, endValue, this)) {
				return true;
			}
			id = data[id];
			if (0==id) {
				break;
			}
		}

		return false;
	}
	
	
	public boolean insert(int setId, int value) {
		assert(setId>=0) : "bad set id "+setId;		
		
		int id = setId;
		int next = 0;
		
		//skip over all the blocks where this value comes after
		while ((next = data[id])!=0 && operator(id).isAfter(id, value, this)) {
			id = next;
		}
		//if is before next must use id if not before next must use next
		if (0==next || operator(next).isBefore(next, value, this)) {
			return operator(id).insert(id, value, this);
		} else {
			//was not before so it must be inside the next
			return operator(next).insert(next, value, this);
		}
	}
	
	public boolean remove(int setId, int value) {
		
		int id = setId;
		do {
			if (operator(id).remove(-1, id, value, this)) {
				return true;
			}
			id = data[id];
		} while (id!=0); 
		return false;
	}
	
	public boolean removeFromAll(int value) {
		int p = 0;
		boolean found = false;
		while (p<blockLimit) {
			found |= operator(p).remove(-1, p, value, this);
			p+=blockSize;
		}
		return found;
	}

	public void visitSetIds(LoisVisitor visitor) {
		
		int limit = data.length-listCount;
		int p = data.length;
		while (--p >= limit) {
			visitor.visit(data[p]);				
		}
		
	}
	
	public void visitSet(int setId, LoisVisitor visitor) {
		int id = setId;
		int next = 0;
		
		//visit all blocks
		do {
			if (!operator(id).visit(id, visitor, this)) {
				return;
			}
			id = data[id];//next block		
		} while (id!=0);
		
	}
	
	public int newSet() {
		int setHead = newBlock();
		
		//grow if there is no room
		if ((blockLimit+recycledCount) >= (data.length-(listCount+1))) {
			growDataSpace();
		}
		
		return data[data.length-(++listCount)] = setHead;

	}

	public void recycle(int idx) {
		
		//grow if there is no room
		if ((blockLimit+recycledCount+1) >= (data.length-listCount)) {
			growDataSpace();
		}
		
		data[blockLimit+recycledCount++] = idx;

	}


	
	
}
