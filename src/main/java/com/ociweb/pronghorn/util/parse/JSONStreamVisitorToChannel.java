package com.ociweb.pronghorn.util.parse;

import java.io.IOException;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.json.JSONAccumRule;
import com.ociweb.json.JSONType;
import com.ociweb.pronghorn.pipe.ChannelWriter;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.util.hash.LongHashTable;
import com.ociweb.pronghorn.util.ByteConsumer;
import com.ociweb.pronghorn.util.PackedBits;

public class JSONStreamVisitorToChannel implements JSONStreamVisitor {

	private static final Logger logger = LoggerFactory.getLogger(JSONStreamVisitorToChannel.class);
	
	private final JSONFieldSchema schema;

	private static final int HEADER_INDEX_FIELDS = 3;
	private static final int HEADER_DIM_USAGE_FIELDS = 2;
	//////////////////
	//rows each map to one of the selected fields
	//////////////////
	//NOTE: these two are populated as the data is parsed.
	private final byte[][] encodedData; //data to be sent as assembled 
	private final int[][] indexData;    //0-bytes, 
	                                    //1-count,
	                                    //2-last count value, for null detect
										//remaining fields only for indexable arrays
	                                    //n (eg 3) -one for each dim, index into counter
	                                    //m (eg 10)-each count values
							//////////////////  example   [3][1][4][2][5][6][3][7][8][9]
							///////////////////////        3
							//////////////////        1     2      3
							//////////////////        4    5 6   7 8 9
							////////////////////////////////////////////////////////
	//these string lengths are used to hold them out of order while text is accumulated
	private final long[][] textLengths;	
	
	//////////////////////////////////////////////////
	//for this dim which rows need to be updated above (code complete)
	//////////////////////////////////////////////////
	//NOTE: this is build once on declaration
	private final int[][] dimUsages;    //0 - (n+1)
	                                    //1 - dim depth, all these fields will use at same depth.
	                                    //n ... field indexes 
	//////////////////////////////////////////////////

	/*
	 *  Field Types
	 *  ** Normal fields without any [] in the path
	 *  ** Grouped field which share the same [] root,  nulls are added to make it match , counts
	 *  ** MapReduce fields which are isolated and do not contain nulls except for explict ones.
	 * 
	 *  if field is grouped we must inject the right nulls to make them line up.
	 */
	
	
	
	private int stackPosition = 0;
	private boolean keyExpected = true;
	
	private final LongHashTable lookupFieldTable;
	private final LongHashTable lookupDimsTable;
	
	private static final int initSize = 4;  
	private long[] uStack = new long[initSize];
	
	private boolean isAccumulating = true;
	
	private PackedBits headerBits = new PackedBits();
	
	private final JSONByteConsumerUTF8 utf8byteConsumer;
	
	private int selectedRow;
		
	public int dimensions(int fieldId) {
		return schema.getMapping(fieldId).dimensions();
	}
	
	public boolean isAligned(int fieldId) {
		return schema.getMapping(fieldId).isAligned;
	}
	

	private void clearForNextAccumulate() {
		
		int i = indexData.length;
		while (--i>=0) {
			if (null!=indexData[i]) {
				Arrays.fill(indexData[i], 0);
			}
		}
		int j = textLengths.length;
		while (--j>=0) {
			if (null!=textLengths[j]) {
				Arrays.fill(textLengths[j], 0);
			}
		}
		
		isAccumulating = true;
	}
	
	/*
	 * 
	 * JSON flat data design for mechanical sympathy.
	 * All stack position hashes can loop around on 64 bits and are unlikely to collide.
	 * The total for each position is fields+maxdepth,  for an array use the array stack dept as its id.
	 * 
	 * When recording the fields every one can be an array, for each new entry we add one more
	 * The storage int arrays will grow as needed by powers of two.
	 * 
	 * To write to PHast stream the entire array is written as one sequential run however it 
	 * represents an n dementinal space. so all arrays are prefixed by.
	 *   1. the number of dementions (compression flags)
	 *   2. the scale of each demention 
	 *   
	 * If we have nested objects they will need to be reconstructed by the consumer this is 
	 * because all transmission are "column oriented" by design. This is optimal for the machine.  
	 * 
	 * 
	 * 
	 * 
	 */
	
	
	//for "no mach" we will use this object to consume the data
	private final ByteConsumer nullByteConsumer = new ByteConsumer() {
		@Override
		public void consume(byte[] backing, int pos, int len, int mask) {	
		}
		@Override
		public void consume(byte value) {
		}
	};

	
	private void ensureCapacity(int space) {
		
		if (space > uStack.length) {
			
			long[] tempU = new long[space*2];			
			System.arraycopy(uStack, 0, tempU, 0, uStack.length);			
			uStack =  tempU;
			
		}		
	}
	
	/**
	 * grow as needed and store new length value in the right row
	 */
	public void addTextLength(long[][] data, int selection, long value, int pos) {
		
		long[] target = data[selection];
		if (null == target) {
			target = new long[pos+1];
			data[selection] = target;
		} else {
			if (target.length < pos+1) {
				long[] temp = new long[(pos+1)*2];
				System.arraycopy(target, 0, temp, 0, target.length);
				target = temp;
				data[selection] = target;
			}
		}
		target[pos] = value;
	}
	
	public JSONStreamVisitorToChannel(JSONFieldSchema schema) {

		this.schema = schema;
		
		/////////////
		//build up index arrays
		/////////////
		int totalDims = 0;
		int x = schema.mappingCount();
		this.indexData = new int[x][]; //counts and index into active dim pos
		this.encodedData = new byte[x][]; //data to be sent
	    this.textLengths = new long[x][];
		while (--x >= 0) {
			int dimensions = schema.getMapping(x).dimensions();
			totalDims += dimensions;
			this.indexData[x] = new int[HEADER_INDEX_FIELDS+dimensions]; //bytes, count, last-count
			this.encodedData[x] = new byte[2];

		}
				
		x =  totalDims;
		this.dimUsages = new int[x][];
		while (--x >= 0) {
			this.dimUsages[x] = new int[2]; //[fieldCount][dimDepth][n fields]
		}
		
		this.utf8byteConsumer = new JSONByteConsumerUTF8(encodedData, indexData);
		LongHashTable lookupFieldTableLocal = new LongHashTable(
					LongHashTable.computeBits(schema.mappingCount()<<2)
				);
		
		LongHashTable lookupDimUsageLocal = new LongHashTable(
				LongHashTable.computeBits(totalDims<<2)
			);
		
		int d = 0;
		int m = schema.mappingCount();
		while (--m>=0) {
			//logger.info("key {} added to table ",mappings[x].pathHash());
			d = JSONFieldMapping.addHashToTable(m, d,
					lookupFieldTableLocal,
					lookupDimUsageLocal,
					dimUsages,
					schema.getMapping(m));			
		}
			
		//////////////
		/////////////
		
		//must build hash table lookup for rows
		lookupDimsTable = lookupDimUsageLocal;
		lookupFieldTable = lookupFieldTableLocal;
		
	}


	public void clear() {
		int x = this.indexData.length;
		while (--x >= 0) {
			Arrays.fill(this.indexData[x], 0);//is this required?
			Arrays.fill(this.textLengths[x], 0);//is this required?
		}	
	}
	
	public void export(ChannelWriter writer, int[] fieldIndexPositions) {
		
		//TODO: are we going to remove these header bits??
		
		headerBits.clear();
		//gather the bits for the nulls since all the fields are nullable
		
		for(int i = 0; i<indexData.length; i++) {
			if (0 == indexData[i][1]) { //TODO: must be incremented for each instance??
				//no data so flag it
				headerBits.setValue(i, 1);
				//logger.info("Note null at position {}",i);
			}
		}
		try {
			headerBits.write(writer);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		/////////////////////////////////////////////////////
		
		for(int i = 0; i<indexData.length; i++) {
			JSONFieldMapping mapping = schema.getMapping(i);
			
			/////////////////write index into the data
			if (null!=fieldIndexPositions) {
				assert(fieldIndexPositions.length==indexData.length);
				DataOutputBlobWriter.setIntBackData((DataOutputBlobWriter<?>)writer, writer.position(), fieldIndexPositions[i]);
			}
			
			/////////////////write data			
			final int dims = mapping.dimensions(); 
			if (dims>0) {
				//write matrix data then 
				int[] meta = indexData[i];
				
				
				//this limit assumes 4 if array data is not found past that point
				final int limit = 1+maxDimPos(this.indexData[i], dims);
				int idx = dims + HEADER_INDEX_FIELDS;
		  	    //logger.info("meta data {} from idx {} to limit {}", Arrays.toString(meta), idx, limit);
				//logger.info("write field header of size {} len {} dims {} indexData {}", limit-idx, limit, mapping.dimensions(),meta);
				while (idx<limit) {	
					writer.writePackedInt(meta[idx++]);
				}
			}
			
			JSONType type = schema.getMapping(i).type;
			if (JSONType.TypeString == type) {
				//convert the write to packed values as we send
				//when the data was collected the length was summed up. as a result
				//this is the earliest point when the length can be written as packed.
				
				byte[] source = encodedData[i];
				int pos = 0;
				int c = indexData[i][1];
				
				for(int j = 0; j<c; j++) {
					//NOTE: if we need to add support for 2G+ strings the change should be here
					int len = (int)textLengths[i][j];
					writer.writePackedInt(len);
					
					if (len>0) {
						writer.write(source, pos, (int)len);
						pos += (int)len;		
					}
				}
				
			} else {
				
				//copy data
				//logger.info("for field {} wrote {} bytes ",i,encodedData[i].length);
				writer.write(encodedData[i],0,indexData[i][0]);
			}
		}
		
		clearForNextAccumulate();
				
	}

	
	@Override
	public void nameSeparator() {
		keyExpected = false;
	}


	@Override
	public void whiteSpace(byte b) {
		//nothing to do with white space
	}

	@Override
	public boolean isReady() {
		return isAccumulating;
	}
	
	
	@Override
	public void beginObject() {
		stackPosition++;
		//logger.info("begin object {}", stackPosition);
		
		keyExpected = true;
		
		ensureCapacity(stackPosition);
		
	}

	
	@Override
	public void customString(int id) {
		
		assert(schema.maxFieldUnits()>0) : "must have some keys";
		
		if (keyExpected) {
			
			long value = (stackPosition > 1) ? (uStack[stackPosition-2]*schema.maxFieldUnits() + id) : id;
			
			uStack[stackPosition-1] = value;
	
			selectedRow = LongHashTable.getItem(lookupFieldTable, value);
						
			if (0==selectedRow && !LongHashTable.hasItem(lookupFieldTable, value)) {
				selectedRow = -1;				
			}
			//logger.info("stack {} selected row {} value {} keys {}",
			//		stackPosition,selectedRow,value, schema.maxFieldUnits());
			
		} else {
			if (selectedRow>=0) {
				//no need to generate this is not a key but a value
				selectedRow = -1;
			}
		}
		
	}
	
	@Override
	public void endObject() {
		if (stackPosition>0) {
			if (--stackPosition == 0) {
				//logger.info("end of full template block, stack is empty");
				
				//stop accumulating until what we have so far is exported.
				isAccumulating = false;
			}
		}
		//logger.info("end of an object depth {}",stackPosition);
		
	}

	@Override
	public void beginArray() {
		
		stackPosition++;		
		//////clear the count
		//logger.info("begin of array depth {}",stackPosition);
		
		ensureCapacity(stackPosition);
		
		long id = stackPosition + schema.uniqueFieldsCount();
		long prev = uStack[stackPosition-2] * schema.maxFieldUnits();
		long value = (stackPosition > 1) ? (prev + id) : id;
		
		uStack[stackPosition-1] = value;
		
		beginningOfArray(value);
		
	}

	@Override
	public void endArray() { 
			
		if (stackPosition>0) {
			stackPosition--;
			//logger.info("end of array depth {}",stackPosition);
			
			long hash = uStack[stackPosition];
			int dimIdx = LongHashTable.getItem(lookupDimsTable, hash);
			if (0!=dimIdx || LongHashTable.hasItem(lookupDimsTable, hash)) {
					
				
				int[] data = this.dimUsages[dimIdx];
				
				final int fieldCount = data[0];//this is (n-1) not the length
				final int dim = data[1];//which dim is this, one based
				//logger.info("write dim data count {} dim {} ",count, dim);
				
				//logger.info("end array {} {} {}",dimIdx,fieldCount,dim);
				
				for(int i=HEADER_DIM_USAGE_FIELDS; i<=fieldCount; i++) {	
					//logger.info("single entry process for {} {}",dim, data[i]);
					singleEntryField(dim, data[i]);	
				}
				
			}
		}
		
	}

	@Override
	public void valueSeparator() {
		if (stackPosition > 0) {
			endOfArrayEntry(uStack[stackPosition-1]);
		} else {
			//error
		}
	}

	
	private void beginningOfArray(long hash) {
		int dimIdx = LongHashTable.getItem(lookupDimsTable, hash);
		if (0!=dimIdx || LongHashTable.hasItem(lookupDimsTable, hash)) {
			int[] data = this.dimUsages[dimIdx];
			final int limit = data[0]; //this is count not length so we use <=
			
			final int dim = data[1];//which dim is this
			for(int i=HEADER_DIM_USAGE_FIELDS; i<=limit; i++) {
			
				final int fieldId = data[i];
				int[] matrixData = this.indexData[fieldId];
				//final int bytesEncoded = matrixData[0];
				final int countInstances = matrixData[1];
				matrixData[2] = countInstances;
								
				int max = maxDimPos(this.indexData[fieldId], dimensions(fieldId));
												
				final int dimIdxPos = dim + HEADER_INDEX_FIELDS - (dim==0?0:1);//TODO: not 100 sure
				matrixData[dimIdxPos] = max+1;
				
				
				//logger.info("at beginning of an array "+dimIdxPos+" stored idx of "+(max+1));
				
				////////////////
				//grow if needed
				if (max+1 >= matrixData.length) {
					
					int[] temp = new int[max*2];
					System.arraycopy(matrixData, 0, temp, 0, matrixData.length);
					this.indexData[fieldId] = matrixData = temp;
					
				}
				////////////////
				
				matrixData[matrixData[dimIdxPos]] = 0;//clear count
				
				
				//0-bytes, 
                //1-count,
				//2-last value
				//remaining fields only for indexable arrays
                //n (eg 3) -one for each dim, index into counter  (TODO: start as zeros !!!! )
                //m (eg 10)-each count values
				
			}
		} else {
			logger.warn("unable to find hash {}",hash);
		}
	}

	private int maxDimPos(int[] matrixData, final int fieldDims) {
		int max = fieldDims+HEADER_INDEX_FIELDS-1;//base positions for counters
		for(int j=HEADER_INDEX_FIELDS; j<fieldDims+HEADER_INDEX_FIELDS; j++) {
			max = Math.max(max, matrixData[j]);
		} 	
		//find max position so we can add 1 for the new position start
		return max;
	}

	
	private void endOfArrayEntry(long hash) {
		int dimIdx = LongHashTable.getItem(lookupDimsTable, hash);
		if (0!=dimIdx || LongHashTable.hasItem(lookupDimsTable, hash)) {
						
			singleEntry(dimIdx);
	
		}
	}

	private void singleEntry(int dimIdx) {
		int[] data = this.dimUsages[dimIdx];
		
		final int count = data[0];//count not length
		final int dim = data[1];//which dim is this
		//logger.info("write dim data count {} dim {} ",count, dim);
		
		for(int i=HEADER_DIM_USAGE_FIELDS; i<=count; i++) {
			
			singleEntryField(dim, data[i]);
			
		}
	}

	private void singleEntryField(final int dim, final int fieldId) {
		final int fieldDims = dimensions(fieldId);
		int[] matrixData = this.indexData[fieldId];

		final int countInstances = matrixData[1];
		
		//logger.info("dims {}=={}", dim, fieldDims);
		
		if (dim == fieldDims) {//is this the largest dim
			
			//logger.info("isAligned {} instances {}=={} ",isAligned(fieldId),countInstances,matrixData[2]);
			
			if (isAligned(fieldId) && (countInstances == matrixData[2])) {
				//did not get a value, so add a null 
				appendNull(fieldId);
			}
			matrixData[2] = countInstances; //store to know if it has changed...
		}

		matrixData[matrixData[dim+HEADER_INDEX_FIELDS-(0==dim?0:1)]]++;//inc array len count

	}



	@Override
	public void literalTrue() {
		
		if (selectedRow>=0) {
			JSONFieldMapping mapping = schema.getMapping(selectedRow);
			
			switch(mapping.type) {
				case TypeBoolean:
					writeBoolean(true);
				break;
				default:
					recordError(mapping.type, JSONType.TypeBoolean, Arrays.toString(mapping.path));
				break;
			}

			selectedRow = -1;
		}

		keyExpected = true;
				
	}

	@Override
	public void literalNull() {
		
		if (selectedRow>=0) {
			appendNull(selectedRow);			
			selectedRow = -1;
		}

		keyExpected = true;
			
	}


	private void appendNull(int row) {
		selectedRow = row;//pass row down to the member methods
		switch(schema.getMapping(row).type) {
			case TypeBoolean:
				writeNullBoolean();
				break;
			case TypeDecimal:
				writeNullDecimal();
				break;
			case TypeInteger:
				writeNullInteger();
				break;
			case TypeString:
				writeNullString();
				break;					
		}	
	
	}



	@Override
	public void literalFalse() {
		
		if (selectedRow>=0) {
			JSONFieldMapping mapping = schema.getMapping(selectedRow);
			
			switch(mapping.type) {
				case TypeBoolean:
					writeBoolean(false);
				break;
				default:
					recordError(mapping.type, JSONType.TypeBoolean, Arrays.toString(mapping.path));
				break;
			}

			selectedRow = -1;
		}

		keyExpected = true;
			
	}


	@Override
	public void numberValue(long m, byte e) {
		
		if (selectedRow>=0) {
			
			JSONFieldMapping mapping = schema.getMapping(selectedRow);
						
			if (JSONType.TypeInteger == mapping.type) {
				if (e==0) {
					//ok as integer
					writeInteger(m);
				} else {
					recordError(mapping.type, JSONType.TypeInteger, Arrays.toString(mapping.path));					
				}
				
			} else if (JSONType.TypeDecimal == mapping.type) {
				writeDecimal(m, e);
			}
		
			selectedRow = -1;
		}

		keyExpected = true;
			
	}

	
	private void writeNullString() {

		int idx = indexData[selectedRow][1]; 
		if (idx>0) {	
			//this block only happens when we have more than one value
			JSONAccumRule rule = schema.getMapping(selectedRow).accumRule;
			if (JSONAccumRule.Last == rule) {
				///if we already have data erase it to be replaced
				idx = indexData[selectedRow][1] = 0;
			} else if (JSONAccumRule.First == rule) {
				///if we already have data skip it
				return;
			}
		}
		
		addTextLength(textLengths, selectedRow, -1, idx);
		indexData[selectedRow][1] =  idx+1;
		
	}


	private void writeNullInteger() {
		int[] idx = indexData[selectedRow];
		
		int fieldCount = idx[1]; 
		if (fieldCount>0) {	
			//this block only happens when we have more than one value
			JSONAccumRule rule = schema.getMapping(selectedRow).accumRule;
			if (JSONAccumRule.Collect != rule) {
				if (JSONAccumRule.Last == rule) {
					///if we already have data erase it to be replaced
					fieldCount = idx[1] = 0;
					idx[0] = 0;
				} else if (JSONAccumRule.First == rule) {
					///if we already have data skip it
					return;
				}
			}
		}
		
		int newPos = idx[0];//bytes count used first

		byte[] data = targetByteArray(newPos, newPos+3);
				
		data[newPos++] = 0;
		data[newPos++] = 0;
		data[newPos++] = (byte)0x80;
		
		idx[0] = newPos;
		idx[1] = fieldCount + 1;
	}


	private void writeNullDecimal() {
		int[] idx = indexData[selectedRow]; 
		
		int fieldCount = idx[1]; 
		if (fieldCount>0) {	
			//this block only happens when we have more than one value
			JSONAccumRule rule = schema.getMapping(selectedRow).accumRule;
			if (JSONAccumRule.Collect != rule) {
				if (JSONAccumRule.Last == rule) {
					///if we already have data erase it to be replaced
					fieldCount = idx[1] = 0;
					idx[0] = 0;
				} else if (JSONAccumRule.First == rule) {
					///if we already have data skip it
					return;
				}
			}
		}
		
		int newPos = idx[0];//bytes count used first
				
		byte[] data = targetByteArray(newPos, newPos+4);
				
		data[newPos++] = 0;
		data[newPos++] = 0;
		data[newPos++] = (byte)0x80;
		data[newPos++] = 0;
		
		idx[0] = newPos;
		idx[1] = fieldCount + 1;
	
	}


	private void writeNullBoolean() {
		int[] idx = indexData[selectedRow]; 
		
		int fieldCount = idx[1]; 
		if (fieldCount>0) {	
			//this block only happens when we have more than one value
			JSONAccumRule rule = schema.getMapping(selectedRow).accumRule;
			if (JSONAccumRule.Collect != rule) {
				if (JSONAccumRule.Last == rule) {
					///if we already have data erase it to be replaced
					fieldCount = idx[1] = 0;
					idx[0] = 0;
				} else if (JSONAccumRule.First == rule) {
					///if we already have data skip it
					return;
				}
			}
		}
		
		int newPos = idx[0];//bytes count used first
				
		byte[] data = targetByteArray(newPos, newPos+1);
		
		data[newPos++] = -1;
		idx[0] = newPos;	
		idx[1] = fieldCount + 1;

	}
	
	private void writeBoolean(boolean b) {
		int[] idx = indexData[selectedRow]; 
		
		int fieldCount = idx[1]; 
		if (fieldCount>0) {	
			//this block only happens when we have more than one value
			JSONAccumRule rule = schema.getMapping(selectedRow).accumRule;
			if (JSONAccumRule.Collect != rule) {
				if (JSONAccumRule.Last == rule) {
					///if we already have data erase it to be replaced
					fieldCount = idx[1] = 0;
					idx[0] = 0;
				} else if (JSONAccumRule.First == rule) {
					///if we already have data skip it
					return;
				}
			}
		}
		
		int newPos = idx[0];//bytes count used first
				
		byte[] data = targetByteArray(newPos, newPos+1);
		
		data[newPos++] = b?(byte)1:(byte)0;
		idx[0] = newPos;
		idx[1] = fieldCount + 1;
	}
	
	private void writeInteger(long m) {
						
		int[] idx = indexData[selectedRow]; 
		
		int fieldCount = idx[1]; 
		if (fieldCount>0) {	
			//this block only happens when we have more than one value
			JSONAccumRule rule = schema.getMapping(selectedRow).accumRule;
			if (JSONAccumRule.Collect != rule) {
				if (JSONAccumRule.Last == rule) {
					///if we already have data erase it to be replaced
					fieldCount = idx[1] = 0;
					idx[0] = 0;
				} else if (JSONAccumRule.First == rule) {
					///if we already have data skip it
					return;
				}
			}
		}
		
		int newPos = idx[0];//bytes count used first
				
		byte[] data = targetByteArray(newPos, newPos+10);

		idx[0] = DataOutputBlobWriter.writePackedLong(m, data, Integer.MAX_VALUE, newPos);
		idx[1] = fieldCount + 1;
	}


	private void writeDecimal(long m, byte e) {
		int[] idx = indexData[selectedRow]; 
		
		int fieldCount = idx[1]; 
		if (fieldCount>0) {	
			//this block only happens when we have more than one value
			JSONAccumRule rule = schema.getMapping(selectedRow).accumRule;
			if (JSONAccumRule.Collect != rule) {
				if (JSONAccumRule.Last == rule) {
					///if we already have data erase it to be replaced
					fieldCount = idx[1] = 0;
					idx[0] = 0;
				} else if (JSONAccumRule.First == rule) {
					///if we already have data skip it
					return;
				}
			}
		}
		
		int newPos = idx[0];//bytes count used first
				
		byte[] data = targetByteArray(newPos, newPos+11);
		
		newPos = DataOutputBlobWriter.writePackedLong(m, data, Integer.MAX_VALUE, newPos);
		data[newPos++] = e;
		idx[0] = newPos;
		idx[1] = fieldCount + 1;
	}
	

	private byte[] targetByteArray(int newPos, int maxPos) {
		byte[] data = encodedData[selectedRow];
		if ( maxPos > data.length) {
			//grow
			byte[] temp = new byte[maxPos*2];
			System.arraycopy(data, 0, temp, 0, newPos);
			data = encodedData[selectedRow] = temp;
		}
		return data;
	}


	private void recordError(JSONType expectedType, JSONType actualType, String path) {
		logger.warn("{} expected {} but field contained {}",path,expectedType,actualType);
	}

	@Override
	public void stringBegin() {
		
		if (selectedRow>=0) {
			int[] idx = indexData[selectedRow]; 
			int fieldCount = idx[1]; 
			if (fieldCount>0) {	
				//this block only happens when we have more than one value
				JSONAccumRule rule = schema.getMapping(selectedRow).accumRule;
				if (JSONAccumRule.Collect != rule) {
					if (JSONAccumRule.Last == rule) {
						///if we already have data erase it to be replaced
						fieldCount = idx[1] = 0;
						idx[0] = 0;
					} else if (JSONAccumRule.First == rule) {
						///if we already have data skip it
						selectedRow = -1;
						return;
					}
				}
			}
			utf8byteConsumer.activeIndex(selectedRow);
		}
	}

	@Override
	public ByteConsumer stringAccumulator() {
			//NOTE: due to the translation of escape characters the data is provided in 
			//      blocks written to the byte consumer.
			
			//we have 1 backing and 1 mask, we must keep the position, length or values in array
			//grow the storage array if needed, not GC free but will find happy maximum.
						
		    if (selectedRow>=0) {
				return utf8byteConsumer;
			} else {
				return nullByteConsumer;
			}
	

	}

	@Override
	public void stringEnd() {
		if (selectedRow>=0) {
			addTextLength(textLengths, selectedRow, 
					      utf8byteConsumer.length(),
					      indexData[selectedRow][1] );
			indexData[selectedRow][1]++;
		}
		keyExpected = true;
		selectedRow=-1;

	}



}
