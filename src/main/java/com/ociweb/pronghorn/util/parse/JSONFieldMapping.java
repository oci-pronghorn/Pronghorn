package com.ociweb.pronghorn.util.parse;


import com.ociweb.json.JSONAccumRule;
import com.ociweb.json.JSONType;
import com.ociweb.pronghorn.pipe.ChannelReader;
import com.ociweb.pronghorn.pipe.util.hash.LongHashTable;
import com.ociweb.pronghorn.util.Appendables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JSONFieldMapping {
	
	private static final Logger logger = LoggerFactory.getLogger(JSONFieldMapping.class);
	
	private final JSONFieldSchema schema;
	private String name;
	private Object association;
	
	public final JSONType type;
	
	public JSONAccumRule accumRule;	
	public final boolean isAligned;
	private int[] values; //immutable once established
	private int dimensions; //immutable once established
	public String[] path; //immutable once established
	
	//capture first, last, all
	//align all values
	//
	
	
	public JSONFieldMapping(
					             JSONFieldSchema schema,
					             JSONType type,
					             boolean isAligned) {
	
		//we need all the paths first in order 
		//to ensure that the multiplier works.
		this.schema = schema;
		this.type = type;
		this.isAligned = isAligned;
		this.accumRule = null;//wait to set in setPath
	}

	public JSONFieldMapping(
					             JSONFieldSchema schema,
					             JSONType type,
					             boolean isAligned,
					             JSONAccumRule accumRule) {
	
		//we need all the paths first in order 
		//to ensure that the multiplier works.
		this.schema = schema;
		this.type = type;
		this.isAligned = isAligned;
		this.accumRule = accumRule; 
	}
	
	public String getName() {
		return name; //TODO: store as array of bytes...
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public void setAssociatedObject(Object optionalAssociation) {
		this.association = optionalAssociation;
		
	}
	
	public Object getAssociatedObject() {
		return association;
	}
	
	public void setPath(JSONFieldSchema schema, CharSequence ... path) {
		this.values = new int[path.length];
		int dimCounter = 0;
		for(int x = 0; x<path.length; x++) {			
			if (isArray(path[x])) {
				//TODO: do we want to support 0, 1-2 etc notation? do it here..
				dimCounter++;
				values[x] = -(x+1); //negative values to mark array usages
			} else {
				values[x] = schema.lookupId(path[x]);
			}
			//logger.info("lookupId for {} returned {} ", path[x], values[x]);
		}
		this.dimensions = dimCounter;
		schema.recordMaxPathLength(path.length);
		
		if (null == accumRule) {
			//set default value based on if this field is dimentional or not
			accumRule = (dimCounter==0)? JSONAccumRule.Last : JSONAccumRule.Collect;
		}
		
	}
	
	public int dimensions() {
		return dimensions;
	}
	

	private boolean isArray(CharSequence value) {
		return (value.charAt(0)=='[');
	}

	public static int addHashToTable(int fieldIdx, int dimsIdx,
			LongHashTable lookupFieldTableLocal,
			LongHashTable lookupDimensions,
			int[][] dimUsages,
			JSONFieldMapping jsonFieldMapping) {
		
		long pathHash = 0;//fieldIdx;
		int dimDepth = 0;
		for(int i = 0; i < jsonFieldMapping.values.length; i++) {
			long prev = pathHash*jsonFieldMapping.schema.maxFieldUnits();
			
			assert(jsonFieldMapping.values[i]!=0);
			if (jsonFieldMapping.values[i]>=0) {
				pathHash = prev + jsonFieldMapping.values[i];
				
			} else {
				//when value is less than 0 it is an array so the id,
				//must be moved up above the rest and the value is negative.
				pathHash = prev + (jsonFieldMapping.schema.uniqueFieldsCount()
						          -jsonFieldMapping.values[i]);
								
				int dimIdx;//lookup which fields make use of this array
				if (!LongHashTable.hasItem(lookupDimensions, pathHash)) {
					dimIdx = dimsIdx++;
					LongHashTable.setItem(lookupDimensions, pathHash, dimIdx);
				} else {
					dimIdx = LongHashTable.getItem(lookupDimensions, pathHash);
				}

				dimDepth++; //must increment before it is stored.
				
				//detect if this is the first addition and record the dim depth
				if (dimUsages[dimIdx][0]==0) {
					//first insert so record specific dim depth 
					addToList(dimIdx, dimDepth, dimUsages);	//adds count plus this field
					
					//logger.info("dim dimDepth {} {} len {}", dimIdx, dimDepth, dimUsages[dimIdx][0]);
				} else {
					assert(dimUsages[dimIdx][1] == dimDepth) : "Internal error, dim depth must match";
				}
				
				addToList(dimIdx, fieldIdx, dimUsages);
				//logger.info("dim fieldIdx {} {} {} ", dimIdx, fieldIdx, Arrays.toString(dimUsages[dimIdx]));
				
				//dimDepth++; moved above for testing remove this line
			}
		
		}
		
		if (LongHashTable.hasItem(lookupFieldTableLocal, pathHash)) {
			throw new UnsupportedOperationException("field "+fieldIdx+" conflicts with previous field, each must be unique.");
		}
		
//		if (jsonFieldMapping.groupId>=0) {
//			//record the pathHash and/or fieldIdx as part of the group
//		    //store the fieldIdx to be looked up from the groupId,
//			
//			//what is the row for this groupId??
//			//addToList(row, fieldIdx, target);
//		}
		
		LongHashTable.setItem(lookupFieldTableLocal, pathHash, fieldIdx);
		return dimsIdx;
	}

	//record all the fieldIDs which will are part of the same groupId.
	//record all the fieldIDs which make use of this same dimIdx
	private static void addToList(int rowIdx, int addValue, int[][] targetList) {
		int count = targetList[rowIdx][0];
		
		//not gc free but only done on startup 
		if (count+2 > targetList[rowIdx].length) {
			//must grow since we have no room for len plus old data plus 1
			
			int[] usages = new int[count+2];
			System.arraycopy(targetList[rowIdx], 0, usages, 0, count+1);		
			targetList[rowIdx] = usages;
			
		}
		
		targetList[rowIdx][0] = ++count;
		targetList[rowIdx][count] = addValue;
				
	}


	public <A extends Appendable> A dump(ChannelReader reader, A out) {
		
		switch (type) {
			case TypeBoolean:
				try {
						Appendables.appendValue(out, "Boolean recorded as: ", reader.readByte());
						out.append("\n");
				} catch (IOException ex) {
					throw new RuntimeException(ex);
				}	
				break;
			case TypeDecimal:
				try{
						long m = reader.readPackedLong();
						byte e = reader.readByte();
						
						out.append("Decimal recorded as: ");
						Appendables.appendDecimalValue(out, m, e);
						out.append("\n");
				} catch (IOException ex) {
					throw new RuntimeException(ex);
				}	
				break;
			case TypeInteger:
				try {
					Appendables.appendValue(out, "Integer recorded as: ", reader.readPackedLong());
					out.append("\n");
				} catch (IOException ex) {
					throw new RuntimeException(ex);
				}	
				break;
			case TypeString:
				try {
					int length = reader.readPackedInt();
					
					Appendables.appendValue(out, "String len: ",length);
					if (length>0) {
						out.append(" with body of ");
						reader.readUTFOfLength(length,out);
					}
					out.append("\n");
				} catch (IOException ex) {
					throw new RuntimeException(ex);
				}						   
				
				break;
			default:
				throw new UnsupportedOperationException();
		
		}
		
		return out;
	}

	
}
