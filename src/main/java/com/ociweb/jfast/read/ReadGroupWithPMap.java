package com.ociweb.jfast.read;

import com.ociweb.jfast.ByteConsumer;
import com.ociweb.jfast.FASTAccept;
import com.ociweb.jfast.NullAdjuster;
import com.ociweb.jfast.ValueDictionaryEntry;
import com.ociweb.jfast.field.Field;
import com.ociweb.jfast.primitive.PrimitiveReader;

//session is a group of frames and/or messages
//frame is a group of messages
//message is a group of fields and field groups


//comes in these types fixed, optional, repeating, or optional and repeating

//if entire group is missing then ...

//segment is a group with a pm

//dicionaries allow key lookup of last value for reuse! elsewhere in the tree1!!

//normal group
//pmap
//fields

//repeating group (sequence type?)
//interations int
//			*TOP
//pmap
//fields
//			*LOOP
/*
 * pmap rules
 * mandatory & has constant operator -> none
 * mandatory & no operator           -> none
 * optional  & has constant operator -> one bit
 * optional group field              -> one bit but blocks all members  
 * 
 * need to read/process pmap first, it may be 'outofrange' by the time we get to the 
 * last fields, this is dependent on the buffer size vs the group size.
 * 
 * use bits to pick members? need mask/idx list others are mandatory
 * 
 * 
 */


/*
 * each group preallocates array of bytes for known pmap size.
 * on visit copy map and set remainder to zero.
 * process each byte lookup operations in map.
 * 
 * 
 */
//A presence map if at least one entry in the Group has a default value;
//need ReadPMap class here? or is this really part of group? most groups do have pmaps
//we can keep reference here so we know what do do with the data

//A TemplateID field if the group layout cannot be statically deduced; 
//meaning that the Template ID is not fixed but can vary from message to messag

//A size field if the template specifies so.

//members

//TODO: urgent delete.
public class ReadGroupWithPMap implements ReadEntry {
		
	//TODO: move pmap and matrix into dictionary entry this class can be deleted.
	
	private final int id;
	private final Field[][] matrix; //never changes but is unique to this node
	private final PMapData pmap;
	
	public ReadGroupWithPMap(int id, Field[][] matrix) {
		this.id = id;
		//the extra required fields must remain zero
		this.pmap = new PMapData(matrix.length);//need the extra length to make req field processing easier.
		this.matrix = matrix;
	}
	
	//A template identifier is represented as an Unsigned Integer in the stream. It is a reportable error [ERR R6]
	//if it is overlong.

	public void readLong(PrimitiveReader reader, int id, FASTAccept visitor,
			NullAdjuster nullAdjuster, ValueDictionaryEntry entry) {
		throw new FASTException();
	}

	public void readInt(PrimitiveReader reader, int id, FASTAccept visitor,
			NullAdjuster nullOff, ValueDictionaryEntry entry) {
		throw new FASTException();
	}

	public void readBytes(PrimitiveReader reader, int id, FASTAccept visitor,
			NullAdjuster nullOff, ValueDictionaryEntry entry) {
		throw new FASTException();
	}

	public void readCharsASCII(PrimitiveReader reader, int id,
			FASTAccept visitor, ValueDictionaryEntry entry) {
		throw new FASTException();
	}

	public void readCharsUTF8(PrimitiveReader reader, int id, FASTAccept visitor,
			NullAdjuster nullOff, ValueDictionaryEntry entry) {
		throw new FASTException();
	}

	public void readDecimal(PrimitiveReader reader, int id, FASTAccept visitor,
			NullAdjuster nullOff, ValueDictionaryEntry entry) {
		throw new FASTException();
	}

	public void readGroup(PrimitiveReader reader, int id, FASTAccept visitor,
			ValueDictionaryEntry entry) {
	    ///////////////////////
		//Read in the pmap data
		///////////////////////    TODO: should this also be in dictionary as single state location?
		
		pmap.clear();
		reader.readStopEncodedBytes(pmap);
		/////////////////////
		//Process the fields
		/////////////////////
		int m = 0;
		while (m<pmap.size()) {
			matrix[m][pmap.get(m)].reader(reader, visitor);
			m++;
		}
	}

	public void readUnsignedLong(PrimitiveReader reader, int id,
			FASTAccept visitor, NullAdjuster nullAdjuster,
			ValueDictionaryEntry entry) {
		// TODO Auto-generated method stub
		
	}

	public void readUnsignedInt(PrimitiveReader reader, int id,
			FASTAccept visitor, NullAdjuster nullOff, ValueDictionaryEntry entry) {
		// TODO Auto-generated method stub
		
	}


}
