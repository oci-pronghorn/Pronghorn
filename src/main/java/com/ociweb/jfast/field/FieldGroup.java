package com.ociweb.jfast.field;

import com.ociweb.jfast.FASTAccept;
import com.ociweb.jfast.FASTProvide;
import com.ociweb.jfast.NullAdjuster;
import com.ociweb.jfast.ReadWriteEntry;
import com.ociweb.jfast.ValueDictionaryEntry;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.read.FieldTypeReadWrite;
import com.ociweb.jfast.read.PMapData;
import com.ociweb.jfast.read.ReadEntry;
import com.ociweb.jfast.write.WriteEntry;

public final class FieldGroup extends Field {

	
	//None of the operators apply to group but it does come in 2 forms group and sequence.
	
	private final int id;
	//old next field
	private final Field[][] matrix; //never changes but is unique to this node
	private final PMapData pmap;
	private final int repeat;

	public FieldGroup(int id, Field[][] matrix, Field next) {
		this.id = id;
		//next assignment
		this.pmap = new PMapData(matrix.length);//need the extra length to make req field processing easier.
		this.matrix = matrix;
		this.repeat=1;
	}

	public final void reader(PrimitiveReader reader, FASTAccept visitor) {
				
		////////////////////////////////////
		//load the full pmap for this group
		////////////////////////////////////
		reader.readStopEncodedBytes(pmap);
		//////////////////////
		//May jump out and process an inline template
		//////////////////////
		
		//TODO: detected template id
		
		
		/////////////////////
		//Process the fields
		/////////////////////
		int m = 0;
		while (m<pmap.size()) {
			matrix[m][pmap.get(m)].reader(reader, visitor);
			m++;
		}
		
		
		//end of reader
		
	}

	public void writer(PrimitiveWriter writer, FASTProvide provider) {
		
		//write group needs nibble chain.
		
		
		
		
		//end of writer
		
	}
	
	public void reset() {
		
		//TODO: must walk matrix!
		
		//valueDictionaryEntry.reset();
		//end of reset
	}

}
