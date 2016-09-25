package com.ociweb.pronghorn.stage.math;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.stage.math.BuildMatrixCompute.MatrixTypes;

public class MatrixSchema extends MessageSchema {

	public final int matrixId = 0;
	public final int columnId = 0;
	public final int rowId = 0;
	public final int typeSize; //in ints
	public final MatrixTypes type;
	
	private final int rows;
	private final int columns;
	
	private final FieldReferenceOffsetManager colFrom;
	
	protected MatrixSchema(FieldReferenceOffsetManager from, FieldReferenceOffsetManager colFrom, int rows, int columns, MatrixTypes type) {
		super(from);
		
		this.rows = rows;
		this.columns = columns;
		this.colFrom = colFrom;
		
		this.type = type;
		this.typeSize = type.size();

	}
	
	public FieldReferenceOffsetManager getColumnFrom() {
		return colFrom;
	}
	
	public int getRows() {
		return rows;
	}
	
	public int getColumns() {
		return columns;
	}


}
