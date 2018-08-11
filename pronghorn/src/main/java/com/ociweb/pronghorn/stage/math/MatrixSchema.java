package com.ociweb.pronghorn.stage.math;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.stage.math.BuildMatrixCompute.MatrixTypes;

public class MatrixSchema<T extends MatrixSchema<T>> extends MessageSchema<T> {

	public final int columnId = 0;
	public final int rowId = 0;
	public final int typeSize; //in ints
	public final MatrixTypes type;
	
	public final int rows;
	public final int columns;
	
	public static final Logger logger = LoggerFactory.getLogger(MatrixSchema.class);
	
	@Override
	public String toString() {
		return "rows:"+rows+" columns:"+columns+" of type "+type;
	}
	
	public MatrixSchema(int rows, int columns, MatrixTypes type) {
		this(rows,columns,type,null);//nothing can use this as the proper schema instead matrix is always sent as rows or columns.
		
	}
	
	protected MatrixSchema(int rows, int columns, MatrixTypes type, FieldReferenceOffsetManager from) {
		super(from);
									
		this.rows = rows;
		this.columns = columns;
		
		this.type = type;
		this.typeSize = type.size();

	}
	
	public FieldReferenceOffsetManager getDecimalFrom() {
		return FieldReferenceOffsetManager.buildSingleNumberBlockFrom(rows*columns, MatrixTypes.Decimals.typeMask, "Matrix"); 				
	}
	
	
	public int getRows() {
		return rows;
	}
	
	public int getColumns() {
		return columns;
	}


}
