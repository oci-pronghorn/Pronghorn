package com.ociweb.jfast.field;

public class FieldRouter {

	public static final int INTEGER_UNSIGNED            = 1;
	public static final int INTEGER_UNSIGNED_NULLABLE   = 2;
	public static final int INTEGER_SIGNED              = 3;
	public static final int INTEGER_SIGNED_NULLABLE     = 4;
	
	
	
	public void run(FieldWriterInteger writer, int typeOpId) {
		
		switch(typeOpId) {
			case INTEGER_UNSIGNED:
				
				break;
			case INTEGER_UNSIGNED_NULLABLE:
				break;
			case INTEGER_SIGNED:
			//	writer.writeSignedInteger(value, id);
				break;
			case INTEGER_SIGNED_NULLABLE:
				//writer.writeNullableSignedInteger(value, id);
				break;		
		
		}
		
		
	}
	
	
}
