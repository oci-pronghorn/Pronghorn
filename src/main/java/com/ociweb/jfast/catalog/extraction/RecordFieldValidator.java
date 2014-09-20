package com.ociweb.jfast.catalog.extraction;

public interface RecordFieldValidator {

	RecordFieldValidator ALL_VALID = new RecordFieldValidator(){

		@Override
		public boolean isValid(int nullCount, int utf8Count, int asciiCount, int firstFieldLength, int firstField) {
			return true;
		}};
	
	boolean isValid(int nullCount, int utf8Count, int asciiCount, int firstFieldLength, int firstField);
	
}
