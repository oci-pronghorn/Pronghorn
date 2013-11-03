package com.ociweb.jfast.read;

import com.ociweb.jfast.Operator;
import com.ociweb.jfast.ValueDictionaryEntry;
import com.ociweb.jfast.field.Field;
import com.ociweb.jfast.field.FieldNoOp;
import com.ociweb.jfast.field.binary.FieldBytes;
import com.ociweb.jfast.field.binary.FieldBytesConstant;
import com.ociweb.jfast.field.binary.FieldBytesNulls;
import com.ociweb.jfast.field.decimal.FieldDecimal;
import com.ociweb.jfast.field.decimal.FieldDecimalConstant;
import com.ociweb.jfast.field.decimal.FieldDecimalNulls;
import com.ociweb.jfast.field.integer.FieldInt;
import com.ociweb.jfast.field.integer.FieldIntConstantMandatory;
import com.ociweb.jfast.field.integer.FieldIntConstantOptional;
import com.ociweb.jfast.field.integer.FieldIntCopy;
import com.ociweb.jfast.field.integer.FieldIntNulls;
import com.ociweb.jfast.field.integer.FieldIntU;
import com.ociweb.jfast.field.integer.FieldIntUConstant;
import com.ociweb.jfast.field.integer.FieldIntUNulls;
import com.ociweb.jfast.field.integer.FieldLong;
import com.ociweb.jfast.field.integer.FieldLongConstant;
import com.ociweb.jfast.field.integer.FieldLongNulls;
import com.ociweb.jfast.field.integer.FieldLongU;
import com.ociweb.jfast.field.integer.FieldLongUConstant;
import com.ociweb.jfast.field.integer.FieldLongUNulls;
import com.ociweb.jfast.field.string.FieldASCII;
import com.ociweb.jfast.field.string.FieldASCIIConstant;
import com.ociweb.jfast.field.string.FieldASCIINulls;
import com.ociweb.jfast.field.string.FieldUTF8;
import com.ociweb.jfast.field.string.FieldUTF8Constant;
import com.ociweb.jfast.field.string.FieldUTF8Nulls;
import com.ociweb.jfast.tree.Necessity;

public enum FieldType implements FieldTypeReadWrite { // rename to field type

	//operation ... compression technique
	//necessity ... frequently immplemented as throw if null vs pass null allong
	//noPMAP ...  only 1 implementation and pmapValue is not used.
	//pmapValue ... two implementations
	//max of 28 Field objects per type
	//x8 types is 224 custom fields managed in this enum.
	
	//static exception checks can be made in here or in the constructors of the fields
	
	//for those fields with pmap in use the zero is the one with the write logic that can decide 1 or 0
	
	Int32 {

		public Field newField(final int id, final ValueDictionaryEntry valueDictionaryEntry, Operator operation,
				               Necessity necessity) {

			
			switch (necessity) {
			case mandatory:
				switch (operation) {
				case None:
					return new FieldInt(id);
				case Constant:
					return new FieldIntConstantMandatory(id, valueDictionaryEntry); //always return constant
				case Copy:
					return new FieldIntCopy(id, valueDictionaryEntry);
				case Default:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Delta://never pmap
					throw new UnsupportedOperationException("Not yet implemented.");
				case Increment:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Tail:
					throw new UnsupportedOperationException("Not yet implemented.");
				default:
					throw new UnsupportedOperationException("Unknown operation:" + operation);

				}
			case optional:
				switch (operation) {
				case None:
					return new FieldIntNulls(id);
				case Constant:
					return new FieldIntConstantOptional(id, valueDictionaryEntry);
					
				case Copy:
					return null;
				case Default:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Delta:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Increment:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Tail:
					throw new UnsupportedOperationException("Not yet implemented.");
				default:
					throw new UnsupportedOperationException("Unknown operation:" + operation);
				}
			default:
				throw new UnsupportedOperationException();
			}
		}
	},
	uInt32 {

		public Field newField(int id, ValueDictionaryEntry valueDictionaryEntry, Operator operator, Necessity necessity) {
			switch (necessity) {
			case mandatory:
				switch (operator) {
				case None:
					return new FieldIntU(id);
				case Constant:
					return new FieldIntUConstant(id, valueDictionaryEntry);
				case Copy:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Default:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Delta:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Increment:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Tail:
					throw new UnsupportedOperationException("Not yet implemented.");
				default:
					throw new UnsupportedOperationException("Unknown operation:" + operator);

				}
			case optional:
				switch (operator) {
				case None:
					return new FieldIntUNulls(id);
				case Constant:
					return new FieldIntUConstant(id, valueDictionaryEntry);
				case Copy:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Default:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Delta:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Increment:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Tail:
					throw new UnsupportedOperationException("Not yet implemented.");
				default:
					throw new UnsupportedOperationException("Unknown operation:" + operator);

				}
			default:
				throw new UnsupportedOperationException();
			}
		}

	},
	Int64 {

		public Field newField(int id, ValueDictionaryEntry valueDictionaryEntry, Operator operator, Necessity necessity) {
			switch (necessity) {
			case mandatory:
				switch (operator) {
				case None:
					return new FieldLong(id);
				case Constant:
					return new FieldLongConstant(id, valueDictionaryEntry);
				case Copy:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Default:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Delta:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Increment:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Tail:
					throw new UnsupportedOperationException("Not yet implemented.");
				default:
					throw new UnsupportedOperationException("Unknown operation:" + operator);

				}
			case optional:
				switch (operator) {
				case None:
					return new FieldLongNulls(id, valueDictionaryEntry);
				case Constant:
					return new FieldLongConstant(id, valueDictionaryEntry);
				case Copy:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Default:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Delta:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Increment:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Tail:
					throw new UnsupportedOperationException("Not yet implemented.");
				default:
					throw new UnsupportedOperationException("Unknown operation:" + operator);

				}
			default:
				throw new UnsupportedOperationException();
			}
		}

	},
	uInt64 {

		public Field newField(int id, ValueDictionaryEntry valueDictionaryEntry, Operator operator, Necessity necessity) {
			switch (necessity) {
			case mandatory:
				switch (operator) {
				case None:
					return new FieldLongU(id);
				case Constant:
					return new FieldLongUConstant(id, valueDictionaryEntry);
				case Copy:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Default:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Delta:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Increment:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Tail:
					throw new UnsupportedOperationException("Not yet implemented.");
				default:
					throw new UnsupportedOperationException("Unknown operation:" + operator);

				}
			case optional:
				switch (operator) {
				case None:
					return new FieldLongUNulls(id);
				case Constant:
					return new FieldLongUConstant(id, valueDictionaryEntry);
				case Copy:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Default:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Delta:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Increment:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Tail:
					throw new UnsupportedOperationException("Not yet implemented.");
				default:
					throw new UnsupportedOperationException("Unknown operation:" + operator);

				}
			default:
				throw new UnsupportedOperationException();
			}
		}

	},
	Decimal {

		public Field newField(int id, ValueDictionaryEntry valueDictionaryEntry, Operator operator, Necessity necessity) {
			switch (necessity) {
			case mandatory:
				switch (operator) {
				case None:
					return new FieldDecimal(id);
				case Constant:
					return new FieldDecimalConstant(id, valueDictionaryEntry);
				case Copy:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Default:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Delta:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Increment:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Tail:
					throw new UnsupportedOperationException("Not yet implemented.");
				default:
					throw new UnsupportedOperationException("Unknown operation:" + operator);

				}
			case optional:
				switch (operator) {
				case None:
					return new FieldDecimalNulls(id);
				case Constant:
					return new FieldDecimalConstant(id, valueDictionaryEntry);
				case Copy:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Default:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Delta:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Increment:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Tail:
					throw new UnsupportedOperationException("Not yet implemented.");
				default:
					throw new UnsupportedOperationException("Unknown operation:" + operator);

				}
			default:
				throw new UnsupportedOperationException();
			}
		}

	},
	Bytes {

		public Field newField(int id, ValueDictionaryEntry valueDictionaryEntry, Operator operator, Necessity necessity) {
			switch (necessity) {
			case mandatory:
				switch (operator) {
				case None:
					return new FieldBytes(id);
				case Constant:
					return new FieldBytesConstant(id, valueDictionaryEntry);
				case Copy:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Default:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Delta:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Increment:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Tail:
					throw new UnsupportedOperationException("Not yet implemented.");
				default:
					throw new UnsupportedOperationException("Unknown operation:" + operator);

				}
			case optional:
				switch (operator) {
				case None:
					return new FieldBytesNulls(id);
				case Constant:
					return new FieldBytesConstant(id, valueDictionaryEntry);
				case Copy:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Default:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Delta:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Increment:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Tail:
					throw new UnsupportedOperationException("Not yet implemented.");
				default:
					throw new UnsupportedOperationException("Unknown operation:" + operator);

				}
			default:
				throw new UnsupportedOperationException();
			}
		}

	},
	CharsASCII {

		public Field newField(int id, ValueDictionaryEntry valueDictionaryEntry, Operator operator, Necessity necessity) {
			switch (necessity) {
			case mandatory:
				switch (operator) {
				case None:
					return new FieldASCII(id, valueDictionaryEntry);
				case Constant:
					return new FieldASCIIConstant(id, valueDictionaryEntry);
				case Copy:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Default:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Delta:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Increment:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Tail:
					throw new UnsupportedOperationException("Not yet implemented.");
				default:
					throw new UnsupportedOperationException("Unknown operation:" + operator);

				}
			case optional:
				switch (operator) {
				case None:
					return new FieldASCIINulls(id, valueDictionaryEntry);
				case Constant:
					return new FieldASCIIConstant(id, valueDictionaryEntry);
				case Copy:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Default:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Delta:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Increment:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Tail:
					throw new UnsupportedOperationException("Not yet implemented.");
				default:
					throw new UnsupportedOperationException("Unknown operation:" + operator);

				}
			default:
				throw new UnsupportedOperationException();
			}
		}

	},
	CharsUTF8 {

		public Field newField(int id, ValueDictionaryEntry valueDictionaryEntry, Operator operator, Necessity necessity) {
			switch (necessity) {
			case mandatory:
				switch (operator) {
				case None:
					return new FieldUTF8(id);
				case Constant:
					return new FieldUTF8Constant(id, valueDictionaryEntry);
				case Copy:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Default:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Delta:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Increment:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Tail:
					throw new UnsupportedOperationException("Not yet implemented.");
				default:
					throw new UnsupportedOperationException("Unknown operation:" + operator);

				}
			case optional:
				switch (operator) {
				case None:
					return new FieldUTF8Nulls(id);
				case Constant:
					return new FieldUTF8Constant(id, valueDictionaryEntry);
				case Copy:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Default:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Delta:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Increment:
					throw new UnsupportedOperationException("Not yet implemented.");
				case Tail:
					throw new UnsupportedOperationException("Not yet implemented.");
				default:
					throw new UnsupportedOperationException("Unknown operation:" + operator);

				}
			default:
				throw new UnsupportedOperationException();
			}
		}

	};

	public static final ValueDictionaryEntry entry = new ValueDictionaryEntry(null); // TODO:delete

}
