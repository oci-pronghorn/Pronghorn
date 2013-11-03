package com.ociweb.jfast;

public enum NullAdjuster {
	Optional {
		@Override
		public int readAdjust(int value) {
			if (value>0) {
				return value-1;
			} else {
				return value;
			}
		}

		@Override
		public long readAdjust(long value) {
			if (value>0) {
				return value-1;
			} else {
				return value;
			}
		}
		
		@Override
		public int writeAdjust(int value) {
			if (value>=0) {
				return value+1;
			} else {
				return value;
			}
		}

		@Override
		public long writeAdjust(long value) {
			if (value>=0) {
				return value+1;
			} else {
				return value;
			}
		}
	},
	Required {
		@Override
		public int readAdjust(int value) {
			return value;
		}
		
		@Override
		public long readAdjust(long value) {
			return value;
		}

		@Override
		public int writeAdjust(int value) {
			return value;
		}

		@Override
		public long writeAdjust(long value) {
			return value;
		}
	};
	
	public abstract int readAdjust(int value);
	public abstract long readAdjust(long value);
	public abstract int writeAdjust(int value);
	public abstract long writeAdjust(long value);
}
