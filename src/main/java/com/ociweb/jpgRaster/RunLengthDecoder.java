package com.ociweb.jpgRaster;

public class RunLengthDecoder {
    public class dynamicArray {
        public int byteArray[];
        private int used;
        public int capacity;

        public dynamicArray(int size) {
            this.byteArray = new int[size];
            this.used = 0;
            this.capacity = size;
        }

        public void insert(int val) {
            if (this.used >= this.capacity) {
                int tempArray[] = new int[2*capacity];
                for (int i = 0; i < capacity; ++i) {
                    tempArray[i] = this.byteArray[i];
                }
                this.byteArray = tempArray;
            }
            this.byteArray[this.used] = val;
            this.used++;
        }
    }

    public static final int INIT_SIZE = 8;


    public int[] decodeRLE(int inputArr[]) {
        dynamicArray arr = new dynamicArray(INIT_SIZE);
        for (int i = 0; i < inputArr.length; ++i) {
            // First value in nibble should be number of zeros in run
            for (int j = 0; j < inputArr[i]; ++j) {
                arr.insert(0); // Insert 0 for Zero RLE
            }
            ++i;
            // Second value in nibble is an actual value
            arr.insert(inputArr[i]);
        }
        return arr.byteArray;
    }
}
