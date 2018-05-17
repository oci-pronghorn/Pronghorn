package com.ociweb.pronghorn.util;

public interface AppendableByteWriter<T extends AppendableByteWriter<T>> extends PHAppendable<T>, ByteWriter {
}

