//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;

import com.ociweb.jfast.primitive.PrimitiveWriter;

public final class FieldWriterLong {

    // for optional fields it is still in the optional format so
    // zero represents null for those fields.
    public final long[] dictionary;
    public final long[] init;
    public final int INSTANCE_MASK;

    public FieldWriterLong(PrimitiveWriter writer, long[] values, long[] init) {
        assert (values.length < TokenBuilder.MAX_INSTANCE);
        assert (TokenBuilder.isPowerOfTwo(values.length));

        this.INSTANCE_MASK = Math.min(TokenBuilder.MAX_INSTANCE, (values.length - 1));
        this.dictionary = values;
        this.init = init;
    }

}
