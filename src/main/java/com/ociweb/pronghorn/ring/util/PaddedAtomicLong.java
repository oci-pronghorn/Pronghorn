package com.ociweb.pronghorn.ring.util;

import java.util.concurrent.atomic.AtomicLong;

public class PaddedAtomicLong extends AtomicLong {
    public PaddedAtomicLong() {
    }

    public PaddedAtomicLong(final long initialValue) {
        super(initialValue);
    }

    public volatile long padding1, padding2, padding3, padding4;
}
