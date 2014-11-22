package com.ociweb.pronghorn.ring.util;

import java.util.concurrent.atomic.AtomicInteger;

public class PaddedAtomicInteger extends AtomicInteger {
    public PaddedAtomicInteger() {
    }

    public PaddedAtomicInteger(final int initialValue) {
        super(initialValue);
    }

    public volatile long padding1, padding2, padding3, padding4;
}
