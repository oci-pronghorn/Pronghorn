package com.ociweb.json.encode;

public class BasicObj {
    boolean b = true;
    int i = 9;
    double d = 123.4;
    String s = "fum";
    BasicObj m;

    BasicObj() {
        this.m = null;
    }

    BasicObj(int i) {
        this.i = i;
        this.m = null;
    }

    BasicObj(BasicObj m) {
        this.m = m;
    }
}
