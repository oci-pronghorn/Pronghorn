package com.ociweb.json.encode;

import com.ociweb.json.appendable.StringOut;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class JSONRootBoolTests {
    private StringOut out;

    @Before
    public void init() {
        out = new StringOut();
    }

    @Test
    public void testEmpty() {
        JSONRenderer<Object> json = new JSONRenderer<>().empty();
        json.render(out, new Object());
        assertEquals("", out.toString());
    }

    @Test
    public void testRootConstantNull() {
        JSONRenderer<Object> json = new JSONRenderer<>()
                .constantNull();
        json.render(out, new Object());
        assertEquals("null", out.toString());
    }

    @Test
    public void testRootBoolTrue() {
        JSONRenderer<Boolean> json = new JSONRenderer<Boolean>()
                .bool(o -> o);
        json.render(out, true);
        assertEquals("true", out.toString());
    }

    @Test
    public void testRootBoolFalse() {
        JSONRenderer<Boolean> json = new JSONRenderer<Boolean>()
                .bool(o -> o);
        json.render(out, false);
        assertEquals("false", out.toString());
    }

    @Test
    public void testRootBoolNul_lNull() {
        JSONRenderer<Boolean> json = new JSONRenderer<Boolean>()
                .nullableBool((o, v) -> v.visit(o != null ? o : false, o == null));
        json.render(out, null);
        assertEquals("null", out.toString());
    }

    @Test
    public void testRootBoolNull_True() {
        JSONRenderer<Boolean> json = new JSONRenderer<Boolean>()
                .nullableBool((o, v) -> v.visit(o != null ? o : false, o == null));
        json.render(out, true);
        assertEquals("true", out.toString());
    }

    @Test
    public void testRootBoolNull_False() {
        JSONRenderer<Boolean> json = new JSONRenderer<Boolean>()
                .nullableBool((o, v) -> v.visit(o != null ? o : false, o == null));
        json.render(out, false);
        assertEquals("false", out.toString());
    }
}
