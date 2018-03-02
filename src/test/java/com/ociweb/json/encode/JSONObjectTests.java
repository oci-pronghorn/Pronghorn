package com.ociweb.json.encode;

import com.ociweb.json.appendable.StringBuilderWriter;
import org.junit.Before;
import org.junit.Test;

import java.util.Objects;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class JSONObjectTests {
    private StringBuilderWriter out;

    @Before
    public void init() {
        out = new StringBuilderWriter();
    }

    @Test
    public void testObjectEmpty() {
        JSONRenderer<BasicObj> json = new JSONRenderer<BasicObj>()
                .beginObject().endObject();
        assertTrue(json.isLocked());
        json.render(out, new BasicObj());
        assertEquals("{}", out.toString());
    }

    @Test
    public void testObjectNull_Yes() {
        JSONRenderer<BasicObj> json = new JSONRenderer<BasicObj>()
                .beginNullObject(Objects::isNull).integer("i", o->o.i).endObject();
        assertTrue(json.isLocked());
        json.render(out, null);
        assertEquals("null", out.toString());
    }

    @Test
    public void testObjectNull_No() {
        JSONRenderer<BasicObj> json = new JSONRenderer<BasicObj>()
                .beginNullObject(Objects::isNull).integer("i", o->o.i).endObject();
        assertTrue(json.isLocked());
        json.render(out, new BasicObj());
        assertEquals("{\"i\":9}", out.toString());
    }

    @Test
    public void testObjectPrimitives() {
        JSONRenderer<BasicObj> json = new JSONRenderer<BasicObj>()
                .beginObject()
                    .bool("b", o->o.b)
                    .integer("i", o->o.i)
                    .decimal("d", (o, v) -> v.visit(o.d, 2))
                    .string("s", o->o.s)
                    .beginObject("m")
                    .endObject()
                .endObject();
        assertTrue(json.isLocked());
        json.render(out, new BasicObj(new BasicObj()));
        assertEquals("{\"b\":true,\"i\":9,\"d\":123.40,\"s\":\"fum\",\"m\":{}}", out.toString());
    }

    @Test
    public void testObjectPrimitivesNull_Yes() {
        JSONRenderer<BasicObj> json = new JSONRenderer<BasicObj>()
                .beginObject()
                    .nullableBool("b", (o, v) -> v.visit(o.b, true))
                    .nullableInteger("i", (o, v) -> v.visit(o.i, true))
                    .nullableDecimal("d", (o, v) -> v.visit(o.d, 2, true))
                    .nullableString("s", o->null)
                    .beginNullableObject("m", o->(o.m == null))
                    .endObject()
                .endObject();
        assertTrue(json.isLocked());
        json.render(out, new BasicObj());
        assertEquals("{\"b\":null,\"i\":null,\"d\":null,\"s\":null,\"m\":null}", out.toString());
    }

    @Test
    public void testObjectPrimitivesNull_No() {
        JSONRenderer<BasicObj> json = new JSONRenderer<BasicObj>()
                .beginObject()
                    .nullableBool("b", (o, v) -> v.visit(o.b, false))
                    .nullableInteger("i", (o, v) -> v.visit(o.i, false))
                    .nullableDecimal("d", (o, v) -> v.visit(o.d, 2, false))
                    .nullableString("s", o->o.s)
                    .beginNullableObject("m", o->(o.m == null))
                    .endObject()
                .endObject();
        assertTrue(json.isLocked());
        json.render(out, new BasicObj(new BasicObj()));
        assertEquals("{\"b\":true,\"i\":9,\"d\":123.40,\"s\":\"fum\",\"m\":{}}", out.toString());
    }

}
