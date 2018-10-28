package com.ociweb.json.encode;

import org.junit.Before;
import org.junit.Test;

import com.ociweb.pronghorn.util.StringBuilderWriter;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.channels.Pipe;

class Bucket {
    boolean b1;
    int i1;
    double d1 = 123.4;
    String s1 = "bob";
    int[] a1 = new int[] { 9, 8, 7, 6, 5, 4, 3, 2, 1, 0 };
    int[] a2 = null;

    Pale nm = null;
    Pale m = new Pale();
}

class Pale {
    boolean b2;
    int i2;
    double d2;
    String s2 = null;
}

public class JSONBucketTests {
    private StringBuilderWriter out;

    @Before
    public void init() {
        out = new StringBuilderWriter();
    }

    @Test
    public void testJson() {

        final JSONRenderer<Bucket> json = new JSONRenderer<Bucket>(new JSONKeywordsPretty())
            .startObject()
                .bool("b", o -> o.b1)
                .integer("i", o -> o.i1)
                .decimal("d", 2, o->o.d1)
                .string("s", (o,t) -> t.append(o.s1))
                .array("a", (o,i) -> i < o.a1.length? o : null)
                    .integer((o, i) -> o.a1[i])
                .array("a2", o -> o.a2,(o,i) -> i < o.length? o : null)
                    .constantNull()
                .startObject("nm", o -> o.nm)
                    .nullableBool("b", o->true, o->o.b2)
                    .nullableInteger("i", o->true, o->o.i2)
                    .nullableDecimal("d", 2, o->true, o->0)
                    .nullableString("s", (o,t) -> t.append(o.s2))
                .endObject()
                .startObject("m", o->o.m)
                    .nullableBool("b", o->true, o->o.b2)
                    .nullableInteger("i", o->true, o->o.i2)
                    .nullableDecimal("d", 2, o->true, o->o.d2)
                    .nullableString("s", (o,t) -> t.append(o.s2))
                .endObject()
            .endObject();
        assertTrue(json.isLocked());

        json.render(out, new Bucket());
        assertEquals(
                "{\n" +
                "\t\"b\": false,\n" +
                "\t\"i\": 0,\n" +
                "\t\"d\": 123.40,\n" +
                "\t\"s\": \"bob\",\n" +
                "\t\"a\": [\n" +
                "\t\t9,\n" +
                "\t\t8,\n" +
                "\t\t7,\n" +
                "\t\t6,\n" +
                "\t\t5,\n" +
                "\t\t4,\n" +
                "\t\t3,\n" +
                "\t\t2,\n" +
                "\t\t1,\n" +
                "\t\t0\n" +
                "\t],\n" +
                "\t\"a2\": null,\n" +
                "\t\"nm\": null,\n" +
                "\t\"m\": {\n" +
                "\t\t\"b\": null,\n" +
                "\t\t\"i\": null,\n" +
                "\t\t\"d\": null,\n" +
                "\t\t\"s\": null\n" +
                "\t}\n" +
                "}\n",
                out.toString());
    }
}