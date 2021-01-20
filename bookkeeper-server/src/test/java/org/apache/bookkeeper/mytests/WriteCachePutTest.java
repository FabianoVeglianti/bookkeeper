package org.apache.bookkeeper.mytests;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.apache.bookkeeper.bookie.storage.ldb.WriteCache;
import org.junit.rules.ExpectedException;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;


@RunWith(value= Parameterized.class)
public class WriteCachePutTest {

    private static final ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
    private static final int entrySize = 1024;
    private static final int cacheCapability = 10 * 1024;

    private long ledgerId;
    private long entryId;
    private ByteBuf entry;
    private boolean expected;
    private Class<? extends Exception> expectedException;

    @Parameterized.Parameters
    public static Collection<?> getTestParameters(){

        ByteBuf containedSizeBuffer = allocator.buffer(entrySize);
        containedSizeBuffer.writerIndex(containedSizeBuffer.capacity());
        ByteBuf oversizeBuffer = allocator.buffer(11*entrySize);
        oversizeBuffer.writerIndex(oversizeBuffer.capacity());

        return Arrays.asList(new Object[][]{
                {0, -1, null, false, NullPointerException.class},
                {-1, 0, containedSizeBuffer, false, IllegalArgumentException.class},
                {0, 0, oversizeBuffer, false, null}
        });
    }

    public WriteCachePutTest(long ledgerId, long entryId, ByteBuf entry, boolean expected, Class<? extends Exception> expectedException){
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.entry = entry;
        this.expected = expected;
        this.expectedException = expectedException;
    }

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void putTest(){
        WriteCache cache = new WriteCache(allocator, cacheCapability);
        if(expectedException != null)
            exceptionRule.expect(expectedException);
        boolean actualValue = cache.put(ledgerId, entryId, entry);

        System.out.println(expected + " " + actualValue);
        assertEquals(expected, actualValue);
    }

}
