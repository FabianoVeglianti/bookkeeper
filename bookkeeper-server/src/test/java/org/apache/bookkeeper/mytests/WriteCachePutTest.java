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
    private static final int maxSegmentSize = 2*1024;

    private long ledgerId;
    private long entryId;
    private ByteBuf entry;
    private boolean expected;
    private Class<? extends Exception> expectedException;
    public boolean isEntryAtEndSegment;

    @Parameterized.Parameters
    public static Collection<?> getTestParameters(){

        ByteBuf containedSizeBuffer = allocator.buffer(entrySize);
        containedSizeBuffer.writerIndex(containedSizeBuffer.capacity());
        ByteBuf oversizeBuffer = allocator.buffer(11*entrySize);
        oversizeBuffer.writerIndex(oversizeBuffer.capacity());


        return Arrays.asList(new Object[][]{
                {0, -1, null, false, NullPointerException.class, false},
                {-1, 0, containedSizeBuffer, false, IllegalArgumentException.class, false},
                {0, 0, oversizeBuffer, false, null, false},
                {0, 0, containedSizeBuffer, false, null, true},
                {0, 0, containedSizeBuffer, true, null, false}
        });
    }

    public WriteCachePutTest(long ledgerId, long entryId, ByteBuf entry, boolean expected, Class<? extends Exception> expectedException, boolean isEntryAtEndSegment){
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.entry = entry;
        this.expected = expected;
        this.expectedException = expectedException;
        this.isEntryAtEndSegment = isEntryAtEndSegment;
    }

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void putTest(){
        if(!isEntryAtEndSegment) {
            WriteCache cache = new WriteCache(allocator, cacheCapability);
            if (expectedException != null)
                exceptionRule.expect(expectedException);
            boolean actualValue = cache.put(ledgerId, entryId, entry);

            assertEquals(expected, actualValue);
        } else {
            WriteCache cache = new WriteCache(allocator, cacheCapability, maxSegmentSize);
            ByteBuf newEntry = allocator.buffer(maxSegmentSize+1);
            newEntry.writerIndex(newEntry.capacity());
            boolean actualValue = cache.put(ledgerId, entryId, newEntry);
            assertEquals(expected, actualValue);
        }
    }



}
