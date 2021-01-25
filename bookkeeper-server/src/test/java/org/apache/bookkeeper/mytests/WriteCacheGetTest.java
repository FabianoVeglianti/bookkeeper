package org.apache.bookkeeper.mytests;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Before;
import org.junit.BeforeClass;
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
public class WriteCacheGetTest {

    private static final ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
    private static final int entrySize = 1024;
    private static final int cacheCapability = 10 * 1024;

    private long ledgerId;
    private long entryId;
    private ByteBuf expected;
    private Class<? extends Exception> expectedException;
    private WriteCache cache;

    @Parameterized.Parameters
    public static Collection<?> getTestParameters(){

        ByteBuf containedSizeBuffer = allocator.buffer(entrySize);
        containedSizeBuffer.writerIndex(containedSizeBuffer.capacity());

        return Arrays.asList(new Object[][]{
                {0, 0, containedSizeBuffer, null},
                {-1, -1, null, IllegalArgumentException.class},
                {0, 0, null, null},
        });
    }

    public WriteCacheGetTest(long ledgerId, long entryId, ByteBuf expected, Class<? extends Exception> expectedException){
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.expected = expected;
        this.expectedException = expectedException;
    }

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Before
    public void setupCache(){
        cache = new WriteCache(allocator, cacheCapability);
        if(expected != null)
            cache.put(ledgerId, entryId, expected);
    }

    @Test
    public void getTest(){

        if(expectedException != null)
            exceptionRule.expect(expectedException);
        ByteBuf actualValue = cache.get(ledgerId, entryId);

        assertEquals(expected, actualValue);
    }


}
