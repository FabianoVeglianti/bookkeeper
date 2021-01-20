package org.apache.bookkeeper.mytests;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorageDataFormats;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorageFactory;
import org.apache.bookkeeper.bookie.storage.ldb.LedgerMetadataIndex;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorageDataFormats.LedgerData;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(value = Parameterized.class)
public class LedgerMetadataIndexSetFencedTest
{
    private long ledgerId;
    private Class<? extends Exception> expectedException;

    private boolean expectedValue;
    private boolean ledgerDeleted;
    private LedgerMetadataIndexGetInstance ledgerMetadataIndexGetInstance;
    private boolean isAlreadyFenced;

    @Mock
    private LedgerMetadataIndex ledgerMetadataIndex;


    @Parameterized.Parameters
    public static Collection<?> getTestParameters(){

        LedgerData ledgerData = LedgerData.newBuilder().setExists(true)
                .setFenced(false).setMasterKey(com.google.protobuf.ByteString.copyFromUtf8("fabiano")).build();


        return Arrays.asList(new Object[][]{
                {0, false, false, Bookie.NoLedgerException.class, false},
                {0, true, false, null, false},
                {0, true, true, null, false},
                {0, false, false, null, true}
        });
    }

    public LedgerMetadataIndexSetFencedTest(long ledgerId, boolean expectedValue, boolean ledgerDeleted, Class<? extends Exception> expectedException, boolean isAlreadyFenced){
        this.ledgerId = ledgerId;
        this.expectedValue = expectedValue;
        this.ledgerDeleted = ledgerDeleted;
        this.expectedException = expectedException;
        this.isAlreadyFenced = isAlreadyFenced;
    }

    @Before
    public void setup() throws IOException {
        ledgerMetadataIndexGetInstance = new LedgerMetadataIndexGetInstance();
        LedgerData ledgerData = LedgerData.newBuilder().setExists(true)
                .setFenced(false).setMasterKey(com.google.protobuf.ByteString.copyFromUtf8("fabiano")).build();
        this.ledgerMetadataIndex = ledgerMetadataIndexGetInstance.getInstance(this.ledgerId, ledgerData);
    }


    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void setFencedTest() throws IOException {


        if(expectedException != null) {
            exceptionRule.expect(expectedException);
            if(expectedException == Bookie.NoLedgerException.class)
                ledgerMetadataIndex.setFenced(1); //questo parametro deve essere diverso da this.ledgerId

        }
        boolean actualValue;
        if(ledgerDeleted) {
            when(ledgerMetadataIndex.get(ledgerId)).thenAnswer(new Answer<LedgerData>() {
                @Override
                public LedgerData answer(InvocationOnMock invocation) throws Throwable {
                    LedgerData ledgerData = ledgerMetadataIndex.get(ledgerId);
                    ledgerMetadataIndex.delete(ledgerId);
                    ledgerMetadataIndex.flush();
                    return ledgerData;
                }
            });

            actualValue = ledgerMetadataIndex.setFenced(ledgerId);
        } else {
            actualValue = ledgerMetadataIndex.setFenced(ledgerId);
        }
        if(isAlreadyFenced){
            actualValue = ledgerMetadataIndex.setFenced(ledgerId);
        }

        assertEquals(expectedValue, actualValue);
    }
}
