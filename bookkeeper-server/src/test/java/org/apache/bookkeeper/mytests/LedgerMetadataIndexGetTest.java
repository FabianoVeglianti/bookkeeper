package org.apache.bookkeeper.mytests;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorageDataFormats;
import org.apache.bookkeeper.bookie.storage.ldb.LedgerMetadataIndex;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorageDataFormats.LedgerData;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(value=Parameterized.class)
public class LedgerMetadataIndexGetTest {

    private long ledgerId;
    private LedgerData ledgerData; //expectedValue
    private Class<? extends Exception> expectedException;
    private LedgerMetadataIndex ledgerMetadataIndex;



    @Parameterized.Parameters
    public static Collection<?> getTestParameters(){

        LedgerData ledgerData = LedgerData.newBuilder().setExists(true)
                .setFenced(false).setMasterKey(com.google.protobuf.ByteString.copyFromUtf8("fabiano")).build();


        return Arrays.asList(new Object[][]{
                {-1, null, Bookie.NoLedgerException.class},
                {0, ledgerData, null},
        });
    }

    public LedgerMetadataIndexGetTest(long ledgerId, LedgerData ledgerData, Class<? extends Exception> expectedException){
        this.ledgerId = ledgerId;
        this.ledgerData = ledgerData;
        this.expectedException = expectedException;
    }

    @Before
    public void setup() throws IOException {
        LedgerMetadataIndexGetInstance instance = new LedgerMetadataIndexGetInstance();
        this.ledgerMetadataIndex = instance.getInstance(this.ledgerId, this.ledgerData);
    }


    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();


    @Test
    public void getTest() throws IOException{
        if(expectedException !=null)
            exceptionRule.expect(expectedException);
        LedgerData actualValue = ledgerMetadataIndex.get(ledgerId);

        assertEquals(this.ledgerData,actualValue);
    }

}
