package org.apache.bookkeeper.mytests;



import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorageDataFormats;
import org.apache.bookkeeper.bookie.storage.ldb.LedgerMetadataIndex;
import org.junit.*;
import org.junit.rules.ExpectedException;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorageDataFormats.LedgerData;

import java.io.IOException;
import java.util.*;


@RunWith(value= Parameterized.class)
public class LedgerMetadataIndexSetTest {

    private long ledgerId;
    private LedgerData ledgerData;
    private Class<? extends Exception> expectedException;
    private boolean isAlreadyThere;
    private LedgerMetadataIndex ledgerMetadataIndex;



    @Parameterized.Parameters
    public static Collection<?> getTestParameters(){

        LedgerData ledgerData = DbLedgerStorageDataFormats.LedgerData.newBuilder().setExists(true)
                .setFenced(false).setMasterKey(com.google.protobuf.ByteString.copyFromUtf8("fabiano")).build();


        return Arrays.asList(new Object[][]{
                {-1, null, NullPointerException.class, false},
                {0, ledgerData, null, false},
                {0, ledgerData, null, true}
        });
    }

    public LedgerMetadataIndexSetTest(long ledgerId, LedgerData ledgerData, Class<? extends Exception> expectedException, boolean isAlreadyThere){
        this.ledgerId = ledgerId;
        this.ledgerData = ledgerData;
        this.expectedException = expectedException;
        this.isAlreadyThere = isAlreadyThere;
    }

    @Before
    public void setup() throws IOException{
        LedgerMetadataIndexGetInstance instance = new LedgerMetadataIndexGetInstance();
        this.ledgerMetadataIndex = instance.getInstance(this.ledgerId, this.ledgerData);
    }


    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void setTest() throws IOException{

        if(expectedException != null)
            exceptionRule.expect(expectedException);
        ledgerMetadataIndex.set(ledgerId, ledgerData);

        if(isAlreadyThere)
            ledgerMetadataIndex.set(ledgerId, ledgerData);

    }


}
