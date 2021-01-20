package org.apache.bookkeeper.mytests;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorageDataFormats;
import org.apache.bookkeeper.bookie.storage.ldb.LedgerMetadataIndex;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorageDataFormats.LedgerData;
import org.mockito.internal.matchers.Null;

@RunWith(value = Parameterized.class)
public class LedgerMetadataIndexSetMasterKeyTest {

    private long ledgerId;
    private Class<? extends Exception> expectedException;
    private LedgerMetadataIndex ledgerMetadataIndex;
    private byte[] masterKey;
    private boolean hasAlreadyMasterKey;



    @Parameterized.Parameters
    public static Collection<?> getTestParameters(){

        byte[] nullMasterKey = null;
        byte[] emptyMasterKey = com.google.protobuf.ByteString.EMPTY.toByteArray();
        byte[] nonEmptyMasterKey = com.google.protobuf.ByteString.copyFromUtf8("string1").toByteArray();

        return Arrays.asList(new Object[][]{
                {-1, nullMasterKey, false, NullPointerException.class},
                {0, emptyMasterKey, false, null},
                {0, nonEmptyMasterKey, true, BookieException.BookieIllegalOpException.class}
        });
    }

    public LedgerMetadataIndexSetMasterKeyTest(long ledgerId, byte[] masterKey, boolean hasAlreadyMasterKey, Class<? extends Exception> expectedException){
        this.ledgerId = ledgerId;
        this.masterKey = masterKey;
        this.hasAlreadyMasterKey = hasAlreadyMasterKey;
        this.expectedException = expectedException;
    }

    @Before
    public void setup() throws IOException {
        LedgerMetadataIndexGetInstance instance = new LedgerMetadataIndexGetInstance();
        LedgerData ledgerData = LedgerData.newBuilder().setExists(true)
                .setFenced(false).setMasterKey(com.google.protobuf.ByteString.copyFromUtf8("string0")).build();

        this.ledgerMetadataIndex = instance.getInstance(this.ledgerId, ledgerData);
    }


    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void setMasterKeyTest() throws IOException {
        if(expectedException == NullPointerException.class) {
            exceptionRule.expect(expectedException);
            ledgerMetadataIndex.setMasterKey(ledgerId, masterKey);
        }

        if(hasAlreadyMasterKey){
            //la master key scelta (string2) deve essere diversa dal contenuto del parametro masterKey
            LedgerData newLedgerData = LedgerData.newBuilder().setExists(true)
                    .setFenced(false).setMasterKey(com.google.protobuf.ByteString.copyFromUtf8("string2")).build();
            long newLedgerId = ledgerId + 1; //deve essere diverso da ledgerId
            ledgerMetadataIndex.set(newLedgerId, newLedgerData);
            ledgerMetadataIndex.flush();
            System.out.println(expectedException);
            if(expectedException == BookieException.BookieIllegalOpException.class)
                exceptionRule.expect(IOException.class);
            ledgerMetadataIndex.setMasterKey(newLedgerId, masterKey);
        } else {
            LedgerData newLedgerData = LedgerData.newBuilder().setExists(true)
                    .setFenced(false).setMasterKey(com.google.protobuf.ByteString.EMPTY).build();
            long newLedgerId = ledgerId + 1; //deve essere diverso da ledgerId
            ledgerMetadataIndex.set(newLedgerId, newLedgerData);
            ledgerMetadataIndex.flush();

            ledgerMetadataIndex.setMasterKey(newLedgerId, masterKey);
        }

    }

}
