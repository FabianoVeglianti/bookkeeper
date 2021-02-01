package org.apache.bookkeeper.mytests;

import com.google.protobuf.ByteString;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.storage.ldb.LedgerMetadataIndex;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorageDataFormats.LedgerData;

@RunWith(value = Parameterized.class)
public class LedgerMetadataIndexSetMasterKeyTest {

    private long ledgerId;
    private Class<? extends Exception> expectedException;
    private LedgerMetadataIndex ledgerMetadataIndex;
    private byte[] masterKey;
    private boolean hasAlreadyMasterKey;
    private boolean testNoMetadataExists;

    private boolean areKeysDifferent;
    private boolean isNewKeyNotZero;


    @Parameterized.Parameters
    public static Collection<?> getTestParameters(){

        byte[] nullMasterKey = null;
        byte[] emptyMasterKey = com.google.protobuf.ByteString.EMPTY.toByteArray();
        byte[] nonEmptyMasterKey = com.google.protobuf.ByteString.copyFromUtf8("string1").toByteArray();

        return Arrays.asList(new Object[][]{
                {-1, nullMasterKey, false, false, NullPointerException.class, true, true},
                {0, emptyMasterKey, false, false, null, true, true},
                {0, nonEmptyMasterKey, true, false, BookieException.BookieIllegalOpException.class, true, true},
                {0, nonEmptyMasterKey, false, true, null, true, true},
                //tests per eseguire tutti i rami nel caso in cui ci sia una password già settata e non vuota
                {0, nonEmptyMasterKey, true, false, null, false, true},
                {0, nonEmptyMasterKey, true, false, null, true, false}
        });
    }

    public LedgerMetadataIndexSetMasterKeyTest(long ledgerId, byte[] masterKey, boolean hasAlreadyMasterKey, boolean testNoMetadataExists, Class<? extends Exception> expectedException, boolean areKeysDifferent, boolean isNewKeyNotZero){
        this.ledgerId = ledgerId;
        this.masterKey = masterKey;
        this.hasAlreadyMasterKey = hasAlreadyMasterKey;
        this.testNoMetadataExists = testNoMetadataExists;
        this.expectedException = expectedException;
        this.areKeysDifferent = areKeysDifferent;
        this.isNewKeyNotZero = isNewKeyNotZero;
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

    private void addExistsNotFencedLedger(long ledgerId, ByteString masterKey) throws IOException{
        LedgerData newLedgerData = LedgerData.newBuilder().setExists(true)
                .setFenced(false).setMasterKey(masterKey).build();
        ledgerMetadataIndex.set(ledgerId, newLedgerData);
        ledgerMetadataIndex.flush();
    }

    @Test
    public void setMasterKeyTest() throws IOException {
        if(expectedException == NullPointerException.class) {
            exceptionRule.expect(expectedException);
            ledgerMetadataIndex.setMasterKey(ledgerId, masterKey);
        }

        if(testNoMetadataExists) {
            long newLedgerId = this.ledgerId + 1; //deve essere diverso da ledgerId
            ledgerMetadataIndex.setMasterKey(newLedgerId, masterKey);
        } else {
            if (hasAlreadyMasterKey) {
                long newLedgerId = ledgerId + 1; //deve essere diverso da ledgerId
                if(areKeysDifferent && isNewKeyNotZero) {
                    System.out.println("x");
                    ByteString newMasterKey = com.google.protobuf.ByteString.copyFromUtf8("string2");
                    addExistsNotFencedLedger(newLedgerId, newMasterKey);
                    if (expectedException == BookieException.BookieIllegalOpException.class)
                        exceptionRule.expect(IOException.class);
                    ledgerMetadataIndex.setMasterKey(newLedgerId, masterKey);

                } else if(areKeysDifferent && !isNewKeyNotZero){

                    ByteString existsMasterKey = com.google.protobuf.ByteString.copyFromUtf8("string2");
                    addExistsNotFencedLedger(newLedgerId, existsMasterKey);
                    ByteString newKeyZeros = com.google.protobuf.ByteString.EMPTY;
                    ledgerMetadataIndex.setMasterKey(newLedgerId,newKeyZeros.toByteArray());

                } else if(!areKeysDifferent && isNewKeyNotZero){
                    System.out.println("y");
                    ByteString existsMasterKey = com.google.protobuf.ByteString.copyFromUtf8("string2");
                    addExistsNotFencedLedger(newLedgerId, existsMasterKey);
                    ledgerMetadataIndex.setMasterKey(newLedgerId,existsMasterKey.toByteArray());

                } //il caso !areKeysDifferent && !isNewKeyNotZero non può verificarsi
            } else {
                //storedMasterKey is all zeros (empty)
                LedgerData newLedgerData = LedgerData.newBuilder().setExists(true)
                        .setFenced(false).setMasterKey(com.google.protobuf.ByteString.EMPTY).build();
                long newLedgerId = ledgerId + 1; //deve essere diverso da ledgerId
                ledgerMetadataIndex.set(newLedgerId, newLedgerData);
                ledgerMetadataIndex.flush();

                ledgerMetadataIndex.setMasterKey(newLedgerId, masterKey);
            }
        }
    }

}
