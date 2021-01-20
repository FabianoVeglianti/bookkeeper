package org.apache.bookkeeper.mytests;

import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorageDataFormats;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorage;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorageFactory;
import org.apache.bookkeeper.bookie.storage.ldb.LedgerMetadataIndex;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.mockito.Mock;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorageDataFormats.LedgerData;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LedgerMetadataIndexGetInstance {
    //I mock  sono usati nel costruttore di ledgerMetadataIndex
    @Mock
    private KeyValueStorageFactory storageFactory = mock(KeyValueStorageFactory.class);
    @Mock
    private KeyValueStorage keyValueStorage = mock(KeyValueStorage.class);
    @Mock
    private KeyValueStorage.CloseableIterator closeableIterator = mock(KeyValueStorage.CloseableIterator.class);

    private Iterator<Map.Entry<byte[], byte[]>> iterator;



    public LedgerMetadataIndex getInstance(long ledgerId, LedgerData ledgerData) throws IOException {
        //Instaziazione di LedgerMetadataIndex

        //#1
        ServerConfiguration conf = new ServerConfiguration();

        //#2
        HashMap<byte[],byte[]> ledgers = new HashMap<>();
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(ledgerId);

        if(ledgerData != null)
            ledgers.put(buffer.array(), ledgerData.toByteArray());

        when(this.storageFactory.newKeyValueStorage(any(), any(), any())).thenReturn(this.keyValueStorage);

        when(this.keyValueStorage.iterator()).then(invocationOnMock -> {
            this.iterator = ledgers.entrySet().iterator();
            return this.closeableIterator;
        });

        when(this.closeableIterator.hasNext()).then(invocationOnMock -> this.iterator.hasNext());
        when(this.closeableIterator.next()).then(invocationOnMock -> this.iterator.next());

        //#3
        String basePath = "fabiano";

        //#4
        NullStatsLogger logger = new NullStatsLogger();

        return new LedgerMetadataIndex(conf, this.storageFactory, basePath, logger);
    }

}
