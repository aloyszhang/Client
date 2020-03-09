import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

public class BookKeeperClientTest {
    private static final String LEDGER_ROOT_PATH = "/ledgers";
    private static final String META_SERVICE_SCHEMA = "zk+null";
    private static final int ENTRY_COUNT = 10;

    private String zkAddress ;
    private ClientConfiguration configuration;
    private ZooKeeper zooKeeper;
    private LedgerCreateCallback ledgerCreateCallback;
    private LedgerDeleteCallback ledgerDeleteCallback;

    private BookKeeper bookKeeper;



    @Before
    public void init () {
        zkAddress = "localhost:2181";

        String metaServiceUri = META_SERVICE_SCHEMA + "://" + zkAddress + LEDGER_ROOT_PATH;
        configuration = new ClientConfiguration();
        configuration.setMetadataServiceUri(metaServiceUri);

        try {
            zooKeeper =  ZooKeeperClient.newBuilder()
                    .connectString(zkAddress)
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
        }
        bookKeeper = BookKeeperClient.createBKClient(zkAddress);

        ledgerCreateCallback = new LedgerCreateCallback();
        ledgerDeleteCallback = new LedgerDeleteCallback();

        System.out.println(URI.create(metaServiceUri).getScheme());
        System.out.println(URI.create(metaServiceUri).getPath());
        System.out.println(URI.create(metaServiceUri).getAuthority());
    }

    @Test
    public void testBookKeeperCreation () {
        BookKeeperClient.createBKClient(zkAddress);
        BookKeeperClient.createBKClient(configuration);
        BookKeeperClient.createBKClient(new ClientConfiguration(), zooKeeper);
    }

    @Test
    public void testLedgerCreate() throws InterruptedException {
        System.out.println(BookKeeperClient.createLedgerHandler(bookKeeper).getId());
        BookKeeperClient.createLedgerHandler(bookKeeper, ledgerCreateCallback);
        while(true) {
            if (LedgerCreateCallback.ledgerMap.size() > 0) {
                System.out.println(LedgerCreateCallback.ledgerMap);
                break;
            } else {
                System.out.println("No ledger created from async way.");
                Thread.sleep(1000);
            }
        }
    }

    @Test
    public void testLedgerDelete() throws Exception {
        List<Long> ledgerIdList = listLedgers();
        int flag = 0;
        for (long ledgerId : ledgerIdList) {
            if (flag % 2 == 0) {
                BookKeeperClient.deleteLedger(bookKeeper, ledgerId);
            } else {
                BookKeeperClient.deleteLedger(bookKeeper, ledgerId, ledgerDeleteCallback);
            }
            flag ++;
        }
    }

    private List<Long> listLedgers() throws IOException {
        List<Long> ledgerIds = new ArrayList<Long>();
        LedgerManager.LedgerRangeIterator iterator = bookKeeper.getLedgerManager().getLedgerRanges(5000);
        while(iterator.hasNext()) {
            LedgerManager.LedgerRange  range = iterator.next();
            if (range.getLedgers() != null && range.getLedgers().size() > 0) {
                ledgerIds.addAll(range.getLedgers());
            }
        }
        System.out.println("All ledgers is :" + ledgerIds);
        return ledgerIds;
    }

    @Test
    public void testAddEntryAndRead() throws Exception{
        LedgerHandle ledgerHandle = BookKeeperClient.createLedgerHandler(bookKeeper);
        for (int i = 0; i < ENTRY_COUNT; i ++){
            ledgerHandle.addEntry(("test message " + i).getBytes("utf-8"));
            System.out.println("Send message " + i + " success.");
        }
        Enumeration<LedgerEntry> entryEnumeration = ledgerHandle.readEntries(0, ledgerHandle.getLastAddConfirmed());
        while (entryEnumeration.hasMoreElements()) {
            LedgerEntry entry = entryEnumeration.nextElement();
            System.out.println("Read message  " + entry.getEntryId() + " success." );
        }
    }



}
