import org.apache.bookkeeper.client.*;
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
import java.util.Random;
import java.util.regex.Pattern;

public class BookKeeperClientTest {
    private static final String LEDGER_ROOT_PATH = "/ledgers";
    private static final String META_SERVICE_SCHEMA = "zk+null";
    private static final int ENTRY_COUNT = 10;

    private String zkAddress ;
    ClientConfiguration configuration;
    private ZooKeeper zooKeeper;
    private LedgerCreateCallback ledgerCreateCallback;
    private LedgerDeleteCallback ledgerDeleteCallback;

    private BookKeeper bookKeeper;



    //@Before
    public void init () {
        zkAddress = "localhost:2181/bookkeeper";

        String metaServiceUri = META_SERVICE_SCHEMA + "://" + zkAddress;
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
    public void testRackAwarePlacementPolicy() throws Exception {
        // 1. build zkc
        zkAddress = "localhost:2181/bookkeeper";

        String metaServiceUri = META_SERVICE_SCHEMA + "://" + zkAddress;
        configuration = new ClientConfiguration();
        configuration.setMetadataServiceUri(metaServiceUri);
        configuration.setEnsemblePlacementPolicy(RackawareEnsemblePlacementPolicy.class);
        ZooKeeper zooKeeper = null;

        try {
            zooKeeper =  ZooKeeperClient.newBuilder()
                    .connectString(zkAddress)
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        // 2. build bkc
        BookKeeper bookKeeper = new BookKeeper(configuration, zooKeeper);

        // 3. create ledger
        BookKeeperClient.createLedgerHandler(bookKeeper, ledgerCreateCallback);
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

    List<Long> listLedgers() throws IOException {
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
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < ENTRY_COUNT; i ++){
                    try {
                        ledgerHandle.addEntry(("test message " + i).getBytes("utf-8"));
                        Thread.sleep(1000);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    System.out.println("Send message " + i + " success.");
                }
            }
        });
        t.start();
        Thread.sleep(3000);
        Enumeration<LedgerEntry> entryEnumeration = ledgerHandle.readEntries(0, ledgerHandle.getLastAddConfirmed());
        while (entryEnumeration.hasMoreElements()) {
            LedgerEntry entry = entryEnumeration.nextElement();
            System.out.println("Read message  " + entry.getEntryId() + " success." );
        }
        Thread.sleep(3000);
        Enumeration<LedgerEntry> entryEnumeration01 = ledgerHandle.readEntries(0, ledgerHandle.getLastAddConfirmed());
        while (entryEnumeration01.hasMoreElements()) {
            LedgerEntry entry = entryEnumeration01.nextElement();
            System.out.println("Read message  " + entry.getEntryId() + " success." );
        }
        t.join();
    }

    @Test
    public void testWriteExistedLedger() throws Exception {
        List<Long> ledgerIdList = listLedgers();

        List<Long> successAddList = new ArrayList<>();
        for (long ledgerId : ledgerIdList){
            System.out.println("#### Ledger id : " + ledgerId);
            LedgerHandle handle = bookKeeper.openLedger(ledgerId, BookKeeper.DigestType.CRC32, BookKeeperClient.LEDGER_PASSWD);
            System.out.println("01 LAC : " + handle.getLastAddConfirmed());
            if (handle.getLedgerMetadata().isClosed()) {
                System.out.println("Ledger has been closed. LedgerId : " + handle.getId());
                continue;
            }
            try{
                handle.append("New message".getBytes());
                System.out.println("Success add message for ledger : " + ledgerId);
                successAddList.addAll(ledgerIdList);
            } catch (BKException.BKLedgerClosedException e) {
                System.out.println("Ledger : " + ledgerId + " has been closed.");
            }

        }
        System.out.println("Success ledger list : " + successAddList);
    }
    @Test
    public void testPolling() throws Exception{
        LedgerHandle ledgerHandle = BookKeeperClient.createLedgerHandler(bookKeeper);
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < ENTRY_COUNT * 10; i ++){
                    try {
                        ledgerHandle.addEntry(("test message " + i).getBytes("utf-8"));
                        Thread.sleep(1000);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    System.out.println("Send message " + i + " success.");
                }
            }
        });
        t.start();


        long startIndex = 0;
        long endIndex  = 0;
        int batch = 4;
        while (!ledgerHandle.isClosed() ||  endIndex <= ledgerHandle.getLastAddConfirmed()){
            long lac = ledgerHandle.getLastAddConfirmed();
            endIndex = Math.min(lac, startIndex + batch - 1);
            if (startIndex > lac ) {
                System.out.println("01 StartIndex : " + startIndex +  " ,EndIndex : " + endIndex + " , lac : " + lac);
                Thread.sleep(1000);
                continue;
            }
            System.out.println("02 StartIndex : " + startIndex +  " ,EndIndex : " + endIndex + " , lac : " + lac);
            Enumeration<LedgerEntry> entryEnumeration = ledgerHandle.readEntries(startIndex, endIndex);
            while (entryEnumeration.hasMoreElements()) {
                LedgerEntry entry = entryEnumeration.nextElement();
                System.out.println("Read message  " + entry.getEntryId() + " success." );
            }
            startIndex = endIndex + 1;
        }


        t.join();
    }


    @Test
    public void testAddEntry() throws Exception{
        LedgerHandle ledgerHandle = BookKeeperClient.createLedgerHandler(bookKeeper);
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                long i = 0 ;
                while(true){
                    try {
                        ledgerHandle.asyncAddEntry((getTestMessageStr(10000)).getBytes("utf-8"), new AsyncCallback.AddCallback() {
                            @Override
                            public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                                System.out.println("Add " + entryId + " success");
                            }
                        }, null);
                        if (i++ > 1000) {
                            break;
                        }
                       // Thread.sleep(20);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        t.start();
        t.join();
    }

    private String getTestMessageStr(int length) {
        String all = "abcdefghijklmnopqrstuvwxyz";
        Random random = new Random(26);
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < length; i ++) {
            sb.append(all.charAt(Math.abs(random.nextInt()) % 26));
        }
        return sb.toString();
    }
    @Test
    public void testBuildMessage () {
        System.out.println(getTestMessageStr(100));
    }

    // 判断一个IP是不是合法
    public static boolean isIp(String  str){
        if(str == null || "".equalsIgnoreCase(str)){
            return false;
        }
        final Pattern pattern = Pattern.compile("(((25[0-5]|2[0-4]\\d|1\\d{2}|[1-9]?\\d)\\.){3}(25[0-5]|2[0-4]\\d|1\\d{2}|[1-9]?\\d);)*(((25[0-5]|2[0-4]\\d|1\\d{2}|[1-9]?\\d)\\.){3}(25[0-5]|2[0-4]\\d|1\\d{2}|[1-9]?\\d))$");//全量匹配
        //模糊匹配，*  *.*  *.*.*    *.*.*.*   *.*.*.*.*
        final Pattern allWildcards = Pattern.compile("^(\\*(\\.\\*)*)$");
        //前缀匹配，xx.*   xx.xx.*  xx.xx.xx.*   x.*;y.*
        final Pattern trailingWildcardsIp4 = Pattern.compile
                (
                        "(((((25[0-5]|2[0-4]\\d|1\\d{2}|[1-9]?\\d)\\.){1,3})(\\*)(\\.\\*)*);)*((((25[0-5]|2[0-4]\\d|1\\d{2}|[1-9]?\\d)\\.){1,3})(\\*)(\\.\\*)*)$"
                ); // "blah.*;a.*", "blah.*.*", etc.
        if(pattern.matcher(str).matches()){
            return true;
        }
        if(allWildcards.matcher(str).matches()){
            return true;
        }
        if(trailingWildcardsIp4.matcher(str).matches()){
            return true;
        }
        return false;
    }

    @Test
    public void ipCheck() {
        String ipList = "10.128.19.*;10.128.13.*;192.168.*.*;192.*";
        boolean isIp = true;
        for(String ip : ipList.split(";")) {
            if (!isIp(ip)) {
                isIp = false;
            }
        }
        System.out.println(isIp);
        System.out.println(isIp(ipList));

    }
}
