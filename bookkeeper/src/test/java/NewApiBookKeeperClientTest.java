import org.apache.bookkeeper.client.api.BKException;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;


public class NewApiBookKeeperClientTest extends BookKeeperClientTest{
    private static final int ENTRY_COUNT = 10;
    private static final String BASE_MESSAGE = "test message ";



    @Test
    public void testBuildNewApiBookKeeper() throws Exception {
        BookKeeperClient.createBkClientWithNewApi(configuration);
    }

    @Test
    public void testCreateNewApiLedger() throws Exception {
        BookKeeper bookKeeper = BookKeeperClient.createBkClientWithNewApi(configuration);
        BookKeeperClient.createWriteHandler(bookKeeper);
    }

    @Test
    public void testNewApiWriteEntry() throws Exception {
        BookKeeper bookKeeper = BookKeeperClient.createBkClientWithNewApi(configuration);
        WriteHandle writeHandle = BookKeeperClient.createWriteHandler(bookKeeper);
        long putRes = -1;
        for ( int i = 0 ; i < ENTRY_COUNT; i ++) {
            if (i % 2 == 0) {
               putRes =  writeHandle.append((BASE_MESSAGE + i).getBytes("utf-8"));
                System.out.println("Put entry sync : " + putRes);
            } else {
                CompletableFuture<Long> addFuture =  writeHandle.appendAsync((BASE_MESSAGE + i).getBytes("utf-8"));
                if (i % 3 == 0) {
                   long entryId = FutureUtils.result(addFuture);
                    System.out.println("Put entry async01 : " + entryId);
                } else if (i % 3 == 1) {
                    addFuture
                            .thenApply(entryId -> {
                                System.out.println("Put entry async02 : " + entryId);
                                return entryId;
                            })
                            .exceptionally(cause -> {
                                System.out.println("Put entry async02 error :" + cause);
                                return -1L;
                            });

                 /*   addFuture.whenCompleteAsync((l,t) -> {
                        if (t != null) {
                            System.out.println("Put entry async02 error :" + t.getCause());
                        } else {
                            System.out.println("Put entry async02: " + l);
                        }
                    });*/
                } else {
                    addFuture.whenComplete(new FutureEventListener<Long>() {
                        @Override
                        public void onSuccess(Long aLong) {
                            System.out.println("Put entry async03 : " + aLong);
                        }

                        @Override
                        public void onFailure(Throwable throwable) {
                            System.out.println("Put entry async03 error :" + throwable.getCause());
                        }
                    });
                }

            }

        }
    }


    @Test
    public void testNewApiReadEntry() throws Exception {
        BookKeeper bookKeeper = BookKeeperClient.createBkClientWithNewApi(configuration);
        WriteHandle writeHandle = BookKeeperClient.createWriteHandler(bookKeeper);
        // add entry
        for (int i = 0; i < ENTRY_COUNT; i ++) {
            writeHandle.appendAsync((BASE_MESSAGE + i).getBytes("utf-8"));
            System.out.println("Append entry :" + i);
        }

        // read entry
        long ledgerId = writeHandle.getId();
        ReadHandle readHandle = BookKeeperClient.createReadHandler(bookKeeper, ledgerId, true);

        // 01
        CompletableFuture<LedgerEntries> readFuture = readHandle.readAsync(0, readHandle.getLastAddConfirmed());
        LedgerEntries ledgerEntries = FutureUtils.result(readFuture);
        Iterator<LedgerEntry> iterator = ledgerEntries.iterator();
        while (iterator.hasNext()) {
            LedgerEntry ledgerEntry = iterator.next();
            System.out.println(" 01 Recv : " + ledgerEntry.getEntryId() + " " + new String(ledgerEntry.getEntryBytes()));
        }

        // 02
        CompletableFuture<LedgerEntries> readFuture02 = readHandle.readAsync(0, readHandle.getLastAddConfirmed());
        readFuture02.thenApply(entry -> {
            List<LedgerEntry> ledgerEntryList = new ArrayList<>();
            Iterator<LedgerEntry> iterator1 = ledgerEntries.iterator();
            while (iterator1.hasNext()) {
                LedgerEntry ledgerEntry = iterator1.next();
                System.out.println(" 02 Recv : " + ledgerEntry.getEntryId() + " " + new String(ledgerEntry.getEntryBytes()));
                ledgerEntryList.add(ledgerEntry);
            }
            return ledgerEntryList;
        }).exceptionally(t -> {
            if (t != null) {
                System.out.println("02 Recv entry error : " + t.getMessage());
            }
            return null;
        });

        // 03
        CompletableFuture<LedgerEntries> readFuture03 = readHandle.readAsync(0, readHandle.getLastAddConfirmed());
        readFuture03.whenComplete(new FutureEventListener<LedgerEntries>() {
            @Override
            public void onSuccess(LedgerEntries ledgerEntries) {
                Iterator<LedgerEntry> iterator1 = ledgerEntries.iterator();
                while (iterator1.hasNext()) {
                    LedgerEntry ledgerEntry = iterator1.next();
                    System.out.println(" 02 Recv : " + ledgerEntry.getEntryId() + " " + new String(ledgerEntry.getEntryBytes()));
                }
            }

            @Override
            public void onFailure(Throwable throwable) {
                System.out.println("02 Recv entry error : " + throwable.getMessage());
            }
        });
    }

    @Test
    public void testNewApiFence() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(2, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r);
            }
        });
        BookKeeper bookKeeper = BookKeeperClient.createBkClientWithNewApi(configuration);
        WriteHandle writeHandle = BookKeeperClient.createWriteHandler(bookKeeper);
        executorService.execute(() -> {
            try {
                for (int i = 0 ; i < 10; i ++) {
                    long entryId = writeHandle.append(BASE_MESSAGE.getBytes("utf-8"));
                    System.out.println("Writer put entry : " + entryId );
                    Thread.sleep(1000);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        Thread.sleep(3000);
        ReadHandle readHandle = BookKeeperClient.createReadHandler(bookKeeper, writeHandle.getId(), true);
        try {
            System.out.println("LedgerId : " + writeHandle.getId() + " LAC : " + readHandle.getLastAddConfirmed());
            LedgerEntries ledgerEntries = readHandle.read(0, readHandle.getLastAddConfirmed());
            for (LedgerEntry ledgerEntry : ledgerEntries) {
                System.out.println("Reader get entry : " + ledgerEntry.getEntryId()
                        + " with content : " + new String(ledgerEntry.getEntryBytes()));
                Thread.sleep(500);
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.out.print(e.getMessage());
        }
    }



}
