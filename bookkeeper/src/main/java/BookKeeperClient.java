import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;

import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import static org.apache.bookkeeper.client.api.WriteFlag.DEFERRED_SYNC;

public class BookKeeperClient {

    public static byte[] LEDGER_PASSWD;

    static {
        try {
            LEDGER_PASSWD = "password".getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }
    // ledger api start
    public static BookKeeper createBKClient(String zkAddress) {
        try {
            return new BookKeeper(zkAddress);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BKException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static BookKeeper createBKClient(ClientConfiguration configuration) {
        try {
            return new BookKeeper(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BKException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static BookKeeper createBKClient(ClientConfiguration configuration, ZooKeeper zooKeeper) {
        try {
            return new BookKeeper(configuration, zooKeeper);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BKException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static LedgerHandle createLedgerHandler(BookKeeper bookKeeper) {
        try {
            return bookKeeper.createLedger(BookKeeper.DigestType.CRC32, LEDGER_PASSWD);
        } catch (BKException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void createLedgerHandler(BookKeeper bookKeeper, AsyncCallback.CreateCallback callback) {
        bookKeeper.asyncCreateLedger(
                3,
                3,
                BookKeeper.DigestType.MAC,
                LEDGER_PASSWD,
                callback,
                "some context"
        );
    }

    public static void deleteLedger(BookKeeper bookKeeper, long ledgerId) {
        try {
            bookKeeper.deleteLedger(ledgerId);
            System.out.println("Delete  ledger :" + ledgerId);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BKException e) {
            e.printStackTrace();
        }
    }

    public static void deleteLedger(BookKeeper bookKeeper, long ledgerId, AsyncCallback.DeleteCallback callback) {
        bookKeeper.asyncDeleteLedger(ledgerId, callback, null);
        System.out.println("Delete  ledger :" + ledgerId);
    }
    // ledger api end

     // new api start
    public static org.apache.bookkeeper.client.api.BookKeeper createBkClientWithNewApi(ClientConfiguration configuration) throws Exception {
        return org.apache.bookkeeper.client.api.BookKeeper.newBuilder(configuration).build();
    }

    public static WriteHandle createWriteHandler(org.apache.bookkeeper.client.api.BookKeeper bookKeeper) throws Exception {
        return bookKeeper.newCreateLedgerOp()
                .withDigestType(DigestType.CRC32)
                .withPassword(LEDGER_PASSWD)
                .withEnsembleSize(3)
                .withWriteQuorumSize(3)
                .withAckQuorumSize(2)
                // with this flag client just put entry to OS cache
                .withWriteFlags(DEFERRED_SYNC)
                .execute()
                .get();
    }

    public static ReadHandle createReadHandler(org.apache.bookkeeper.client.api.BookKeeper bookKeeper,
                                               long ledgerId, boolean recovery) throws Exception {
        if (recovery) {
            return bookKeeper.newOpenLedgerOp()
                    .withLedgerId(ledgerId)
                    .withPassword(LEDGER_PASSWD)
                    // with Recovery mode, it will fence and seal the ledger and no more entries are allowed append to it.
                    .withRecovery(true)
                    .execute()
                    .get();
        } else {
            return bookKeeper.newOpenLedgerOp()
                    .withLedgerId(ledgerId)
                    .withPassword(LEDGER_PASSWD)
                    .execute()
                    .get();
        }

    }
    // new api end



}
