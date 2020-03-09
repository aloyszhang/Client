import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;


public class NewApiBookKeeperClientTest {
    private static final String LEDGER_ROOT_PATH = "/ledgers";
    private static final String META_SERVICE_SCHEMA = "zk+null";
    private static final int ENTRY_COUNT = 10;
    private static final String BASE_MESSAGE = "test message ";

    private ClientConfiguration configuration;

    @Before
    public void init () {
        String zkAddress = "localhost:2181";
        String metaServiceUriStr = META_SERVICE_SCHEMA + "://" + zkAddress + LEDGER_ROOT_PATH;
        configuration = new ClientConfiguration();
        configuration.setMetadataServiceUri(metaServiceUriStr);
    }

    @Test
    public void testBuildNewApiBookKeeper() throws Exception {
        BookKeeperClient.createBkClientWithNewApi(configuration);
    }

    @Test
    public void testCreateNewApiLedger() throws Exception {
        BookKeeper bookKeeper = BookKeeperClient.createBkClientWithNewApi(configuration);
        BookKeeperClient.createWiteHandler(bookKeeper);
    }

    @Test
    public void testNewApiWriteEntry() throws Exception {
        BookKeeper bookKeeper = BookKeeperClient.createBkClientWithNewApi(configuration);
        WriteHandle writeHandle = BookKeeperClient.createWiteHandler(bookKeeper);
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
}
