import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.LedgerHandle;

import java.util.HashMap;

public class LedgerCreateCallback implements AsyncCallback.CreateCallback {
    public static HashMap<Long, LedgerHandle> ledgerMap = new HashMap<Long, LedgerHandle>();
    public void createComplete(int rc, LedgerHandle lh, Object ctx) {
        System.out.println("Create ledger " + lh.getId() + " success.");
        ledgerMap.put(lh.getId(), lh);
    }
}
