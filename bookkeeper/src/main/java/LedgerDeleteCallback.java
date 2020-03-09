import org.apache.bookkeeper.client.AsyncCallback;

public class LedgerDeleteCallback implements  AsyncCallback.DeleteCallback {
    public void deleteComplete(int rc, Object ctx) {
        System.out.println("Delete ledger result coe : " + rc + " control object :  " + ctx);
    }
}
