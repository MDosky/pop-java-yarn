package yarn.popjava;

import popjava.PopJava;
import popjava.annotation.POPAsyncConc;
import popjava.annotation.POPClass;
import popjava.annotation.POPObjectDescription;

/**
 * This class implements a simple async app master.
 */
@POPClass(isDistributable = false)
public class ApplicationMasterAsync {

    public static void main(String[] args) throws InterruptedException {
        ApplicationMasterPOP master = new ApplicationMasterPOP(args);
        master.setup();
        // wait for Central Servers to be up
        Thread.sleep(10000);
        master.runMainLoop();
    }
}
