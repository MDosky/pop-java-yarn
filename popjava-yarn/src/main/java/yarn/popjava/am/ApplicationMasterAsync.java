package yarn.popjava.am;

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
        while(!master.isReady())
            Thread.sleep(100);
        master.runMainLoop();
    }
}
