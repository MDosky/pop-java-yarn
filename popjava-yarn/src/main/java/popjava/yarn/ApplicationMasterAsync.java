package popjava.yarn;

import popjava.annotation.POPClass;

/**
 * This class implements a simple async app master.
 */
@POPClass(isDistributable = false)
public class ApplicationMasterAsync {

    public static void main(String... args) throws Exception {
        ApplicationMasterPOP master = new ApplicationMasterPOP(args);
        master.setup();
        master.runMainLoop();
    }
}
