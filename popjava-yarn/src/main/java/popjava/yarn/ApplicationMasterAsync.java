package popjava.yarn;

import com.beust.jcommander.JCommander;
import popjava.annotation.POPClass;
import popjava.system.POPSystem;

/**
 * This class implements a simple async app master.
 */
@POPClass(isDistributable = false)
public class ApplicationMasterAsync {

    public static void main(String... args) throws Exception {
        args = POPSystem.initialize(args);
        ApplicationMasterPOP master = new ApplicationMasterPOP(args);
        master.setup();
        master.runMainLoop();
        POPSystem.end();
    }
}
