package popjava.yarn;

import com.beust.jcommander.JCommander;
import popjava.annotation.POPClass;

/**
 * This class implements a simple async app master.
 */
@POPClass(isDistributable = false)
public class ApplicationMasterAsync {

    public static void main(String... args) throws Exception {
        ApplicationMasterPOP master = new ApplicationMasterPOP();
        new JCommander(master, args);
        master.setup();
        master.runMainLoop();
    }
}
