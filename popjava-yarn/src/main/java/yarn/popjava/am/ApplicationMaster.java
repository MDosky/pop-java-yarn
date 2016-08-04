package yarn.popjava.am;

import popjava.annotation.POPClass;

/**
 * This class simply start a POP version of an Async App Master
 * This ParObject are heavy, the Hadoop classpath is full of libraries
 * so we try to instantiate the minor numer of object possible.
 * @author Dosky
 */
@POPClass(isDistributable = false)
public class ApplicationMaster {

    public static void main(String[] args) throws InterruptedException {
        ApplicationMasterPOP master = new ApplicationMasterPOP(args);
        // setup app master
        master.setup();
        // wait for it to be ready
        while(!master.isReady())
            Thread.sleep(100);
        master.runMainLoop();
    }
}
