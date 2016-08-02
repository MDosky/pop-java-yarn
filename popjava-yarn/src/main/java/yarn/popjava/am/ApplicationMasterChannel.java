package yarn.popjava.am;

import java.text.NumberFormat;
import popjava.PopJava;
import popjava.annotation.POPAsyncConc;
import popjava.annotation.POPClass;
import popjava.annotation.POPObjectDescription;
import popjava.annotation.POPSyncConc;
import popjava.baseobject.POPAccessPoint;

/**
 * This is a simple channel to communicate between the AM and something
 * else. Simple because if it is not we will have missing libraries problem.
 * @author Dosky
 */
@POPClass
public class ApplicationMasterChannel {
    
    private ApplicationMasterPOP master;

    // this constructor shouldn't be used
    @POPObjectDescription(url = "localhost")
    public ApplicationMasterChannel() {
        
        NumberFormat format = NumberFormat.getInstance();
        new Thread(() -> {
            while(true) {
                Runtime runtime = Runtime.getRuntime();
                StringBuilder sb = new StringBuilder();
                long maxMemory = runtime.maxMemory();
                long allocatedMemory = runtime.totalMemory();
                long freeMemory = runtime.freeMemory();

                System.out.format("[Channel] Max: %s, Alloc: %s, Free: %s\n", format.format(maxMemory / 1024), format.format(allocatedMemory / 1024), format.format(freeMemory / 1024));
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ex) {
                }
            }
        }).start();
    }
    
    @POPSyncConc
    public void setMaster(POPAccessPoint master) {
        this.master = PopJava.newActive(ApplicationMasterPOP.class, master);
    }
    
    /**
     * Simple rely to master
     * @param memory
     * @param vcores 
     */
    @POPAsyncConc
    public void requestContainer(int memory, int vcores) {
        master.requestContainer(memory, vcores);
    }
    
    /**
     * Simple rely to master
     * @param task
     * @param jobm 
     */
    @POPAsyncConc
    public void setupServers(String task, String jobm) {
        master.setupServers(task, jobm);
    }
}
