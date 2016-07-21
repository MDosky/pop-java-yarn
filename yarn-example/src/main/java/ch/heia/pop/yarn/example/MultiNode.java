package ch.heia.pop.yarn.example;

import popjava.annotation.POPClass;
import popjava.annotation.POPSyncSeq;

/**
 *
 * @author Dosky
 */
@POPClass(isDistributable = false)
public class MultiNode {

    public static void main(String[] args) {
        RemoteNode rn = new RemoteNode(10);
        int res = rn.doCreate();
        System.out.println(res);
    }
    
    @POPClass
    public static class RemoteNode {

        private final int remaining;

        public RemoteNode() {
            this(0);
        }
        
        public RemoteNode(int r) {
            remaining = r;
        }
         
        @POPSyncSeq
        public int doCreate() {
            if(remaining <= 0)
                return 0;
            RemoteNode rn = new RemoteNode(remaining - 1);
            return remaining + rn.doCreate();
        }
    }
}
