package ch.heia.pop.yarn.example;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import popjava.PopJava;
import popjava.annotation.POPClass;
import popjava.annotation.POPSyncSeq;
import popjava.base.POPObject;
import popjava.baseobject.POPAccessPoint;
import popjava.system.POPSystem;
import popjava.util.Util;

/**
 *
 * @author Dosky
 */
public class MultiNode {

    public static void main(String[] args) {
            POPSystem.initialize(args);
//        List<String> argsList = new ArrayList<>(Arrays.asList(args));
//        String jm = Util.removeStringFromList(argsList, "-jobservice=");
//
//        POPSystem.jobService = new POPAccessPoint(jm);
//        POPSystem.setStarted();

        RemoteNode rn = PopJava.newActive(RemoteNode.class, 10);
        int res = PopJava.newActive(RemoteNode.class, rn.getAccessPoint()).doCreate();
        System.out.println(res);
        
        POPSystem.end();
    }
    
    @POPClass
    public static class RemoteNode extends POPObject {

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
            RemoteNode rn = PopJava.newActive(RemoteNode.class, remaining - 1);
            return remaining + rn.doCreate();
        }
    }
}
