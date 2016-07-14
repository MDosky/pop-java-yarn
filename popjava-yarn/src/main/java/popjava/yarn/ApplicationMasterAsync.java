package popjava.yarn;

import popjava.PopJava;
import popjava.annotation.POPAsyncConc;
import popjava.annotation.POPClass;
import popjava.annotation.POPObjectDescription;
import popjava.annotation.POPSyncConc;

/**
 * This class implements a simple async app master.
 */
@POPClass(isDistributable = false)
public class ApplicationMasterAsync {

    public static void main(String[] args) throws InterruptedException {
        for(String s : args)
            System.out.println(s);
//        ApplicationMasterPOP master = new ApplicationMasterPOP(args);
//        master.setup();
//        master.runMainLoop();
        A a = new A();
        PopJava.getThis(a).b();
        
        Thread.sleep(1000);
    }
    
    @POPClass
    public static class A {

        @POPObjectDescription(url = "localhost")
        public A() {
        }
        
        @POPAsyncConc
        public void b() {
            System.out.println(":=|");
        }
    }
}
