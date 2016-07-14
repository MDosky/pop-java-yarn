/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package popjava.yarn;

import popjava.PopJava;
import popjava.annotation.POPAsyncConc;
import popjava.annotation.POPClass;
import popjava.annotation.POPObjectDescription;

/**
 *
 * @author Dosky
 */
@POPClass(isDistributable = false)
public class Test {

    public static void main(String[] args) throws InterruptedException {
        for (String s : args) {
            System.out.println(s);
        }
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
