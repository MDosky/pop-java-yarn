package ch.heia.pop.yarn.example;

import java.math.BigInteger;
import java.util.Date;
import popjava.annotation.POPAsyncConc;
import popjava.annotation.POPClass;
import popjava.annotation.POPConfig;
import popjava.annotation.POPConfig.Type;
import popjava.annotation.POPObjectDescription;
import popjava.annotation.POPSyncMutex;
import popjava.annotation.POPSyncSeq;
import popjava.base.POPObject;
import popjava.baseobject.ConnectionType;

@POPClass
public class TestClass extends POPObject {

    private StringBuffer LOG = new StringBuffer();

    @POPObjectDescription(connection = ConnectionType.DEAMON, connectionSecret = "", url = POPObjectDescription.LOCAL_DEBUG_URL)
    public TestClass() {
    }

    @POPObjectDescription(connection = ConnectionType.DEAMON, connectionSecret = "")
    public TestClass(@POPConfig(Type.URL) String url) {
    }

    @POPAsyncConc
    public void doSomething() {
        LOG.append("Doing something...\n");
        int m = 5;

        for (int i = 0; i < m; i++) {
            long n = 200 + new java.util.Random().nextInt(200);
            LOG.append("Factorial of " + n + " = " + factorial(n).toString().substring(0, 15) + "...\n");
        }
        LOG.append("Waiting for thread.\n");
    }

    @POPSyncSeq
    public BigInteger factorial(long i) {
        BigInteger n = BigInteger.ONE;
        for (; i > 0; i--) {
            n = n.multiply(new BigInteger(i + ""));
        }
        return n;
    }

    @POPSyncMutex
    public String report() {
        return LOG.toString();
    }

}
