package ch.heia.pop.yarn.example;

import java.net.Inet4Address;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.logging.Level;
import java.util.logging.Logger;
import popjava.PopJava;
import popjava.annotation.POPClass;
import popjava.annotation.POPConfig;
import popjava.annotation.POPObjectDescription;
import popjava.annotation.POPSyncSeq;
import popjava.baseobject.ConnectionType;

/**
 *
 * @author Dosky
 */
@POPClass
public class First {

    @POPObjectDescription(connection = ConnectionType.DEAMON, connectionSecret = "", url = POPObjectDescription.LOCAL_DEBUG_URL)
    public First() {
    }

    @POPObjectDescription(connection = ConnectionType.DEAMON, connectionSecret = "")
    public First(@POPConfig(POPConfig.Type.URL) String url) {
    }

    @POPSyncSeq
    public void dos(String... urls) throws SocketException {
        if (urls.length > 0) {
            First f = PopJava.newActive(First.class, urls[0]);
            f.dos(Arrays.copyOfRange(urls, 1, urls.length));
        }

        String s = "";
        Enumeration e = NetworkInterface.getNetworkInterfaces();
        while (e.hasMoreElements()) {
            NetworkInterface n = (NetworkInterface) e.nextElement();
            Enumeration ee = n.getInetAddresses();
            while (ee.hasMoreElements()) {
                Inet4Address i = (Inet4Address) ee.nextElement();
                s += " " + i.getHostAddress();
            }
        }
        System.out.println("[" + urls.length + "] " + s);
    }
}
