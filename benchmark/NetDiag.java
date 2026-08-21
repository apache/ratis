import org.apache.ratis.util.NetUtils;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.OpenSsl;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContext;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContextBuilder;

import java.io.File;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.DatagramSocket;
import java.net.StandardProtocolFamily;
import java.nio.channels.DatagramChannel;

/**
 * Diagnostyka srodowiskowa dla luki 142ms w conn A.
 * Sprawdza: rozwiazywanie nazw (czas + rodzina adresow), silnik TLS, gniazda UDP.
 * Uruchomienie:  java -cp ratis.jar NetDiag.java dcc-1 dcc-2 dcc-3
 */
public class NetDiag {
  public static void main(String[] args) throws Exception {
    final String[] hosts = args.length > 0 ? args : new String[]{"dcc-1", "dcc-2", "dcc-3"};

    System.out.println("==================== SRODOWISKO ====================");
    System.out.println("os              = " + System.getProperty("os.name") + " / " + System.getProperty("os.arch"));
    System.out.println("java            = " + System.getProperty("java.version"));
    System.out.println("rdzenie         = " + Runtime.getRuntime().availableProcessors());
    System.out.println("preferIPv4Stack = " + System.getProperty("java.net.preferIPv4Stack"));
    System.out.println("preferIPv6      = " + System.getProperty("java.net.preferIPv6Addresses"));

    System.out.println();
    System.out.println("==================== ROZWIAZYWANIE NAZW ====================");
    System.out.println("Kod wola NetUtils.createSocketAddr() przy KAZDYM connect (QuicRpcProxy:375).");
    for (String h : hosts) {
      // pierwsze wywolanie - zimne
      long t0 = System.nanoTime();
      final InetAddress[] all = InetAddress.getAllByName(h);
      final double coldMs = (System.nanoTime() - t0) / 1e6;

      // kolejne - sprawdzamy czy cache dziala
      double warmTotal = 0;
      final int n = 20;
      for (int i = 0; i < n; i++) {
        final long s = System.nanoTime();
        InetAddress.getAllByName(h);
        warmTotal += (System.nanoTime() - s) / 1e6;
      }

      // to samo przez sciezke ktorej uzywa Ratis
      double ratisTotal = 0;
      for (int i = 0; i < n; i++) {
        final long s = System.nanoTime();
        NetUtils.createSocketAddr(h + ":10024");
        ratisTotal += (System.nanoTime() - s) / 1e6;
      }

      final StringBuilder sb = new StringBuilder();
      for (InetAddress a : all) {
        sb.append(a.getHostAddress())
          .append(a instanceof Inet6Address ? " [IPv6]" : a instanceof Inet4Address ? " [IPv4]" : " [?]")
          .append("  ");
      }
      System.out.printf("%-8s zimne=%7.3f ms   cieple=%7.3f ms   przez Ratis=%7.3f ms%n",
          h, coldMs, warmTotal / n, ratisTotal / n);
      System.out.printf("         adresy: %s%n", sb.toString().trim());
    }

    System.out.println();
    System.out.println("==================== GNIAZDA UDP ====================");
    try (DatagramChannel any = DatagramChannel.open()) {
      any.bind(new InetSocketAddress(0));
      final DatagramSocket s = any.socket();
      System.out.println("DatagramChannel.open() domyslnie -> " + s.getLocalSocketAddress()
          + "  (klasa adresu: " + s.getLocalAddress().getClass().getSimpleName() + ")");
    }
    try (DatagramChannel v4 = DatagramChannel.open(StandardProtocolFamily.INET)) {
      v4.bind(new InetSocketAddress(0));
      System.out.println("wymuszony INET  (IPv4) -> OK, " + v4.socket().getLocalSocketAddress());
    } catch (Exception e) {
      System.out.println("wymuszony INET  (IPv4) -> BLAD: " + e);
    }
    try (DatagramChannel v6 = DatagramChannel.open(StandardProtocolFamily.INET6)) {
      v6.bind(new InetSocketAddress(0));
      System.out.println("wymuszony INET6 (IPv6) -> OK, " + v6.socket().getLocalSocketAddress());
    } catch (Exception e) {
      System.out.println("wymuszony INET6 (IPv6) -> BLAD: " + e);
    }

    System.out.println();
    System.out.println("==================== SILNIK TLS NETTY (R2) ====================");
    System.out.println("OpenSsl.isAvailable() = " + OpenSsl.isAvailable());
    final Throwable cause = OpenSsl.unavailabilityCause();
    if (cause != null) {
      System.out.println("powod niedostepnosci  = " + cause);
    } else {
      System.out.println("wersja                = " + OpenSsl.versionString());
    }
    final String ssl = System.getProperty("ssl.dir", "ratis-test/src/test/resources/ssl");
    try {
      final SslContext server = SslContextBuilder
          .forServer(new File(ssl + "/server.crt"), new File(ssl + "/server.pem")).build();
      final String cls = server.getClass().getSimpleName();
      System.out.println("kontekst serwera      = " + cls);
      System.out.println(cls.contains("OpenSsl")
          ? ">>> OK: natywny BoringSSL — porownanie z QUIC uczciwe"
          : ">>> UWAGA: JDK SSLEngine — ramie TCP karane, porownanie niewazne");
    } catch (Exception e) {
      System.out.println("nie udalo sie zbudowac kontekstu z " + ssl + ": " + e);
    }
  }
}
