package cis5550.kvs;

import java.io.IOException;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

public class ManualTest {

  public static void main(String[] args) throws IOException, InterruptedException {
    testOrderedScan(args[0]);
  }

  private static void testOrderedScan(String masterPort) throws IOException, InterruptedException {
    System.out.println("testOrderedScan");
    KVSClient client = new KVSClient("localhost:%s".formatted(masterPort));
    IntStream.range(0, 100).forEach(i -> {
      try {
        client.put("test", String.valueOf(i), "v", String.valueOf(i));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    StreamSupport.stream(((Iterable<Row>) () -> {
      try {
        return client.scan("test");
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }).spliterator(), false).forEach(System.out::println);
  }

}
