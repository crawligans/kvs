package cis5550.generic;

import cis5550.kvs.Row;
import java.io.IOException;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import javax.naming.OperationNotSupportedException;

public interface Table extends Map<String, Row> {

  Map.Entry<Integer, Row> putVersion(String k, Row v, int version)
      throws OperationNotSupportedException;

  Map.Entry<Integer, Row> putVersion(String k, Row v) throws IOException;

  Map.Entry<Integer, Row> computeVersion(String k, int version,
      BiFunction<String, Row, Row> compute)
      throws OperationNotSupportedException;

  Map.Entry<Integer, Row> computeVersion(String k, BiFunction<String, Row, Row> compute)
      throws Exception;

  Map.Entry<Integer, Row> getVersion(String k, int version) throws OperationNotSupportedException;

  Map.Entry<Integer, Row> getVersion(String k);

  Row get(String k, int version) throws OperationNotSupportedException;

  Stream<Row> valueStream();
}
