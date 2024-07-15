package org.apache.arrow.datafusion.jdbc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class FileSystemDataDiscoverer {
  private final Path root;
  private static final java.util.Set<String> fileExtensions = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
  static {
    fileExtensions.addAll(List.of("csv", "avro", "parquet", "orc", "json", "xml"));
  }
  private final Predicate<Path> filePredicate = ((Predicate<Path>) Files::isRegularFile).and(Files::isReadable).and(path -> {
    String[] parts = path.toString().split("\\.");
    String extension = parts[parts.length - 1];
    return fileExtensions.contains(extension);
  });


  public FileSystemDataDiscoverer(Path root) {
    this.root = root;
  }

  public Stream<Path> find() throws IOException {
    return Files.walk(root).filter(filePredicate);
  }
}
