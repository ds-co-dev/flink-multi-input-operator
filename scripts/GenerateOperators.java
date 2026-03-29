import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

class GenerateOperators {

  public static void main(String[] args) throws IOException {
    Path root = findRepoRoot();
    Path outDir =
        root.resolve("src/main/java/dev/ds_co/flink/streaming/api/operators");
    Files.createDirectories(outDir);
    for (int n = 3; n <= 25; n++) {
      String content = generateOperator(n);
      Path out = outDir.resolve("KeyedMultiInputOperator" + n + ".java");
      Files.writeString(out, content, StandardCharsets.UTF_8);
      System.out.println("Wrote " + root.relativize(out));
    }
  }

  private static Path findRepoRoot() {
    Path dir = Paths.get("").toAbsolutePath();
    while (dir != null) {
      Path pom = dir.resolve("pom.xml");
      if (Files.isRegularFile(pom)) {
        return dir;
      }
      dir = dir.getParent();
    }
    throw new IllegalStateException("Could not find pom.xml; run from repo root");
  }

  private static String generateOperator(int n) {
    String typesSingle = joinComma(rangeInTypes(n));
    String addHandlers = joinLines(range(1, n, i -> "    addInputHandler(in" + i + ");"));
    String abstractMethods =
        joinDoubleNewline(
            range(
                1,
                n,
                i ->
                    "  protected abstract void processElement"
                        + i
                        + "(IN"
                        + i
                        + " value, Context ctx, Collector<OUT> out) throws Exception;"));
    String inputs = joinDoubleNewline(range(1, n, i -> inputBlock(n, i)));

    String classDecl;
    if (n >= 11) {
      String typesMultiline = chunkJoinTypes(n);
      classDecl =
          "public abstract class KeyedMultiInputOperator"
              + n
              + "<\n    "
              + typesMultiline
              + "> extends BaseMultiInputKeyedOperator<OUT> {\n";
    } else {
      classDecl =
          "public abstract class KeyedMultiInputOperator"
              + n
              + "<"
              + typesSingle
              + "> extends BaseMultiInputKeyedOperator<OUT> {\n";
    }

    return "package dev.ds_co.flink.streaming.api.operators;\n\n"
        + "import java.util.List;\n"
        + "import org.apache.flink.streaming.api.operators.AbstractInput;\n"
        + "import org.apache.flink.streaming.api.operators.Input;\n"
        + "import org.apache.flink.streaming.api.operators.StreamOperatorParameters;\n"
        + "import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;\n"
        + "import org.apache.flink.util.Collector;\n\n"
        + classDecl
        + "\n"
        + "  private static final long serialVersionUID = 1L;\n\n"
        + "  protected KeyedMultiInputOperator"
        + n
        + "(StreamOperatorParameters<OUT> params) {\n"
        + "    super(params, "
        + n
        + ");\n"
        + addHandlers
        + "\n"
        + "  }\n\n"
        + abstractMethods
        + "\n\n"
        + inputs
        + "\n\n"
        + "  @Override\n"
        + "  @SuppressWarnings(\"rawtypes\")\n"
        + "  public List<Input> getInputs() {\n"
        + "    return super.getInputs();\n"
        + "  }\n"
        + "}\n";
  }

  private static String chunkJoinTypes(int n) {
    StringBuilder sb = new StringBuilder();
    java.util.List<String> parts = new java.util.ArrayList<>();
    java.util.List<String> typeList = new java.util.ArrayList<>();
    for (int i = 1; i <= n; i++) {
      typeList.add("IN" + i);
    }
    typeList.add("OUT");
    for (int i = 0; i < typeList.size(); i += 8) {
      int end = Math.min(i + 8, typeList.size());
      parts.add(String.join(", ", typeList.subList(i, end)));
    }
    for (int i = 0; i < parts.size(); i++) {
      if (i > 0) {
        sb.append(",\n    ");
      }
      sb.append(parts.get(i));
    }
    return sb.toString();
  }

  private static String inputBlock(int n, int i) {
    return "  private final AbstractInput<IN"
        + i
        + ", OUT> in"
        + i
        + " =\n"
        + "      new AbstractInput<>(this, "
        + i
        + ") {\n"
        + "        @Override\n"
        + "        public void processElement(StreamRecord<IN"
        + i
        + "> record) throws Exception {\n"
        + "          setKeyContextElement(record);\n"
        + "          processElement"
        + i
        + "(record.getValue(), ctx(record), out);\n"
        + "        }\n"
        + "      };";
  }

  private static String[] rangeInTypes(int n) {
    String[] a = new String[n + 1];
    for (int i = 1; i <= n; i++) {
      a[i - 1] = "IN" + i;
    }
    a[n] = "OUT";
    return a;
  }

  private static String joinComma(String[] parts) {
    return String.join(", ", parts);
  }

  private static String joinLines(String[] lines) {
    StringBuilder sb = new StringBuilder();
    for (String line : lines) {
      sb.append(line).append('\n');
    }
    return sb.toString();
  }

  private static String joinDoubleNewline(String[] parts) {
    return String.join("\n\n", parts);
  }

  private static String[] range(int from, int to, java.util.function.IntFunction<String> fn) {
    String[] a = new String[to - from + 1];
    for (int i = from; i <= to; i++) {
      a[i - from] = fn.apply(i);
    }
    return a;
  }
}
