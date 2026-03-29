import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

class GenerateTestHarnesses {

  public static void main(String[] args) throws IOException {
    Path root = findRepoRoot();
    Path outDir =
        root.resolve("src/test/java/dev/ds_co/flink/streaming/util/testing");
    Files.createDirectories(outDir);
    for (int n = 3; n <= 25; n++) {
      String content = generateHarness(n);
      Path out = outDir.resolve("KeyedMultiInputOperatorTestHarness" + n + ".java");
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

  private static String generateHarness(int n) {
    String typeParams = formatHarnessTypeParams(n);
    String operatorTypeParams = formatOperatorInTypes(n);
    String classType = "KeyedMultiInputOperator" + n + "<" + operatorTypeParams + ", OUT>";
    String ctorParams = formatCtorKeySelectors(n);
    String ctorArgs = formatCtorArgs(n);
    String closureCleaner = formatClosureCleaner(n);
    String statePartitioners = formatStatePartitioners(n);
    String processMethods = formatProcessMethods(n);

    return "package dev.ds_co.flink.streaming.util.testing;\n\n"
        + "import dev.ds_co.flink.streaming.api.operators.KeyedMultiInputOperator"
        + n
        + ";\n"
        + "import dev.ds_co.flink.streaming.api.operators.MultiInputOperatorFactory;\n"
        + "import java.util.ArrayList;\n"
        + "import java.util.List;\n"
        + "import org.apache.flink.api.common.ExecutionConfig;\n"
        + "import org.apache.flink.api.common.typeinfo.TypeInformation;\n"
        + "import org.apache.flink.api.java.ClosureCleaner;\n"
        + "import org.apache.flink.api.java.functions.KeySelector;\n"
        + "import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;\n"
        + "import org.apache.flink.streaming.util.MultiInputStreamOperatorTestHarness;\n\n"
        + "public class KeyedMultiInputOperatorTestHarness"
        + n
        + "<\n        "
        + typeParams
        + ">\n"
        + "    extends MultiInputStreamOperatorTestHarness<OUT> {\n\n"
        + "  public KeyedMultiInputOperatorTestHarness"
        + n
        + "(\n"
        + "      Class<? extends "
        + classType
        + "> operatorClass,\n"
        + "      "
        + ctorParams
        + ",\n"
        + "      TypeInformation<K> keyTypeInfo)\n"
        + "      throws Exception {\n"
        + "    this(operatorClass, "
        + ctorArgs
        + ", keyTypeInfo, 1, 1, 0);\n"
        + "  }\n\n"
        + "  public KeyedMultiInputOperatorTestHarness"
        + n
        + "(\n"
        + "      Class<? extends "
        + classType
        + "> operatorClass,\n"
        + "      "
        + ctorParams
        + ",\n"
        + "      TypeInformation<K> keyTypeInfo,\n"
        + "      int maxParallelism,\n"
        + "      int numSubtasks,\n"
        + "      int subtaskIndex)\n"
        + "      throws Exception {\n"
        + "    super(new MultiInputOperatorFactory<>(operatorClass), maxParallelism, numSubtasks, subtaskIndex);\n\n"
        + closureCleaner
        + "\n\n"
        + statePartitioners
        + "\n"
        + "    config.setStateKeySerializer(keyTypeInfo.createSerializer(executionConfig.getSerializerConfig()));\n"
        + "    config.serializeAllConfigs();\n"
        + "  }\n\n"
        + processMethods
        + "\n\n"
        + "  @Override\n"
        + "  @SuppressWarnings(\"unchecked\")\n"
        + "  public List<OUT> extractOutputValues() {\n"
        + "    List<OUT> result = new ArrayList<>();\n"
        + "    for (Object record : getOutput()) {\n"
        + "      if (record instanceof StreamRecord) {\n"
        + "        result.add((OUT) ((StreamRecord<?>) record).getValue());\n"
        + "      }\n"
        + "    }\n"
        + "    return result;\n"
        + "  }\n"
        + "}\n";
  }

  private static String formatHarnessTypeParams(int n) {
    List<String> typeList = new ArrayList<>();
    typeList.add("K");
    for (int i = 1; i <= n; i++) {
      typeList.add("IN" + i);
    }
    typeList.add("OUT");
    if (n >= 10) {
      List<String> parts = new ArrayList<>();
      for (int i = 0; i < typeList.size(); i += 8) {
        int end = Math.min(i + 8, typeList.size());
        parts.add(String.join(", ", typeList.subList(i, end)));
      }
      return String.join(",\n        ", parts);
    }
    return String.join(", ", typeList);
  }

  private static String formatOperatorInTypes(int n) {
    List<String> ins = new ArrayList<>();
    for (int i = 1; i <= n; i++) {
      ins.add("IN" + i);
    }
    return String.join(", ", ins);
  }

  private static String formatCtorKeySelectors(int n) {
    List<String> lines = new ArrayList<>();
    for (int i = 1; i <= n; i++) {
      lines.add("KeySelector<IN" + i + ", K> keySelector" + i);
    }
    if (n >= 11) {
      List<String> parts = new ArrayList<>();
      for (int i = 0; i < lines.size(); i += 5) {
        int end = Math.min(i + 5, lines.size());
        parts.add(String.join(", ", lines.subList(i, end)));
      }
      return String.join(",\n      ", parts);
    }
    return String.join(", ", lines);
  }

  private static String formatCtorArgs(int n) {
    List<String> args = new ArrayList<>();
    for (int i = 1; i <= n; i++) {
      args.add("keySelector" + i);
    }
    return String.join(", ", args);
  }

  private static String formatClosureCleaner(int n) {
    StringBuilder sb = new StringBuilder();
    for (int i = 1; i <= n; i++) {
      if (i > 1) {
        sb.append("\n    ");
      }
      sb.append(
          "ClosureCleaner.clean(keySelector"
              + i
              + ", ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);");
    }
    return sb.toString();
  }

  private static String formatStatePartitioners(int n) {
    StringBuilder sb = new StringBuilder();
    for (int i = 1; i <= n; i++) {
      if (i > 1) {
        sb.append("\n    ");
      }
      sb.append("config.setStatePartitioner(").append(i - 1).append(", keySelector").append(i).append(");");
    }
    return sb.toString();
  }

  private static String formatProcessMethods(int n) {
    StringBuilder sb = new StringBuilder();
    for (int i = 1; i <= n; i++) {
      if (i > 1) {
        sb.append("\n\n  ");
      }
      sb.append("public void processElement")
          .append(i)
          .append("(StreamRecord<IN")
          .append(i)
          .append("> record) throws Exception {\n")
          .append("    processElement(")
          .append(i - 1)
          .append(", record);\n")
          .append("  }");
    }
    return sb.toString();
  }
}
