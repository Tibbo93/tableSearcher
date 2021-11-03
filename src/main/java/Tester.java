import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import model.Indexer;
import model.Searcher;
import model.Table;
import model.deserializer.TableDeserializer;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class Tester {

    @Test
    public void testIndexingAndExportDataset() {
        System.out.println("START - Test indexing and export dataset");
        System.out.println("===============================================================================================\n");

        long start = System.currentTimeMillis();
        Path indexPath = Paths.get(System.getenv("indexPath"));
        Indexer indexer = new Indexer(indexPath);

        indexer.index(System.getenv("jsonPath"));
        indexer.getDataset().exportDataset();
        System.out.println(indexer.getDataset());

        System.out.println("\n===============================================================================================");
        System.out.println("END - Execution time: " + (System.currentTimeMillis() - start) / 1000 + " seconds");
    }

    @Test
    public void testSearchLargeTable() {
        System.out.println("START - Test search with large table");
        System.out.println("===============================================================================================\n");

        long start = System.currentTimeMillis();
        Path indexPath = Paths.get(System.getenv("indexPath"));
        Searcher searcher = new Searcher(indexPath);

        try {
            Table table = this.getTable(System.getenv("largeTablePath"));
            List<Integer> topKSet = searcher.search(table.getColumns().get(0), 2, table.getMongoId().getOid());

            System.out.println("\n===============================================================================================");
            System.out.println("RESULT - k sets with highest counts: " + topKSet);
        } catch (IOException e) {
            System.out.println("Cannot open table json. Error: " + e.getMessage());
        }

        System.out.println("END - Execution time: " + (System.currentTimeMillis() - start) / 1000 + " seconds");
    }

    private Table getTable(String tablePath) throws IOException {
        Gson gson = new GsonBuilder()
                .registerTypeAdapter(Table.class, new TableDeserializer())
                .create();

        try (JsonReader jsonReader = new JsonReader(
                new InputStreamReader(
                        new FileInputStream(tablePath), StandardCharsets.UTF_8))) {

            jsonReader.setLenient(true);

            return gson.fromJson(jsonReader, Table.class);
        }
    }
}
