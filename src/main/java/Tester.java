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
import java.util.ArrayList;
import java.util.List;

public class Tester {

    @Test
    public void testIndexingAndExportDataset() {
        System.out.println("START - Test indexing and export dataset");
        System.out.println("===============================================================================================\n");

        long start = System.currentTimeMillis();
        Path indexPath = Paths.get(System.getenv("indexPath"));

        try {
            Indexer indexer = new Indexer(indexPath);
            indexer.index(System.getenv("jsonPath"));
            indexer.getDataset().exportDataset();
            System.out.println(indexer.getDataset());

            System.out.println("\n===============================================================================================");
            System.out.println("END - Execution time: " + (System.currentTimeMillis() - start) / 1000 + " seconds");
        } catch (IOException e) {
            System.out.println("Error during indexing json: " + e.getMessage());
        }
    }

    @Test
    public void testSearchLargeTable() {
        System.out.println("START - Test search with large table");

        Path indexPath = Paths.get(System.getenv("indexPath"));
        Searcher searcher = new Searcher(indexPath);

        try {
            Table table = this.getTable(System.getenv("largeTablePath"));
            long start = System.currentTimeMillis();
            System.out.println("Start searching...");
            List<Integer> topKSet = searcher.search(table.getColumns().get(0), 2, table.getMongoId().getOid());
            long time = (System.currentTimeMillis() - start) / 1000;
            System.out.println("END - Execution time: " + time + " seconds");

            System.out.println("\n===============================================================================================");
            System.out.println("RESULT - k sets with highest counts: " + topKSet);
        } catch (IOException e) {
            System.out.println("Cannot open table json. Error: " + e.getMessage());
        }
    }

    @Test
    public void testSearchMultiTable() {
        System.out.println("START - Test search with large table");

        Path indexPath = Paths.get(System.getenv("indexPath"));
        Searcher searcher = new Searcher(indexPath);

        List<Long> tempi = new ArrayList<>();
        int[] tables = {4, 4, 12, 25, 50, 100, 200, 401, 802, 1599, 3143, 6102};
        //int[] tables = {4,12,25,50,100};
        for (int n : tables) {
            try {
                Table table = this.getTable("C:\\Users\\aless\\Desktop\\MAGISTRALE\\Ingegneria dei dati\\progetti\\tables\\table-" + n + ".json");
                long start = System.currentTimeMillis();
                System.out.println("Start searching...");
                List<Integer> topKSet = searcher.search(table.getColumns().get(0), 2, table.getMongoId().getOid());
                long time = (System.currentTimeMillis() - start) / 1000;
                tempi.add(time);
                System.out.println("END - Execution time: " + time + " seconds");

                System.out.println("\n===============================================================================================");
                System.out.println("RESULT - k sets with highest counts: " + topKSet);
            } catch (IOException e) {
                System.out.println("Cannot open table json. Error: " + e.getMessage());
            }
        }
        System.out.println("Tempi: " + tempi);
        System.out.println("Execution time map: " + searcher.getExecutionTimeList());
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
