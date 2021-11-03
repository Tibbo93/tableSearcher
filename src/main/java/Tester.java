import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

public class Tester {
    @Test
    public void testIndexingAndExportDataset() {
        System.out.println("START - Test indexing and export dataset");
        System.out.println("===============================================================================================\n");

        long start = System.currentTimeMillis();
        Path indexPath = Paths.get(System.getenv("indexPath"));
        TableSearcher tableSearcher = new TableSearcher(indexPath);

        tableSearcher.createIndex(System.getenv("jsonPath"));
        tableSearcher.getDataset().exportDataset();
        System.out.println(tableSearcher.getDataset());

        System.out.println("\n===============================================================================================");
        System.out.println("END - Execution time: " + (System.currentTimeMillis() - start) / 1000 + " seconds");
    }
}
