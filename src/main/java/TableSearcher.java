import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import lombok.Data;
import model.Dataset;
import model.Table;
import model.deserializer.TableDeserializer;
import org.apache.lucene.codecs.simpletext.SimpleTextCodec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Locale;

@Data
public class TableSearcher {
    private Directory indexPathDirectory;
    private final Gson gson;
    private IndexWriter writer;
    private Dataset dataset;

    public TableSearcher(Path indexPath) {
        this.gson = new GsonBuilder().registerTypeAdapter(
                        Table.class,
                        new TableDeserializer())
                .setPrettyPrinting().create();
        this.dataset = new Dataset(0, 0, 0, 0);
        try {
            this.indexPathDirectory = FSDirectory.open(indexPath);
            this.writer = new IndexWriter(
                    this.indexPathDirectory,
                    new IndexWriterConfig().setCodec(new SimpleTextCodec()));
        } catch (IOException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    public Table parseJSON(JsonReader jsonReader) {
        return this.gson.fromJson(jsonReader, Table.class);
    }

    public void createIndex(String jsonPath) {
        try (JsonReader jsonReader = new JsonReader(
                new InputStreamReader(
                        new FileInputStream(jsonPath), StandardCharsets.UTF_8))) {

            this.writer.deleteAll();
            jsonReader.setLenient(true);

            while (jsonReader.hasNext() && jsonReader.peek() != JsonToken.END_DOCUMENT) {
                Table table = parseJSON(jsonReader);

                createDocument(table);
                dataset.updateDataset(table.getMaxDimensions().getRow() + 1, table.getMaxDimensions().getColumn() + 1);
            }
            this.writer.commit();
        } catch (FileNotFoundException e) {
            System.out.println("Error: FILE NOT FOUND");
        } catch (IOException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    public void createDocument(Table table) {
        table.getColumns()
                .forEach((col, list) -> {
                            Document document = new Document();
                            list.forEach(cell -> {
                                if (cell.isHeader()) {
                                    document.add(new StringField("header", cell.getCleanedText().toLowerCase(Locale.ROOT), Field.Store.YES));
                                } else if (cell.getCleanedText().isEmpty()) {
                                    this.dataset.incrementNullValueCounter();
                                } else {
                                    document.add(new StringField("content", cell.getCleanedText().toLowerCase(Locale.ROOT), Field.Store.YES));
                                }
                            });
                            int distinctValues = (int) document.getFields().stream().map(IndexableField::stringValue).distinct().count();
                            this.dataset.updateDistribution(dataset.getDistinctValuesDistribution(), distinctValues);
                            try {
                                this.writer.addDocument(document);
                            } catch (IOException e) {
                                System.out.println("Error: " + e.getMessage());
                            }
                        }
                );
    }
}
