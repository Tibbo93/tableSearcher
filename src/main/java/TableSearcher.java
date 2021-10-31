import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import lombok.Data;
import model.Dataset;
import model.Table;
import model.deserializer.TableDeserializer;
import org.apache.lucene.analysis.core.LowerCaseFilterFactory;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilterFactory;
import org.apache.lucene.analysis.standard.StandardTokenizerFactory;
import org.apache.lucene.codecs.simpletext.SimpleTextCodec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

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
                    new IndexWriterConfig(
                            CustomAnalyzer.builder()
                                    .withTokenizer(StandardTokenizerFactory.NAME)
                                    .addTokenFilter(LowerCaseFilterFactory.NAME)
                                    .addTokenFilter(WordDelimiterGraphFilterFactory.NAME)
                                    .build()
                    )
                            .setCodec(new SimpleTextCodec()));
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

            jsonReader.setLenient(true);

            while (jsonReader.hasNext() && jsonReader.peek() != JsonToken.END_DOCUMENT) {
                Table table = parseJSON(jsonReader);

                createDocument(table);
                dataset.updateDataset(table.getMaxDimensions().getRow() + 1, table.getMaxDimensions().getColumn() + 1);
            }
        } catch (FileNotFoundException e) {
            System.out.println("Error: FILE NOT FOUND");
        } catch (IOException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    public void createDocument(Table table) {
        Document document = new Document();

        try {
            table.getColumns()
                    .forEach((col, list) ->
                            list.forEach(cell -> {
                                if (cell.isHeader()) {
                                    document.add(new TextField("header", cell.getCleanedText(), Field.Store.YES));
                                } else if (cell.getCleanedText().isEmpty()) {
                                    this.dataset.incrementNullValueCounter();
                                } else {
                                    document.add(new TextField("content", cell.getCleanedText(), Field.Store.YES));
                                }
                            }));

            this.writer.addDocument(document);
        } catch (IOException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }
}
