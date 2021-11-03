package model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import lombok.Data;
import model.deserializer.TableDeserializer;
import model.serializer.TableSerializer;
import org.apache.lucene.codecs.simpletext.SimpleTextCodec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

@Data
public class Indexer {
    private Directory indexPathDirectory;
    private final Gson gson;
    private IndexWriter writer;
    private Dataset dataset;

    public Indexer(Path indexPath) {
        this.gson = new GsonBuilder()
                .registerTypeAdapter(Table.class, new TableDeserializer())
                .registerTypeAdapter(Table.class, new TableSerializer())
                .create();
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

    public void index(String jsonPath) {
        try (JsonReader jsonReader = new JsonReader(
                new InputStreamReader(
                        new FileInputStream(jsonPath), StandardCharsets.UTF_8))) {

            this.writer.deleteAll();
            jsonReader.setLenient(true);

            while (jsonReader.hasNext() && jsonReader.peek() != JsonToken.END_DOCUMENT) {
                Table table = parseJSON(jsonReader);

                this.addDocument(table);
                dataset.updateDataset(table.getMaxDimensions().getRow() + 1, table.getMaxDimensions().getColumn() + 1);
            }

            this.writer.commit();
            this.writer.close();
            this.indexPathDirectory.close();
        } catch (IOException e) {
            System.out.println("Cannot index file. Error: " + e.getMessage());
        }
    }

    public Table parseJSON(JsonReader jsonReader) {
        return this.gson.fromJson(jsonReader, Table.class);
    }

    private void addDocument(Table table) {
        table.getColumns()
                .forEach((col, list) -> {
                            Document document = new Document();
                            document.add(new StringField("tableId", table.getMongoId().getOid(), Field.Store.YES));
                            list.forEach(cell -> {
                                if (cell.getType().equals("EMPTY")) {
                                    this.dataset.incrementNullValuesCounter();
                                } else {
                                    String str = cell.getCleanedText().toLowerCase().replaceAll("\\p{Punct}", "").trim();
                                    if (cell.isHeader() && !str.isEmpty()) {
                                        document.add(new StringField("header", str, Field.Store.YES));
                                    } else if (!str.isEmpty()) {
                                        document.add(new StringField("content", str, Field.Store.YES));
                                    }
                                }
                            });

                            int distinctValues = (int) document.getFields().stream().map(IndexableField::stringValue).distinct().count();
                            this.dataset.updateDistribution(dataset.getDistinctValuesDistribution(), distinctValues);

                            try {
                                this.writer.addDocument(document);
                            } catch (IOException e) {
                                System.out.println("Cannot add document to index. Error: " + e.getMessage());
                            }
                        }
                );
    }

    public void exportTable(Table table, String jasonName) {
        String jsonString = this.gson.toJson(table);
        try (FileWriter file = new FileWriter(jasonName)) {
            file.write(jsonString);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}