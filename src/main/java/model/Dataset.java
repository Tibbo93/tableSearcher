package model;

import com.google.gson.GsonBuilder;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.FileWriter;
import java.io.IOException;
import java.util.TreeMap;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Dataset {
    private int tablesCounter;
    private int rowsCounter;
    private int colsCounter;
    private int nullValuesCounter;
    private final TreeMap<Integer, Integer> rowsDistribution = new TreeMap<>();
    private final TreeMap<Integer, Integer> colsDistribution = new TreeMap<>();
    private final TreeMap<Integer, Integer> distinctValuesDistribution = new TreeMap<>();

    public void incrementTablesCounter() {
        this.tablesCounter++;
    }

    public void incrementRowsCounter(int count) {
        this.rowsCounter += count;
    }

    public void incrementColsCounter(int count) {
        this.colsCounter += count;
    }

    public void incrementNullValuesCounter() {
        this.nullValuesCounter++;
    }

    @Override
    public String toString() {
        return "Numero totale di tabelle: " + this.tablesCounter +
                "\nNumero totale di righe: " + this.rowsCounter +
                "\nNumero totale di colonne: " + this.colsCounter +
                "\nNumero totale di valori nulli: " + this.nullValuesCounter +
                "\nNumero medio di righe: " + this.rowsCounter / (float) this.tablesCounter +
                "\nNumero medio di colonne: " + this.colsCounter / (float) this.tablesCounter +
                "\nDistribuzione numero di righe: " + this.rowsDistribution +
                "\nDistribuzione numero di colonne: " + this.colsDistribution +
                "\nDistribuzione valori distinti: " + this.distinctValuesDistribution;
    }

    public void updateDistribution(TreeMap<Integer, Integer> distribution, int key) {
        if (!distribution.containsKey(key)) {
            distribution.put(key, 1);
        } else {
            distribution.put(key, distribution.get(key) + 1);
        }
    }

    public void exportDataset() {
        String jsonString = new GsonBuilder().setPrettyPrinting().create().toJson(this);
        try (FileWriter file = new FileWriter("statistics.json")) {
            file.write(jsonString);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void updateDataset(int rows, int cols) {
        //UPDATE STATISTICS HASHMAP
        this.incrementTablesCounter();
        this.incrementRowsCounter(rows);
        this.incrementColsCounter(cols);

        //UPDATE DISTRIBUTIONS HASHMAP
        this.updateDistribution(this.getRowsDistribution(), rows);
        this.updateDistribution(this.getColsDistribution(), cols);
    }
}
