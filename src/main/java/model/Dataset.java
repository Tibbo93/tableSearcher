package model;

import com.google.gson.GsonBuilder;
import lombok.Data;

import java.io.FileWriter;
import java.io.IOException;
import java.util.TreeMap;

@Data
public class Dataset {
    private int tablesCounter;
    private int rowsCounter;
    private int colsCounter;
    private int nullValuesCounter;
    private final TreeMap<Integer, Integer> rowsDistribution;
    private final TreeMap<Integer, Integer> colsDistribution;
    private final TreeMap<Integer, Integer> distinctValuesDistribution;
    private final TreeMap<Integer, Integer> nullValuesDistribution;

    public Dataset() {
        this.tablesCounter = 0;
        this.rowsCounter = 0;
        this.colsCounter = 0;
        this.nullValuesCounter = 0;
        this.rowsDistribution = new TreeMap<>();
        this.colsDistribution = new TreeMap<>();
        this.distinctValuesDistribution = new TreeMap<>();
        this.nullValuesDistribution = new TreeMap<>();
    }

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
                "\nNumero totale di righe con header compreso: " + this.rowsCounter +
                "\nNumero totale di righe senza header: " + (this.rowsCounter - this.tablesCounter) +
                "\nNumero totale di colonne: " + this.colsCounter +
                "\nNumero totale di valori nulli: " + this.nullValuesCounter +
                "\nNumero medio di righe: " + (this.rowsCounter - this.tablesCounter) / this.tablesCounter +
                "\nNumero medio di colonne: " + (this.colsCounter / this.tablesCounter) +
                "\nNumero medio di valori nulli: " + (this.nullValuesCounter / this.tablesCounter) +
                "\nDistribuzione numero di righe: " + this.rowsDistribution +
                "\nDistribuzione numero di colonne: " + this.colsDistribution +
                "\nDistribuzione valori distinti: " + this.distinctValuesDistribution +
                "\nDistribuzione valori nulli: " + this.nullValuesDistribution;
    }

    public void updateDistribution(TreeMap<Integer, Integer> distribution, int key) {
        distribution.merge(key, 1, Integer::sum);
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
        this.updateDistribution(this.getRowsDistribution(), rows - 1);
        this.updateDistribution(this.getColsDistribution(), cols);
    }
}
