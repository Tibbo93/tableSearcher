package model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Dataset {
    private int tablesCounter;
    private int rowsCounter;
    private int colsCounter;
    private int nullValuesCounter;
    private final HashMap<Integer, Integer> rowsDistribution = new HashMap<>();
    private final HashMap<Integer, Integer> colsDistribution = new HashMap<>();
    private final HashMap<Integer, Integer> distinctValueDistribution = new HashMap<>();
//        statistics.put("averageRows", 0);
//        statistics.put("averageCols", 0);
//        statistics.put("averageNullValue", 0);

    public void incrementTablesCounter() {
        this.tablesCounter++;
    }

    public void incrementRowsCounter(int count) {
        this.rowsCounter += count;
    }

    public void incrementColsCounter(int count) {
        this.colsCounter += count;
    }

    public void incrementNullValueCounter() {
        this.nullValuesCounter++;
    }

    public void incrementRowsDistribution(Integer key, Integer count) {
        this.rowsDistribution.put(key, this.rowsDistribution.get(key) + 1);
    }

    public void incrementColsDistribution(Integer key, Integer count) {
        this.colsDistribution.put(key, this.colsDistribution.get(key) + 1);
    }

    public void incrementDistinctValueDistribution(Integer key, Integer count) {
        this.distinctValueDistribution.put(key, this.distinctValueDistribution.get(key) + 1);
    }

    public LinkedHashMap<Integer, Integer> sortRowsDistribution() {
        return this.rowsDistribution.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (oldValue, newValue) -> oldValue, LinkedHashMap::new));
    }

    public LinkedHashMap<Integer, Integer> sortColsDistribution() {
        return this.colsDistribution.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (oldValue, newValue) -> oldValue, LinkedHashMap::new));
    }
}
