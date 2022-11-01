package model;

import lombok.Data;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

@Data
public class Searcher {
    private Directory indexDirectory;
    private IndexReader reader;
    private IndexSearcher searcher;
    private Map<Integer, Long> executionTimeList;

    public Searcher(Path indexDirectory) {
        try {
            this.indexDirectory = FSDirectory.open(indexDirectory);
            this.reader = DirectoryReader.open(this.indexDirectory);
            this.searcher = new IndexSearcher(this.reader);
            this.executionTimeList = new HashMap<>();
        } catch (IOException e) {
            System.out.println("Cannot create Searcher object. Error: " + e.getMessage());
        }
    }

    public List<Integer> search(List<Cell> query, int k, String tableId) throws IOException {
        long startTime = System.currentTimeMillis();
        List<Integer> topK = this.mergeList(this.filterQuery(query), k, tableId);
        long endTime = System.currentTimeMillis();
        this.executionTimeList.put(query.size(), endTime - startTime);

        //this.indexDirectory.close();
        //this.reader.close();

        return topK;
    }

    public List<String> filterQuery(List<Cell> query) {
        return query.stream()
                .filter(cell -> !cell.isHeader())
                .filter(cell -> !cell.getType().equals("EMPTY"))
                .map(cell -> cell.getCleanedText().toLowerCase().replaceAll("[\\p{Punct}]", "").trim())
                .filter(s -> !s.isEmpty())
                .distinct()
                .toList();
    }

    public List<Integer> mergeList(List<String> query, int k, String tableId) throws IOException {
        HashMap<Integer, Integer> set2count = new HashMap<>();

        for (String token : query) {
            TermQuery termQuery = new TermQuery(new Term("text", token));
            TopDocs hits = this.searcher.search(termQuery, Integer.MAX_VALUE);

            Arrays.stream(hits.scoreDocs)
                    .map(scoreDoc -> scoreDoc.doc)
                    .forEach(doc -> {
                        try {
                            if (!this.searcher.doc(doc).get("tableId").equals(tableId)) {
                                set2count.merge(doc, 1, Integer::sum);
                            }
                        } catch (IOException e) {
                            System.out.println("Cannot retrieve doc " + doc);
                        }
                    });
        }

        LinkedHashMap<Integer, Integer> sortedSet2Count = this.sortSet2count(set2count);
        return this.getTopK(sortedSet2Count, k);
    }

    private LinkedHashMap<Integer, Integer> sortSet2count(HashMap<Integer, Integer> set2count) {
        return set2count.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (oldValue, newValue) -> oldValue, LinkedHashMap::new));
    }

    private List<Integer> getTopK(LinkedHashMap<Integer, Integer> set2count, int k) {
        List<Integer> topKValues = set2count.values()
                .stream()
                .distinct()
                .limit(k)
                .toList();

        return set2count.entrySet()
                .stream()
                .filter(entry -> topKValues.contains(entry.getValue()))
                .map(Map.Entry::getKey)
                .toList();
    }
}
