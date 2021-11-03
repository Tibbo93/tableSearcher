package model;

import lombok.Data;
import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

@Data
public class Searcher {
    private Directory indexDirectory;
    private IndexReader reader;

    public Searcher(Path indexDirectory) {
        try {
            this.indexDirectory = FSDirectory.open(indexDirectory);
            this.reader = DirectoryReader.open(this.indexDirectory);
        } catch (IOException e) {
            System.out.println("Cannot create Searcher object. Error: " + e.getMessage());
        }
    }

    public List<Integer> search(List<Cell> query, int k) throws IOException {
        HashMap<Integer, Integer> set2count = new HashMap<>();
        Terms terms = MultiTerms.getTerms(reader, "content");

        query.stream()
                .filter(cell -> !cell.isHeader())
                .map(cell -> cell.getCleanedText().trim().toLowerCase(Locale.ROOT))
                .distinct()
                .forEach(token -> {
                    System.out.println("Analyzing token: " + token);
                    PostingsEnum postingList = this.getPostingList(token, terms);

                    if (postingList != null) {
                        readPostingList(postingList, set2count);
                    }
                    System.out.println(set2count);
                });

        LinkedHashMap<Integer, Integer> sortedSet2count = this.sortSet2count(set2count);
        return this.getTopK(sortedSet2count, k);


    }

    private PostingsEnum getPostingList(String token, Terms terms) {
        try {
            TermsEnum termsEnum = terms.iterator();
            BytesRef term;

            while ((term = termsEnum.next()) != null) {
                if (token.equals(term.utf8ToString())) {
                    return termsEnum.postings(null);
                }
            }
        } catch (IOException e) {
            System.out.println("Cannot retrieve posting list for token '" + token + "'. Error: " + e.getMessage());
        }
        return null;
    }

    private void readPostingList(PostingsEnum postingList, HashMap<Integer, Integer> set2count) {
        int docID;

        try {
            while ((docID = postingList.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (!set2count.containsKey(docID)) {
                    set2count.put(docID, 1);
                } else {
                    set2count.put(docID, set2count.get(docID) + 1);
                }
            }
        } catch (IOException e) {
            System.out.println("Cannot read posting list. Error: " + e.getMessage());
        }
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
