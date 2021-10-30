package model;

import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Table {

    @Data
    public static class Dimension {
        private int row;
        private int column;
    }

    @Data
    public static class Id {
        @SerializedName("$oid")
        private String oid;
    }

    @SerializedName("_id")
    private Id mongoId;
    private String className;
    private String id;
    private Map<Integer, List<Cell>> columns;
    private String beginIndex;
    private String endIndex;
    private String referenceContext;
    private String type;
    private String classe;
    private Dimension maxDimensions;
    private List<String> headersCleaned;
    private int keyColumn;
}
