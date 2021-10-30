package model;

import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Cell {

    @Data
    public static class Coordinate {
        private int row;
        private int column;
    }

    private String className;
    private String innerHTML;
    private boolean isHeader;
    private String type;
    @SerializedName("Coordinates")
    private Coordinate coordinates;
    private String cleanedText;
}
