package model;

import com.google.gson.*;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableDeserializer implements JsonDeserializer<Table> {
    @Override
    public Table deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {

        Gson gson = new Gson();
        JsonObject jsonTable = jsonElement.getAsJsonObject();
        Table table = gson.fromJson(jsonTable, Table.class);
        Map<Integer, List<Cell>> columns = new HashMap<>();

        jsonTable.get("cells").getAsJsonArray()
                .forEach(c -> {
                    Cell cell = gson.fromJson(c, Cell.class);
                    int col = cell.getCoordinates().getColumn();
                    if (!columns.containsKey(col)) {
                        List<Cell> cells = new ArrayList<>();
                        cells.add(cell);
                        columns.put(col, cells);
                    } else {
                        columns.get(col).add(cell);
                    }
                });
        table.setColumns(columns);

        return table;
    }
}
