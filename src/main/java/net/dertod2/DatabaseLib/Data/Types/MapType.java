package net.dertod2.DatabaseLib.Data.Types;

import com.google.gson.*;
import net.dertod2.DatabaseLib.Data.IncludedTypes;
import net.dertod2.DatabaseLib.Exceptions.UnhandledDataTypeException;

import java.lang.reflect.Type;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class MapType extends AbstractType {
    private static String EMPTY_MAP;

    static {
        JsonArray mainArray = new JsonArray();

        JsonArray jsonArray = new JsonArray();
        jsonArray.add(JsonNull.INSTANCE);
        jsonArray.add(JsonNull.INSTANCE);

        mainArray.add(jsonArray);

        MapType.EMPTY_MAP = new Gson().toJson(mainArray);
    }

    public MapType() {
        super(Map.class.getName(), HashMap.class.getName());
    }

    public String setResult(Object value) {
        if (value == null) return MapType.EMPTY_MAP;

        @SuppressWarnings("unchecked")
        Map<Object, Object> map = (Map<Object, Object>) value;
        if (map.isEmpty()) return MapType.EMPTY_MAP;

        String keyType = null;
        String valueType = null;

        boolean customKey = false;
        boolean customValue = false;

        JsonArray mainArray = new JsonArray();
        for (Entry<Object, Object> entry : map.entrySet()) {
            JsonArray jsonArray = new JsonArray();
            Object key = entry.getKey();
            Object val = entry.getValue();

            if (keyType == null) {
                keyType = key == null ? null : key.getClass().getTypeName();
                customKey = IncludedTypes.getByObject(keyType) == IncludedTypes.Unknown;
            }

            if (valueType == null) {
                valueType = val == null ? null : val.getClass().getTypeName();
                customValue = IncludedTypes.getByObject(valueType) == IncludedTypes.Unknown;
            }

            // Do the entry
            if (customKey) {
                if (key == null) jsonArray.add(JsonNull.INSTANCE);
                else {
                    AbstractType abstractType = this.abstractDatabase.getDataType(keyType);
                    if (abstractType != null) {
                        jsonArray.add(abstractType.setResult(key));
                    } else {
                        throw new UnhandledDataTypeException(key.getClass());
                    }
                }
            } else {
                switch (key) {
                    case null -> jsonArray.add(JsonNull.INSTANCE);
                    case Boolean b -> jsonArray.add(b);
                    case Number number -> jsonArray.add(number);
                    case Character c -> jsonArray.add(c);
                    case String ignored -> jsonArray.add(key.toString());
                    case Timestamp ignored -> jsonArray.add(key.toString());
                    default -> {
                    }
                }
            }

            if (customValue) {
                if (val == null) jsonArray.add(JsonNull.INSTANCE);
                else {
                    AbstractType abstractType = this.abstractDatabase.getDataType(valueType);
                    if (abstractType != null) {
                        jsonArray.add(abstractType.setResult(val));
                    } else {
                        throw new UnhandledDataTypeException(val.getClass());
                    }
                }
            } else {
                switch (val) {
                    case null -> jsonArray.add(JsonNull.INSTANCE);
                    case Boolean b -> jsonArray.add(b);
                    case Number number -> jsonArray.add(number);
                    case Character c -> jsonArray.add(c);
                    case String s -> jsonArray.add(s);
                    case Timestamp ignored -> jsonArray.add(val.toString());
                    default -> {
                    }
                }
            }

            mainArray.add(jsonArray);
        }

        return new Gson().toJson(mainArray);
    }

    public Object getResult(String value, Type[] genericTypes) {
        Map<Object, Object> map = new HashMap<>();
        if (value == null || value.isEmpty()) return map;

        JsonArray mainArray = JsonParser.parseString(value).getAsJsonArray();
        Iterator<JsonElement> iterator = mainArray.iterator();

        String keyType = genericTypes[0].getTypeName();
        String valueType = genericTypes[1].getTypeName();

        IncludedTypes customKey = IncludedTypes.getByObject(keyType);
        IncludedTypes customValue = IncludedTypes.getByObject(valueType);

        while (iterator.hasNext()) {
            JsonArray jsonArray = iterator.next().getAsJsonArray();

            JsonElement jsonKey = jsonArray.get(0);
            JsonElement jsonValue = jsonArray.get(1);

            Object key = null;
            Object val = null;

            if (customKey == IncludedTypes.Unknown) {
                AbstractType abstractKey = this.abstractDatabase.getDataType(keyType);
                key = jsonKey.isJsonNull() ? null : abstractKey.getResult(jsonKey.getAsString(), Arrays.copyOfRange(genericTypes, 1, genericTypes.length));
            } else {
                switch (customKey) {
                    case Boolean -> key = jsonKey.isJsonNull() ? null : jsonKey.getAsBoolean();
                    case Byte -> key = jsonKey.isJsonNull() ? null : jsonKey.getAsByte();
                    case Char -> key = jsonKey.isJsonNull() ? null : jsonKey.getAsString().charAt(0);
                    case Double -> key = jsonKey.isJsonNull() ? null : jsonKey.getAsDouble();
                    case Float -> key = jsonKey.isJsonNull() ? null : jsonKey.getAsFloat();
                    case Int -> key = jsonKey.isJsonNull() ? null : jsonKey.getAsInt();
                    case Long -> key = jsonKey.isJsonNull() ? null : jsonKey.getAsLong();
                    case Short -> key = jsonKey.isJsonNull() ? null : jsonKey.getAsShort();
                    case String -> key = jsonKey.isJsonNull() ? null : jsonKey.getAsString();
                    case Timestamp -> key = jsonKey.isJsonNull() ? null : Timestamp.valueOf(jsonKey.getAsString());
                    default -> {
                    }
                }
            }

            if (customValue == IncludedTypes.Unknown) {
                AbstractType abstractKey = this.abstractDatabase.getDataType(valueType);
                val = jsonValue.isJsonNull() ? null : abstractKey.getResult(jsonValue.getAsString(), Arrays.copyOfRange(genericTypes, 1, genericTypes.length));
            } else {
                switch (customKey) {
                    case Boolean:
                        val = jsonValue.isJsonNull() ? null : jsonValue.getAsBoolean();
                        break;
                    case Byte:
                        val = jsonValue.isJsonNull() ? null : jsonValue.getAsByte();
                        break;
                    case Char:
                        val = jsonValue.isJsonNull() ? null : jsonValue.getAsString().charAt(0);
                        break;
                    case Double:
                        val = jsonValue.isJsonNull() ? null : jsonValue.getAsDouble();
                        break;
                    case Float:
                        val = jsonValue.isJsonNull() ? null : jsonValue.getAsFloat();
                        break;
                    case Int:
                        val = jsonValue.isJsonNull() ? null : jsonValue.getAsInt();
                        break;
                    case Long:
                        val = jsonValue.isJsonNull() ? null : jsonValue.getAsLong();
                        break;
                    case Short:
                        val = jsonValue.isJsonNull() ? null : jsonValue.getAsShort();
                        break;
                    case String:
                        val = jsonValue.isJsonNull() ? null : jsonValue.getAsString();
                        break;
                    case Timestamp:
                        val = jsonValue.isJsonNull() ? null : Timestamp.valueOf(jsonValue.getAsString());
                        break;
                    case Unknown:
                        break;
                }
            }

            map.put(key, val);
        }

        return map;
    }
}