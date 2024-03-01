package net.dertod2.DatabaseLib.Data.Types;

import com.google.gson.*;
import net.dertod2.DatabaseLib.Data.IncludedTypes;
import net.dertod2.DatabaseLib.Exceptions.UnhandledDataTypeException;

import java.lang.reflect.Type;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class ListType extends AbstractType {
    private static final String OLD_REGEX = "¶";
    private static final String OLD_NULL = "NULL";
    private static String EMPTY_LIST;

    static {
        ListType.EMPTY_LIST = new Gson().toJson(new JsonArray());
    }

    public ListType() {
        super(List.class.getName(), ArrayList.class.getName());
    }

    public String setResult(Object value) {
        if (value == null) return ListType.EMPTY_LIST;

        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) value;
        if (list.isEmpty()) return ListType.EMPTY_LIST;

        String type = list.getFirst() == null ? null : list.getFirst().getClass().getTypeName();
        boolean customType = IncludedTypes.getByObject(type) == IncludedTypes.Unknown;

        JsonArray jsonArray = new JsonArray();

        if (customType) {
            for (Object object : list) {
                if (object == null) jsonArray.add(JsonNull.INSTANCE);
                else {
                    assert type != null;
                    AbstractType abstractType = this.abstractDatabase.getDataType(type);
                    if (abstractType != null) {
                        jsonArray.add(abstractType.setResult(object));
                    } else {
                        throw new UnhandledDataTypeException(object.getClass());
                    }
                }
            }
        } else {
            for (Object object : list) {
                switch (object) {
                    case null -> // Primitive type or String
                            jsonArray.add(JsonNull.INSTANCE);
                    case Boolean b -> jsonArray.add(b);
                    case Number number -> jsonArray.add(number);
                    case Character c -> jsonArray.add(c);
                    case String ignored -> jsonArray.add(object.toString());
                    case Timestamp ignored -> jsonArray.add(object.toString());
                    default -> {
                    }
                }
            }
        }

        return new Gson().toJson(jsonArray);
    }

    public Object getResult(String value, Type[] genericTypes) {
        IncludedTypes primitiveWrapper = genericTypes != null && genericTypes[0] != null ? IncludedTypes.getByObject(genericTypes[0].getTypeName()) : IncludedTypes.String;

        List<Object> list = new ArrayList<>();
        if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")) return list;

        if ((!value.contains("[") && !value.contains("]"))) { // Test if old layout, so this is compatible to old save layout
            String[] splitter = value.split(ListType.OLD_REGEX);
            switch (primitiveWrapper) {
                case Boolean:
                    for (String split : splitter) {
                        if (split.equals(ListType.OLD_NULL)) {
                            list.add(null);
                            continue;
                        }

                        list.add(Boolean.valueOf(split));
                    }
                    break;
                case Byte:
                    for (String split : splitter) {
                        if (split.equals(ListType.OLD_NULL)) {
                            list.add(null);
                            continue;
                        }

                        list.add(Byte.valueOf(split));
                    }
                    break;
                case Char:
                    for (String split : splitter) {
                        if (split.equals(ListType.OLD_NULL)) {
                            list.add(null);
                            continue;
                        }

                        list.add(split.charAt(0));
                    }
                    break;
                case Double:
                    for (String split : splitter) {
                        if (split.equals(ListType.OLD_NULL)) {
                            list.add(null);
                            continue;
                        }

                        list.add(Double.valueOf(split));
                    }
                    break;
                case Float:
                    for (String split : splitter) {
                        if (split.equals(ListType.OLD_NULL)) {
                            list.add(null);
                            continue;
                        }

                        list.add(Float.valueOf(split));
                    }
                    break;
                case Int:
                    for (String split : splitter) {
                        if (split.equals(ListType.OLD_NULL)) {
                            list.add(null);
                            continue;
                        }

                        list.add(Integer.valueOf(split));
                    }
                    break;
                case Long:
                    for (String split : splitter) {
                        if (split.equals(ListType.OLD_NULL)) {
                            list.add(null);
                            continue;
                        }

                        list.add(Long.valueOf(split));
                    }
                    break;
                case Short:
                    for (String split : splitter) {
                        if (split.equals(ListType.OLD_NULL)) {
                            list.add(null);
                            continue;
                        }

                        list.add(Short.valueOf(split));
                    }
                    break;
                case String:
                    for (String split : splitter) {
                        if (split.equals(ListType.OLD_NULL)) {
                            list.add(null);
                            continue;
                        }

                        list.add(split);
                    }
                    break;
                case Timestamp:
                    for (String split : splitter) {
                        if (split.equals(ListType.OLD_NULL)) {
                            list.add(null);
                            continue;
                        }

                        list.add(Timestamp.valueOf(split));
                    }
                    break;
                case Unknown:
                    AbstractType abstractType = this.abstractDatabase.getDataType(genericTypes[0].getTypeName());
                    for (String split : splitter) {
                        if (split.equals(ListType.OLD_NULL)) {
                            list.add(null);
                            continue;
                        }

                        list.add(abstractType.getResult(split, genericTypes));
                    }
                    break;
            }

            return list;
        }

        JsonElement jsonMain = JsonParser.parseString(value);

        JsonArray jsonArray = jsonMain.getAsJsonArray();
        Iterator<JsonElement> iterator = jsonArray.iterator();

        if (primitiveWrapper == IncludedTypes.Unknown) {
            AbstractType abstractType = this.abstractDatabase.getDataType(genericTypes[0].getTypeName());
            while (iterator.hasNext()) {
                JsonElement jsonElement = iterator.next();
                list.add(jsonElement.isJsonNull() ? null : abstractType.getResult(jsonElement.getAsString(), genericTypes.length > 1 ? Arrays.copyOfRange(genericTypes, 1, genericTypes.length) : null));
            }
        } else {
            switch (primitiveWrapper) {
                case Boolean:
                    while (iterator.hasNext()) {
                        JsonElement jsonElement = iterator.next();
                        list.add(jsonElement.isJsonNull() ? null : jsonElement.getAsBoolean());
                    }

                    break;
                case Byte:
                    while (iterator.hasNext()) {
                        JsonElement jsonElement = iterator.next();
                        list.add(jsonElement.isJsonNull() ? null : jsonElement.getAsByte());
                    }

                    break;
                case Char:
                    while (iterator.hasNext()) {
                        JsonElement jsonElement = iterator.next();
                        list.add(jsonElement.isJsonNull() ? null : jsonElement.getAsString().charAt(0));
                    }

                    break;
                case Double:
                    while (iterator.hasNext()) {
                        JsonElement jsonElement = iterator.next();
                        list.add(jsonElement.isJsonNull() ? null : jsonElement.getAsDouble());
                    }

                    break;
                case Float:
                    while (iterator.hasNext()) {
                        JsonElement jsonElement = iterator.next();
                        list.add(jsonElement.isJsonNull() ? null : jsonElement.getAsFloat());
                    }

                    break;
                case Int:
                    while (iterator.hasNext()) {
                        JsonElement jsonElement = iterator.next();
                        list.add(jsonElement.isJsonNull() ? null : jsonElement.getAsInt());
                    }

                    break;
                case Long:
                    while (iterator.hasNext()) {
                        JsonElement jsonElement = iterator.next();
                        list.add(jsonElement.isJsonNull() ? null : jsonElement.getAsLong());
                    }

                    break;
                case Short:
                    while (iterator.hasNext()) {
                        JsonElement jsonElement = iterator.next();
                        list.add(jsonElement.isJsonNull() ? null : jsonElement.getAsShort());
                    }

                    break;
                case String:
                    while (iterator.hasNext()) {
                        JsonElement jsonElement = iterator.next();
                        list.add(jsonElement.isJsonNull() ? null : jsonElement.getAsString());
                    }

                    break;
                case Timestamp:
                    while (iterator.hasNext()) {
                        JsonElement jsonElement = iterator.next();
                        list.add(jsonElement.isJsonNull() ? null : Timestamp.valueOf(jsonElement.getAsString()));
                    }

                    break;
            }
        }

        return list;
    }
}