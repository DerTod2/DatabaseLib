package net.dertod2.DatabaseLib.Data;

import net.dertod2.DatabaseLib.Exceptions.UnsupportedMapDepthException;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public class RowHelper {

    protected static String[] extractFieldTypes(Field field) {
        String name = field.getType().getName();
        Type genericType = field.getGenericType();

        if (genericType instanceof ParameterizedType) {
            Type[] arguments = ((ParameterizedType) genericType).getActualTypeArguments();

            if (arguments.length == 1) { // List
                return new String[]{name, arguments[0].getTypeName()};
            } else if (arguments.length == 2) { // Normal Map
                if (arguments[1] instanceof ParameterizedType) { // Bigger Maps, not supported!
                    throw new UnsupportedMapDepthException();
                } else { // Normal Map
                    return new String[]{name, arguments[0].getTypeName(), arguments[1].getTypeName()};
                }
            } else { // Bigger Maps, not supported!
                throw new UnsupportedMapDepthException();
            }
        } else {
            return new String[]{getPrimitiveObject(name)};
        }
    }

    public static String getPrimitiveObject(String name) {
        return switch (name) {
            case "boolean" -> Boolean.class.getName();
            case "byte" -> Byte.class.getName();
            case "char" -> Character.class.getName();
            case "double" -> Double.class.getName();
            case "float" -> Float.class.getName();
            case "int" -> Integer.class.getName();
            case "long" -> Long.class.getName();
            case "short" -> Short.class.getName();
            default -> name;
        };
    }
}