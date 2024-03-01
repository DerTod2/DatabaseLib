package net.dertod2.DatabaseLib.Data.Types;

import java.lang.reflect.Type;
import java.util.UUID;

public class UniqueIdType extends AbstractType {

    public UniqueIdType() {
        super(UUID.class.getName());
    }

    public String setResult(Object value) {
        return value.toString();
    }

    public Object getResult(String value, Type[] genericTypes) {
        return UUID.fromString(value);
    }
}