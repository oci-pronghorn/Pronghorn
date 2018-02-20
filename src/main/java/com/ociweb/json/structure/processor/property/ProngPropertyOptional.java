package com.ociweb.json.structure.processor.property;

public enum ProngPropertyOptional {
    full(true, true),
    onlyOnInit(true, false),
    onlyOnSet(false, true),
    not(false, false);

    private final boolean getterNullable;
    private final boolean setterNullable;

    ProngPropertyOptional(boolean getterNullable, boolean setterNullable) {
        this.getterNullable = getterNullable;
        this.setterNullable = setterNullable;
    }

    public ProngPropertyOptional accumeOptional(boolean optional, boolean asGetter) {
        if (asGetter) {
            return get(optional, this.setterNullable);
        }
        return get(this.getterNullable, optional);
    }

    public static ProngPropertyOptional get(boolean getterNullable, boolean setterNullable) {
        if (getterNullable && setterNullable) {
            return ProngPropertyOptional.full;
        }
        else  if (getterNullable) {
            return ProngPropertyOptional.onlyOnInit;
        }
        else  if (setterNullable) {
            return ProngPropertyOptional.onlyOnSet;
        }
        else  {
            return ProngPropertyOptional.not;
        }
    }

    public boolean getterNullable() {
        return getterNullable;
    }

    public boolean setterNullable() {
        return setterNullable;
    }

    public boolean isNullable() {
        return getterNullable | setterNullable;
    }
}
