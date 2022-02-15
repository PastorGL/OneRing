package ash.nazg.commons.operations;

import ash.nazg.config.tdl.metadata.DefinitionEnum;

public enum JoinSpec implements DefinitionEnum {
    INNER("Inner join"),
    LEFT("Left outer join"),
    RIGHT("Right outer join"),
    OUTER("Full outer join");

    private final String descr;

    JoinSpec(String descr) {
        this.descr = descr;
    }

    @Override
    public String descr() {
        return descr;
    }
}
