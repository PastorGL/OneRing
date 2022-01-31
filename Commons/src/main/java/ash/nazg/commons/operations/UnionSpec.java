package ash.nazg.commons.operations;

import ash.nazg.config.tdl.metadata.DefinitionEnum;

public enum UnionSpec implements DefinitionEnum {
    CONCAT("Just concatenate inputs, don't look into records"),
    XOR("Only emit records that occur strictly in one input RDD"),
    AND("Only emit records that occur in all input RDDs");

    private final String descr;

    UnionSpec(String descr) {
        this.descr = descr;
    }

    @Override
    public String descr() {
        return descr;
    }
}
