package ash.nazg.spark;

import ash.nazg.config.tdl.metadata.OperationMeta;

public class OperationInfo {
    public final Class<? extends Operation> opClass;
    public final OperationMeta meta;

    public OperationInfo(Class<? extends Operation> opClass, OperationMeta meta) {
        this.opClass = opClass;
        this.meta = meta;
    }
}
