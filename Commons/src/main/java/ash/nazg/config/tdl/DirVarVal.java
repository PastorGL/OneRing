package ash.nazg.config.tdl;

public class DirVarVal {
    public final Directive dir;
    public final String variable;
    public final String value;

    public DirVarVal(String dir, String variable, String value) {
        this.dir = Directive.valueOf(dir.toUpperCase());
        this.variable = variable;
        this.value = value;
    }

    public DirVarVal(String dir) {
        this.dir = Directive.valueOf(dir.toUpperCase());
        this.variable = "";
        this.value = null;
    }
}
