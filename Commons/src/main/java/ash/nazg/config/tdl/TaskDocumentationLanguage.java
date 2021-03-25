/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.config.tdl;

import java.util.ArrayList;
import java.util.List;

public class TaskDocumentationLanguage {
    public static class Package extends Pair {
        public List<Pair> ops = new ArrayList<>();
        public List<Pair> ins = new ArrayList<>();
        public List<Pair> outs = new ArrayList<>();

        public Package(String name, String descr) {
            super(name, descr);
        }

        public List<Pair> getOps() {
            return ops;
        }
        public List<Pair> getIns() {
            return ins;
        }
        public List<Pair> getOuts() {
            return outs;
        }
    }

    public static class Operation {
        public String verb;
        public String descr;

        public String pkg;

        public Input positionalInputs;
        public Integer positionalMin;
        public List<Input> namedInputs = new ArrayList<>();

        public Output positionalOutputs;
        public List<Output> namedOutputs = new ArrayList<>();

        public List<Parameter> mandatoryParameters = new ArrayList<>();
        public List<Parameter> optionalParameters = new ArrayList<>();
        public List<Parameter> dynamicParameters = new ArrayList<>();

        public String getVerb() {
            return verb;
        }

        public String getDescr() {
            return descr;
        }

        public String getPkg() {
            return pkg;
        }

        public List<Input> getNamedInputs() {
            return namedInputs;
        }

        public List<Output> getNamedOutputs() {
            return namedOutputs;
        }

        public List<Parameter> getMandatoryParameters() {
            return mandatoryParameters;
        }

        public List<Parameter> getOptionalParameters() {
            return optionalParameters;
        }

        public List<Parameter> getDynamicParameters() {
            return dynamicParameters;
        }

        public Input getPositionalInputs() {
            return positionalInputs;
        }

        public Integer getPositionalMin() {
            return positionalMin;
        }

        public Output getPositionalOutputs() {
            return positionalOutputs;
        }
    }

    public static class Pair {
        public String name;
        public String descr;

        public Pair(String name, String descr) {
            this.name = name;
            this.descr = descr;
        }

        public String getName() {
            return name;
        }

        public String getDescr() {
            return descr;
        }
    }

    public static class Parameter extends Pair {
        public List<Pair> values = new ArrayList<>();
        public String type;
        public Pair defaults;

        public Parameter(String name, String descr) {
            super(name, descr);
        }

        public List<Pair> getValues() {
            return values;
        }

        public String getType() {
            return type;
        }

        public Pair getDefaults() {
            return defaults;
        }
    }

    public static class Input extends Pair {
        public List<String> type;
        public boolean columnar;

        public Input(String name, String descr) {
            super(name, descr);
        }

        public List<String> getType() {
            return type;
        }

        public boolean isColumnar() {
            return columnar;
        }
    }

    public static class Output extends Input {
        public List<Pair> generated = new ArrayList<>();

        public Output(String name, String descr) {
            super(name, descr);
        }

        public List<Pair> getGenerated() {
            return generated;
        }
    }

    public static class Adapter extends Pair {
        public String proto;

        public String pkg;

        public String kind;

        public Adapter(String kind, String name, String descr) {
            super(name, descr);
            this.kind = kind;
        }

        public String getKind() {
            return kind;
        }

        public String getProto() {
            return proto;
        }

        public String getDescr() {
            return descr;
        }

        public String getPkg() {
            return pkg;
        }
    }
}
