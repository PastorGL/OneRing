package ash.nazg.dist;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.WrapperConfig;
import ash.nazg.storage.Adapters;
import org.apache.commons.lang3.StringUtils;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

public class DistCpSettings {
    public final boolean toCluster;
    public final boolean fromCluster;
    public final boolean anyDirection;

    public final String inputDir;
    public final String outputDir;
    public final String wrapperStorePath;

    private DistCpSettings(String wrap, String inputDir, String outputDir, String wrapperStorePath) {
        CpDirection cpDirection = CpDirection.parse(wrap);
        this.toCluster = cpDirection.toCluster;
        this.fromCluster = cpDirection.fromCluster;
        this.anyDirection = cpDirection.anyDirection;

        this.inputDir = inputDir;
        this.outputDir = outputDir;
        this.wrapperStorePath = wrapperStorePath;
    }

    public static DistCpSettings fromConfig(WrapperConfig wrapperConfig) {
        String wrap = wrapperConfig.getDistCpProperty("wrap", "none");
        String inputDir = wrapperConfig.getDistCpProperty("dir.to", "hdfs:///input");
        String outputDir = wrapperConfig.getDistCpProperty("dir.from", "hdfs:///output");
        String wrapperStorePath = wrapperConfig.getDistCpProperty("store", null);

        return new DistCpSettings(wrap, inputDir, outputDir, wrapperStorePath);
    }

    public static List<Tuple3<String, String, String>> srcDestGroup(String inputPath) throws InvalidConfigValueException {
        List<Tuple3<String, String, String>> ret = new ArrayList<>();

        int curlyLevel = 0;

        List<String> splits = new ArrayList<>();

        StringBuilder current = new StringBuilder();
        for (int i = 0; i < inputPath.length(); i++) {
            char c = inputPath.charAt(i);

            switch (c) {
                case '\\': {
                    current.append(c).append(inputPath.charAt(++i));
                    break;
                }
                case '{': {
                    curlyLevel++;
                    current.append(c);
                    break;
                }
                case '}': {
                    curlyLevel--;
                    current.append(c);
                    break;
                }
                case ',': {
                    if (curlyLevel == 0) {
                        splits.add(current.toString());
                        current = new StringBuilder();
                    } else {
                        current.append(c);
                    }
                    break;
                }
                default: {
                    current.append(c);
                }
            }
        }
        splits.add(current.toString());

        for (String split : splits) {
            Matcher m = Adapters.PATH_PATTERN.matcher(split);
            if (m.matches()) {
                String rootPath = m.group(1);
                String path = m.group(2);

                List<String> transSubs = new ArrayList<>();
                int groupingSub = -1;

                String sub = path;
                int s = 0;

                nextSub:
                while (true) {
                    StringBuilder translatedSub = new StringBuilder();

                    curlyLevel = 0;
                    boolean inSet = false;
                    for (int i = 0; i < sub.length(); i++) {
                        char c = sub.charAt(i);

                        switch (c) {
                            case '/': {
                                if (!inSet && (curlyLevel == 0)) {
                                    transSubs.add(translatedSub.toString());

                                    if (++i != sub.length()) {
                                        s++;

                                        sub = sub.substring(i);
                                        continue nextSub;
                                    } else {
                                        break nextSub;
                                    }
                                } else {
                                    translatedSub.append(c);
                                }
                                break;
                            }
                            case '\\': {
                                translatedSub.append(c);
                                if (++i != sub.length()) {
                                    translatedSub.append(sub.charAt(i));
                                }
                                break;
                            }
                            case '$':
                            case '(':
                            case ')':
                            case '|':
                            case '+': {
                                translatedSub.append('\\').append(c);
                                break;
                            }
                            case '{': {
                                curlyLevel++;
                                translatedSub.append("(?:");
                                if (groupingSub < 0)  {
                                   groupingSub = s - 1;
                                }
                                break;
                            }
                            case '}': {
                                if (curlyLevel > 0) {
                                    curlyLevel--;
                                    translatedSub.append(")");
                                } else {
                                    translatedSub.append(c);
                                }
                                break;
                            }
                            case ',': {
                                translatedSub.append((curlyLevel > 0) ? '|' : c);
                                break;
                            }
                            case '?': {
                                translatedSub.append('.');
                                if (groupingSub < 0)  {
                                    groupingSub = s - 1;
                                }
                                break;
                            }
                            case '*': {
                                translatedSub.append(".*");
                                if (groupingSub < 0)  {
                                    groupingSub = s - 1;
                                }
                                break;
                            }
                            case '[': {
                                inSet = true;
                                translatedSub.append(c);
                                if (groupingSub < 0)  {
                                    groupingSub = s - 1;
                                }
                                break;
                            }
                            case '^': {
                                if (inSet) {
                                    translatedSub.append('\\');
                                }
                                translatedSub.append(c);
                                break;
                            }
                            case '!': {
                                translatedSub.append(inSet && ('[' == sub.charAt(i - 1)) ? '^' : '!');
                                break;
                            }
                            case ']': {
                                inSet = false;
                                translatedSub.append(c);
                                break;
                            }
                            default: {
                                translatedSub.append(c);
                            }
                        }
                    }

                    if (inSet || (curlyLevel > 0)) {
                        throw new InvalidConfigValueException("Glob pattern '" + split + "' contains unbalances range [] or braces {} definition");
                    }

                    if (groupingSub < 0) {
                        groupingSub = s;
                    }

                    transSubs.add(translatedSub.toString());

                    break;
                }

                if (s < 1) {
                    throw new InvalidConfigValueException("Glob pattern '" + split + "' has no valid grouping candidate part in the path");
                }

                String groupSub = transSubs.get(groupingSub);

                transSubs.remove(groupingSub);
                transSubs.add(groupingSub, "(" + groupSub + ")");

                String joined = StringUtils.join(transSubs.subList(0, groupingSub), '/');
                if (!joined.isEmpty()) {
                    joined += "/";
                }
                ret.add(new Tuple3<>(groupSub,
                        rootPath + "/" + joined + groupSub,
                        ".*/" + StringUtils.join(transSubs.subList(groupingSub, transSubs.size()), '/') + ".*"
                ));
            } else {
                throw new InvalidConfigValueException("Glob pattern '" + split + "' must have protocol specification and its first path part must be not a grouping candidate");
            }
        }

        return ret;
    }
}
