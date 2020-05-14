package ash.nazg.dist;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.WrapperConfig;
import ash.nazg.storage.Adapters;
import org.apache.commons.lang3.StringUtils;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
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

                String[] subs = path.split("/");
                String[] transSubs = new String[subs.length];
                int groupingSub = -1;

                int s = 0;
                boolean translatedOnce = false;
                for (String sub : subs) {
                    boolean translated = false;

                    StringBuilder translatedSub = new StringBuilder();
                    curlyLevel = 0;
                    int set = 0;
                    for (int i = 0; i < sub.length(); i++) {
                        char c = sub.charAt(i);

                        switch (c) {
                            case '\\': {
                                translatedSub.append(c).append(sub.charAt(++i));
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
                                translatedOnce = translated = true;
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
                                translatedOnce = translated = true;
                                break;
                            }
                            case '*': {
                                translatedSub.append(".*");
                                translatedOnce = translated = true;
                                break;
                            }
                            case '[': {
                                set++;
                                translatedSub.append(c);
                                translatedOnce = translated = true;
                                break;
                            }
                            case '^' : {
                                if (set > 0) {
                                    translatedSub.append('\\');
                                }
                                translatedSub.append(c);
                                break;
                            }
                            case '!': {
                                translatedSub.append((set > 0) && ('[' == sub.charAt(i - 1)) ? '^' : '!');
                                break;
                            }
                            case ']': {
                                set = 0;
                                translatedSub.append(c);
                                break;
                            }
                            default : {
                                translatedSub.append(c);
                            }
                        }
                    }

                    if (translated) {
                        if (groupingSub < 0) {
                            groupingSub = s - 1;
                        }
                    }

                    transSubs[s] = translatedSub.toString();

                    s++;
                }

                if (!translatedOnce) {
                    groupingSub = subs.length - 1;
                }

                if (groupingSub < 0) {
                    throw new InvalidConfigValueException("Glob pattern '" + split + "' has no valid grouping candidate part in the path");
                }

                String groupSub = transSubs[groupingSub] + "";

                transSubs[groupingSub] = "(" + groupSub + ")";

                String joined = StringUtils.join(Arrays.copyOfRange(transSubs, 0, groupingSub), '/');
                if (!joined.isEmpty()) {
                    joined += "/";
                }
                ret.add(new Tuple3<>(groupSub,
                        rootPath + "/" + joined + groupSub,
                        ".*/" + StringUtils.join(Arrays.copyOfRange(transSubs, groupingSub, transSubs.length), '/') + ".*"
                ));
            } else {
                throw new InvalidConfigValueException("Glob pattern '" + split + "' must have protocol specification and its first path part must be not a grouping candidate");
            }
        }

        return ret;
    }
}
