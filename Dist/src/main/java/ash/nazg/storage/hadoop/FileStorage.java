package ash.nazg.storage.hadoop;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.storage.StorageAdapter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

public class FileStorage {
    public static final Map<String, Class<? extends CompressionCodec>> CODECS = new HashMap<>();

    static {
        CODECS.put("gz", GzipCodec.class);
        CODECS.put("gzip", GzipCodec.class);
        CODECS.put("bz2", BZip2Codec.class);
        CODECS.put("snappy", SnappyCodec.class);
        CODECS.put("lz4", Lz4Codec.class);
        CODECS.put("zstd", ZStandardCodec.class);
    }

    /**
     * Create a list of file groups from a glob pattern.
     *
     * @param inputPath glob pattern(s) to files
     * @return list of groups in form of Tuple3 [ group name, path, regex ]
     * @throws InvalidConfigValueException if glob pattern is incorrect
     */
    public static List<Tuple2<String, String>> srcDestGroup(String inputPath) throws InvalidConfigValueException {
        List<Tuple2<String, String>> ret = new ArrayList<>();

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
            Matcher m = StorageAdapter.PATH_PATTERN.matcher(split);
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
                                if (groupingSub < 0) {
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
                                if (groupingSub < 0) {
                                    groupingSub = s - 1;
                                }
                                break;
                            }
                            case '*': {
                                translatedSub.append(".*");
                                if (groupingSub < 0) {
                                    groupingSub = s - 1;
                                }
                                break;
                            }
                            case '[': {
                                inSet = true;
                                translatedSub.append(c);
                                if (groupingSub < 0) {
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
                ret.add(new Tuple2<>(
                        rootPath + "/" + joined + groupSub,
                        ".*/" + StringUtils.join(transSubs.subList(groupingSub, transSubs.size()), '/') + ".*"
                ));
            } else {
                throw new InvalidConfigValueException("Glob pattern '" + split + "' must have protocol specification and its first path part must be not a grouping candidate");
            }
        }

        return ret;
    }

    public static String suffix(String name) {
        String suffix = "";

        if (!StringUtils.isEmpty(name)) {
            String[] parts = name.split("\\.");
            if (parts.length > 1) {
                suffix = parts[parts.length - 1];
            }
        }

        return suffix;
    }
}
