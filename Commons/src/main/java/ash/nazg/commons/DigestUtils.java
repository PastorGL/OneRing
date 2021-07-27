package ash.nazg.commons;

import scala.Tuple2;

import java.security.MessageDigest;
import java.security.Provider;
import java.security.Security;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class DigestUtils {
    static public final List<Tuple2<String, String>> DIGEST_ALGOS = new ArrayList<>();

    static {
        String digest = MessageDigest.class.getSimpleName();

        Provider[] providers = Security.getProviders();
        for (Provider provider : providers) {
            Set<Provider.Service> services = provider.getServices();
            for (Provider.Service service : services) {
                if (service.getType().equalsIgnoreCase(digest)) {
                    DIGEST_ALGOS.add(new Tuple2<>(provider.getName(), service.getAlgorithm()));
                }
            }
        }
    }
}
