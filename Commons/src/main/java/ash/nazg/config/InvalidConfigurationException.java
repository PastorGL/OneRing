/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.config;

public class InvalidConfigurationException extends RuntimeException {
    public InvalidConfigurationException(String message, Exception cause) {
        super(message, cause);
    }

    public InvalidConfigurationException(String message) {
        super(message);
    }
}
