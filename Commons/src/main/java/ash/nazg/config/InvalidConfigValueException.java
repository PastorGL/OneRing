/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.config;

public class InvalidConfigValueException extends RuntimeException {
    public InvalidConfigValueException(String message, Exception cause) {
        super(message, cause);
    }

    public InvalidConfigValueException(String message) {
        super(message);
    }
}
