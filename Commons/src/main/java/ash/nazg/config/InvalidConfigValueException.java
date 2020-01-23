package ash.nazg.config;

public class InvalidConfigValueException extends RuntimeException {
    public InvalidConfigValueException(String message, Exception cause) {
        super(message, cause);
    }

    public InvalidConfigValueException(String message) {
        super(message);
    }
}
