package io.toonformat.toon4s;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.toonformat.toon4s.error.DecodeError;
import io.toonformat.toon4s.error.EncodeError;

/**
 * The Java usage shown in the README, compiled with the build so it cannot rot. JavaQuickstartSpec
 * runs it.
 */
public final class JavaQuickstart {

    private JavaQuickstart() {}

    /** Encodes Java collections; a LinkedHashMap keeps its field order in the output. */
    public static String encode() {
        Map<String, Object> ada = new LinkedHashMap<>();
        ada.put("id", 1);
        ada.put("name", "Ada");
        Map<String, Object> bob = new LinkedHashMap<>();
        bob.put("id", 2);
        bob.put("name", "Bob");
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("users", List.of(ada, bob));

        ToonResult<EncodeError, String> result = JToon4s.encode(payload);
        if (!result.isSuccess()) {
            throw new IllegalStateException(result.getError().getMessage());
        }
        return result.getValue();
    }

    /** Decodes TOON into the JsonValue tree; a failure carries a DecodeError. */
    public static JsonValue decode(String toon) {
        ToonResult<DecodeError, JsonValue> result = JToon4s.decode(toon);
        if (!result.isSuccess()) {
            throw new IllegalArgumentException(result.getError().getMessage());
        }
        return result.getValue();
    }

    /** The error message for malformed input, without throwing. */
    public static String decodeError(String toon) {
        ToonResult<DecodeError, JsonValue> result = JToon4s.decode(toon);
        return result.isSuccess() ? null : result.getError().getMessage();
    }
}
