package de.umr.raft.raftlogreplicationdemo.models.eventstore;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.security.SecureRandom;
import java.util.Base64;

@RequiredArgsConstructor
public class ClearStreamRequest {
    @Getter @NonNull final String streamName;
    @Getter @NonNull final String token;

    private static String createToken() {
        SecureRandom secureRandom = new SecureRandom();
        Base64.Encoder base64Encoder = Base64.getUrlEncoder();
        byte[] randomBytes = new byte[24];
        secureRandom.nextBytes(randomBytes);
        return base64Encoder.encodeToString(randomBytes);
    }

    public static ClearStreamRequest create(String streamName) {
        String token = createToken();
        return new ClearStreamRequest(streamName, token);
    }
}
