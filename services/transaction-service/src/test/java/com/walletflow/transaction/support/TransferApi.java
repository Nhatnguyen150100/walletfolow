package com.walletflow.transaction.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.walletflow.common.security.AuthHeaders;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/** Gọi API của transaction-service với header danh tính như gateway gắn. */
public final class TransferApi {

    private TransferApi() {
    }

    public static Map<String, Object> body(UUID from, UUID to, long amount) {
        Map<String, Object> body = new HashMap<>();
        body.put("fromWalletId", from.toString());
        body.put("toWalletId", to.toString());
        body.put("amountMinor", amount);
        body.put("currency", "VND");
        return body;
    }

    public static Map<String, Object> topupBody(UUID wallet, long amount) {
        Map<String, Object> body = new HashMap<>();
        body.put("walletId", wallet.toString());
        body.put("amountMinor", amount);
        body.put("currency", "VND");
        return body;
    }

    public static ResponseEntity<String> topup(TestRestTemplate http, UUID tenant, UUID user,
                                               String key, Map<String, Object> body) {
        return post(http, "/api/v1/transactions/topup", tenant, user, key, body);
    }

    /** Trả body dạng chuỗi THÔ — test idempotency so sánh từng byte. */
    public static ResponseEntity<String> transfer(TestRestTemplate http, UUID tenant, UUID user,
                                                  String key, Map<String, Object> body) {
        return post(http, "/api/v1/transactions/transfer", tenant, user, key, body);
    }

    private static ResponseEntity<String> post(TestRestTemplate http, String path, UUID tenant, UUID user,
                                               String key, Map<String, Object> body) {
        HttpHeaders headers = identity(tenant, user);
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (key != null) {
            headers.set("Idempotency-Key", key);
        }
        return http.exchange(path, HttpMethod.POST, new HttpEntity<>(body, headers), String.class);
    }

    public static ResponseEntity<JsonNode> get(TestRestTemplate http, UUID tenant, UUID user, String txnId) {
        return http.exchange("/api/v1/transactions/" + txnId, HttpMethod.GET,
                new HttpEntity<>(identity(tenant, user)), JsonNode.class);
    }

    private static HttpHeaders identity(UUID tenant, UUID user) {
        HttpHeaders headers = new HttpHeaders();
        headers.set(AuthHeaders.TENANT_ID, tenant.toString());
        if (user != null) {
            headers.set(AuthHeaders.USER_ID, user.toString());
        }
        headers.set(AuthHeaders.USER_ROLE, "USER");
        return headers;
    }
}
