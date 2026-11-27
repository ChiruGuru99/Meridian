package io.meridian.ledger;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/** Protects read-only ledger data without putting credentials on health probes. */
@Component
public class HttpApiKeyFilter extends OncePerRequestFilter {
    private final byte[] expected;

    public HttpApiKeyFilter(@Value("${ledger.http.api-key}") String apiKey) {
        this.expected = ("Bearer " + apiKey).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return !request.getRequestURI().startsWith("/api/");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String authorization = request.getHeader("Authorization");
        byte[] actual = authorization == null ? new byte[0] : authorization.getBytes(StandardCharsets.UTF_8);
        if (!MessageDigest.isEqual(expected, actual)) {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            response.setContentType("application/problem+json");
            response.setHeader("WWW-Authenticate", "Bearer realm=\"meridian-ledger\"");
            response.getWriter().write("{\"code\":\"unauthenticated\",\"detail\":\"a valid ledger credential is required\"}");
            return;
        }
        chain.doFilter(request, response);
    }
}
