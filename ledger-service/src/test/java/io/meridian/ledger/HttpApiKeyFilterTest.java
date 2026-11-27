package io.meridian.ledger;

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import static org.assertj.core.api.Assertions.assertThat;

class HttpApiKeyFilterTest {
    private final HttpApiKeyFilter filter =
            new HttpApiKeyFilter("test-ledger-read-key-0123456789abcdef");

    @Test
    void rejectsUnauthenticatedReadApiRequests() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/api/accounts");
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(request, response, (req, res) -> {
            throw new AssertionError("unauthenticated request reached the controller");
        });

        assertThat(response.getStatus()).isEqualTo(401);
        assertThat(response.getHeader("WWW-Authenticate")).contains("Bearer");
    }

    @Test
    void permitsTheConfiguredCredentialAndHealthProbes() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/api/accounts");
        request.addHeader("Authorization", "Bearer test-ledger-read-key-0123456789abcdef");
        MockHttpServletResponse response = new MockHttpServletResponse();
        boolean[] called = {false};
        filter.doFilter(request, response, (req, res) -> called[0] = true);
        assertThat(called[0]).isTrue();

        called[0] = false;
        MockHttpServletRequest health = new MockHttpServletRequest(
                "GET", "/actuator/health/readiness");
        filter.doFilter(health, new MockHttpServletResponse(), (req, res) -> called[0] = true);
        assertThat(called[0]).isTrue();
    }
}
