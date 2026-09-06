package com.metroride.fare.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class ListenAddressTest {

    @Test
    void goStyleAllInterfacesAddressHasNoBindHost() {
        ListenAddress address = ListenAddress.parse(":8087");

        assertThat(address.port()).isEqualTo(8087);
        assertThat(address.bindHost()).isEmpty();
    }

    @Test
    void explicitHostIsKept() {
        ListenAddress address = ListenAddress.parse("127.0.0.1:9000");

        assertThat(address.port()).isEqualTo(9000);
        assertThat(address.bindHost()).contains("127.0.0.1");
    }

    @Test
    void bracketedIpv6HostIsUnwrapped() {
        assertThat(ListenAddress.parse("[::1]:8087").bindHost()).contains("::1");
    }

    @Test
    void rejectsMalformedValues() {
        assertThatThrownBy(() -> ListenAddress.parse("8087")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ListenAddress.parse(":abc")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ListenAddress.parse(":70000")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ListenAddress.parse("")).isInstanceOf(IllegalArgumentException.class);
    }
}
