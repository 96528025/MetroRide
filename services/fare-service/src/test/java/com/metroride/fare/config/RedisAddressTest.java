package com.metroride.fare.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class RedisAddressTest {

    @Test
    void parsesHostAndPort() {
        RedisAddress address = RedisAddress.parse("redis:6379");

        assertThat(address.host()).isEqualTo("redis");
        assertThat(address.port()).isEqualTo(6379);
    }

    @Test
    void defaultsThePort() {
        assertThat(RedisAddress.parse("localhost")).isEqualTo(new RedisAddress("localhost", 6379));
    }

    @Test
    void rejectsUrlsAndMalformedValues() {
        assertThatThrownBy(() -> RedisAddress.parse("redis://redis:6379")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> RedisAddress.parse(":6379")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> RedisAddress.parse("redis:port")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> RedisAddress.parse("")).isInstanceOf(IllegalArgumentException.class);
    }
}
