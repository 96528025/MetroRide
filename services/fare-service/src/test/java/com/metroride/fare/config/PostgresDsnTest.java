package com.metroride.fare.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class PostgresDsnTest {

    @Test
    void translatesTheComposeDsn() {
        PostgresDsn dsn = PostgresDsn.parse("postgres://metroride:metroride@postgres:5432/metroride?sslmode=disable");

        assertThat(dsn.jdbcUrl()).isEqualTo("jdbc:postgresql://postgres:5432/metroride?sslmode=disable");
        assertThat(dsn.username()).isEqualTo("metroride");
        assertThat(dsn.password()).isEqualTo("metroride");
    }

    @Test
    void translatesTheGoDefaultDsn() {
        PostgresDsn dsn = PostgresDsn.parse(MetroRideEnvironmentPostProcessor.DEFAULT_POSTGRES_DSN);

        assertThat(dsn.jdbcUrl()).isEqualTo("jdbc:postgresql://localhost:5432/metroride?sslmode=disable");
        assertThat(dsn.username()).isEqualTo("metroride");
        assertThat(dsn.password()).isEqualTo("metroride");
    }

    @Test
    void defaultsThePortAndAllowsMissingCredentials() {
        PostgresDsn dsn = PostgresDsn.parse("postgresql://db.internal/metroride");

        assertThat(dsn.jdbcUrl()).isEqualTo("jdbc:postgresql://db.internal:5432/metroride");
        assertThat(dsn.username()).isNull();
        assertThat(dsn.password()).isNull();
    }

    @Test
    void decodesPercentEncodedCredentials() {
        PostgresDsn dsn = PostgresDsn.parse("postgres://svc:p%40ss%3Aword@localhost:5432/metroride");

        assertThat(dsn.username()).isEqualTo("svc");
        assertThat(dsn.password()).isEqualTo("p@ss:word");
    }

    @Test
    void rejectsOtherSchemesAndMissingParts() {
        assertThatThrownBy(() -> PostgresDsn.parse("jdbc:postgresql://localhost/metroride"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("scheme");
        assertThatThrownBy(() -> PostgresDsn.parse("postgres://localhost:5432"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("database");
        assertThatThrownBy(() -> PostgresDsn.parse("host=localhost dbname=metroride"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PostgresDsn.parse(" "))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
