CREATE TABLE IF NOT EXISTS ecb_exchange_rates(
                        currency_code VARCHAR(3) NOT NULL,
                        rate DECIMAL(10, 4) NOT NULL,
                        updated_at TIMESTAMP NOT NULL,
                        PRIMARY KEY(currency_code)
                    );
