CREATE TABLE dim_date (
    date_id     SERIAL PRIMARY KEY,
    full_date   DATE NOT NULL UNIQUE,
    month       INT NOT NULL,
    year        INT NOT NULL,
    quarter     INT NOT NULL
);

CREATE TABLE dim_product (
    product_id  SERIAL PRIMARY KEY,
    name        VARCHAR(255),
    category    VARCHAR(255)
);

CREATE TABLE dim_customer (
    customer_id SERIAL PRIMARY KEY,
    sex         CHAR(1) CHECK (sex IN ('M','F')),
    birth_date  DATE NOT NULL,
    region      VARCHAR(255) NOT NULL
);

CREATE TABLE fact_sales (
    order_id    SERIAL PRIMARY KEY,
    date_id     INT REFERENCES dim_date(date_id),
    product_id  INT REFERENCES dim_product(product_id),
    customer_id INT REFERENCES dim_customer(customer_id),
    amount      NUMERIC(10,2) NOT NULL,
    quantity    INT NOT NULL
);