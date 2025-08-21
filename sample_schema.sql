CREATE DATABASE IF NOT EXISTS source_db;
USE source_db;

CREATE TABLE IF NOT EXISTS customer_orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    amount DECIMAL(10,2),
    status VARCHAR(20)
);
