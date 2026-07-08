-- =============================================================
--  Workshop DB Init
--  Chạy tự động khi MySQL container start lần đầu
-- =============================================================

-- Grant Debezium user replication privilege
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
GRANT SELECT ON ecommerce.* TO 'debezium'@'%';
GRANT RELOAD ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;

USE ecommerce;

-- ── Bảng users ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS users (
    id         BIGINT AUTO_INCREMENT PRIMARY KEY,
    name       VARCHAR(100)        NOT NULL,
    email      VARCHAR(150)        NOT NULL UNIQUE,
    tier       ENUM('normal','vip','premium') DEFAULT 'normal',
    created_at TIMESTAMP           DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP           DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- ── Bảng products ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS products (
    id             BIGINT AUTO_INCREMENT PRIMARY KEY,
    name           VARCHAR(200)   NOT NULL,
    category       VARCHAR(100),
    price          DECIMAL(15,2)  NOT NULL,
    stock_quantity INT            DEFAULT 0,
    created_at     TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP      DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- ── Bảng orders ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS orders (
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id      BIGINT          NOT NULL,
    status       ENUM('pending','paid','cancelled','refunded') DEFAULT 'pending',
    total_amount DECIMAL(15,2)   DEFAULT 0,
    created_at   TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP       DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_user_id  (user_id),
    INDEX idx_status   (status),
    INDEX idx_created  (created_at)
);

-- ── Bảng order_items ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS order_items (
    id         BIGINT AUTO_INCREMENT PRIMARY KEY,
    order_id   BIGINT         NOT NULL,
    product_id BIGINT         NOT NULL,
    quantity   INT            NOT NULL DEFAULT 1,
    unit_price DECIMAL(15,2)  NOT NULL,
    created_at TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_order_id   (order_id),
    INDEX idx_product_id (product_id)
);

-- =============================================================
--  Seed data
-- =============================================================

INSERT INTO users (name, email, tier) VALUES
    ('Nguyen Van A',  'nva@example.com',  'normal'),
    ('Tran Thi B',    'ttb@example.com',  'vip'),
    ('Le Van C',      'lvc@example.com',  'premium'),
    ('Pham Thi D',    'ptd@example.com',  'normal'),
    ('Hoang Van E',   'hve@example.com',  'vip'),
    ('Bot Account',   'bot@example.com',  'normal'),  -- dùng cho fraud demo
    ('Fraud User',    'fraud@example.com','normal');   -- dùng cho fraud demo

INSERT INTO products (name, category, price, stock_quantity) VALUES
    ('iPhone 15 Pro',        'Electronics', 29990000,  50),
    ('Samsung Galaxy S24',   'Electronics', 22990000,  30),
    ('MacBook Air M3',       'Computers',   32990000,  20),
    ('Sony WH-1000XM5',      'Audio',        8990000,  15),
    ('Nike Air Max 2024',    'Fashion',      3490000, 100),
    ('Adidas Ultraboost',    'Fashion',      2990000,  80),
    ('Vitamin C 1000mg',     'Health',        290000, 200),
    ('Whey Protein 2kg',     'Health',        890000,   8),  -- stock thấp → trigger alert
    ('Instant Noodle Pack',  'Food',           45000, 500),
    ('Green Tea Box',        'Food',          120000,   5);  -- stock thấp → trigger alert

INSERT INTO orders (user_id, status, total_amount) VALUES
    (1, 'paid',      29990000),
    (2, 'paid',       8990000),
    (3, 'paid',      32990000),
    (4, 'pending',   22990000),
    (5, 'paid',       3490000),
    (1, 'cancelled',  2990000);

INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
    (1, 1, 1, 29990000),
    (2, 4, 1,  8990000),
    (3, 3, 1, 32990000),
    (4, 2, 1, 22990000),
    (5, 5, 1,  3490000),
    (6, 6, 1,  2990000);

-- =============================================================
--  Extended sample data (Lakehouse ETL Lab — Part 2)
--  Gives the medallion pipeline a meaningful dataset out of the box:
--  more users/tiers, more product categories, and many orders +
--  order_items so the GOLD marts (revenue by category, RFM, reorder)
--  produce real results before you even run `demo_cdc_changes`.
--
--  NOTE: ids of the original seed are preserved on purpose —
--    users:    id=6 Bot Account (used by Part 1 fraud demo)
--    products: id=8 Whey (stock 8), id=10 Green Tea (stock 5) low-stock demos
-- =============================================================

-- ── More users (ids 8-14) — varied tiers for RFM segmentation ─
INSERT INTO users (name, email, tier) VALUES
    ('Vo Thi F',      'vtf@example.com',  'premium'),
    ('Dang Van G',    'dvg@example.com',  'vip'),
    ('Bui Thi H',     'bth@example.com',  'normal'),
    ('Do Van I',      'dvi@example.com',  'premium'),
    ('Ngo Thi K',     'ntk@example.com',  'vip'),
    ('Ly Van L',      'lvl@example.com',  'normal'),
    ('Truong Thi M',  'ttm@example.com',  'vip');

-- ── More products (ids 11-18) — extra categories + low stock ──
INSERT INTO products (name, category, price, stock_quantity) VALUES
    ('Dell XPS 13',          'Computers',   28000000,  12),   -- low stock
    ('Logitech MX Master',   'Electronics',  2200000,  40),
    ('Kindle Paperwhite',    'Electronics',  3200000,   7),   -- low stock
    ('Yoga Mat',             'Sports',        350000,  60),
    ('Dumbbell Set 20kg',    'Sports',       1500000,   9),   -- low stock
    ('Coffee Beans 1kg',     'Food',          250000, 120),
    ('Air Fryer',            'Home',         1890000,  25),
    ('Desk Lamp LED',        'Home',          450000,   4);   -- low stock

-- ── More orders (ids 7-36) — spread over ~40 days, mostly paid ─
INSERT INTO orders (user_id, status, total_amount, created_at) VALUES
    ( 1, 'paid',      29990000, NOW() - INTERVAL  2 DAY),
    ( 2, 'paid',      22990000, NOW() - INTERVAL  3 DAY),
    ( 3, 'paid',      32990000, NOW() - INTERVAL  5 DAY),
    ( 8, 'paid',      28000000, NOW() - INTERVAL  5 DAY),
    ( 9, 'paid',       3490000, NOW() - INTERVAL  7 DAY),
    ( 4, 'pending',    8990000, NOW() - INTERVAL  1 DAY),
    ( 5, 'paid',       2990000, NOW() - INTERVAL  8 DAY),
    (10, 'paid',        250000, NOW() - INTERVAL  9 DAY),
    (11, 'paid',       1890000, NOW() - INTERVAL 10 DAY),
    (12, 'paid',       3200000, NOW() - INTERVAL 11 DAY),
    ( 1, 'paid',        890000, NOW() - INTERVAL 12 DAY),
    ( 2, 'cancelled',    45000, NOW() - INTERVAL 13 DAY),
    (13, 'paid',        350000, NOW() - INTERVAL 14 DAY),
    (14, 'paid',       1500000, NOW() - INTERVAL 15 DAY),
    ( 3, 'paid',       2200000, NOW() - INTERVAL 16 DAY),
    ( 8, 'paid',      29990000, NOW() - INTERVAL 18 DAY),
    ( 9, 'paid',        120000, NOW() - INTERVAL 19 DAY),
    ( 4, 'refunded',  22990000, NOW() - INTERVAL 20 DAY),
    ( 5, 'paid',        290000, NOW() - INTERVAL 21 DAY),
    (11, 'paid',      32990000, NOW() - INTERVAL 22 DAY),
    (12, 'paid',       8990000, NOW() - INTERVAL 24 DAY),
    ( 1, 'paid',        450000, NOW() - INTERVAL 25 DAY),
    (10, 'pending',    2990000, NOW() - INTERVAL  2 DAY),
    (13, 'paid',       3490000, NOW() - INTERVAL 27 DAY),
    (14, 'paid',      28000000, NOW() - INTERVAL 28 DAY),
    ( 2, 'paid',        250000, NOW() - INTERVAL 30 DAY),
    ( 3, 'paid',       1890000, NOW() - INTERVAL 31 DAY),
    ( 8, 'paid',       3200000, NOW() - INTERVAL 33 DAY),
    ( 9, 'paid',        890000, NOW() - INTERVAL 35 DAY),
    ( 5, 'paid',      29990000, NOW() - INTERVAL 40 DAY);

-- ── More order_items (ids 7-54) ──────────────────────────────
--  One line per new order (ids 7-36), plus extra lines that drive
--  sales for the low-stock products so the reorder report is useful.
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
    ( 7,  1, 1, 29990000),
    ( 8,  2, 1, 22990000),
    ( 9,  3, 1, 32990000),
    (10, 11, 1, 28000000),
    (11,  5, 1,  3490000),
    (12,  4, 1,  8990000),
    (13,  6, 1,  2990000),
    (14, 16, 1,   250000),
    (15, 17, 1,  1890000),
    (16, 13, 1,  3200000),
    (17,  8, 1,   890000),
    (18,  9, 1,    45000),
    (19, 14, 1,   350000),
    (20, 15, 1,  1500000),
    (21, 12, 1,  2200000),
    (22,  1, 1, 29990000),
    (23, 10, 1,   120000),
    (24,  2, 1, 22990000),
    (25,  7, 1,   290000),
    (26,  3, 1, 32990000),
    (27,  4, 1,  8990000),
    (28, 18, 1,   450000),
    (29,  6, 1,  2990000),
    (30,  5, 1,  3490000),
    (31, 11, 1, 28000000),
    (32, 16, 2,   250000),
    (33, 17, 1,  1890000),
    (34, 13, 1,  3200000),
    (35,  8, 2,   890000),
    (36,  1, 1, 29990000),
    -- extra lines (multi-item orders + low-stock demand)
    ( 7,  7, 2,   290000),
    ( 9,  4, 1,  8990000),
    (10, 12, 1,  2200000),
    (16, 10, 3,   120000),
    (17,  8, 1,   890000),
    (20, 15, 1,  1500000),
    (22, 13, 1,  3200000),
    (26, 18, 2,   450000),
    (30, 14, 2,   350000),
    (31, 11, 1, 28000000),
    (32,  9, 5,    45000),
    (33, 18, 1,   450000),
    (34, 13, 2,  3200000),
    (35,  8, 1,   890000),
    (36, 10, 2,   120000),
    (11, 15, 1,  1500000),
    (13, 11, 1, 28000000),
    (14, 16, 3,   250000);
