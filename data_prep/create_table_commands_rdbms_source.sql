-- =========================================================
-- E-COMMERCE RDBMS DATA MODEL (3NF) - MySQL
-- Core OLTP schema: customers, products, orders, payments, shipments, promotions
-- =========================================================

-- Optional: create and use a database
CREATE DATABASE IF NOT EXISTS ecommerce_rdbms
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

USE ecommerce_rdbms;

-- =========================================================
-- 1. LOOKUP / REFERENCE TABLES
-- =========================================================

-- 1.1 Customer status lookup (ACTIVE, INACTIVE, BLOCKED, etc.)
CREATE TABLE customer_status (
    status_code     VARCHAR(20)  NOT NULL,
    description     VARCHAR(255) NOT NULL,
    PRIMARY KEY (status_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 1.2 Address type lookup (HOME, OFFICE, etc.)
CREATE TABLE address_type (
    address_type_code   VARCHAR(20)  NOT NULL,
    description         VARCHAR(255) NOT NULL,
    PRIMARY KEY (address_type_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 1.3 Countries (ISO codes)
CREATE TABLE country (
    country_code    VARCHAR(3)   NOT NULL,
    country_name    VARCHAR(100) NOT NULL,
    PRIMARY KEY (country_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 1.4 Brand master
CREATE TABLE brand (
    brand_id        INT AUTO_INCREMENT NOT NULL,
    brand_name      VARCHAR(255)       NOT NULL,
    description     TEXT               NULL,
    created_at      DATETIME           NOT NULL,
    updated_at      DATETIME           NOT NULL,
    PRIMARY KEY (brand_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 1.5 Product category (supports hierarchy via parent_category_id)
CREATE TABLE category (
    category_id         INT AUTO_INCREMENT NOT NULL,
    category_name       VARCHAR(255)       NOT NULL,
    parent_category_id  INT                NULL,
    category_level      TINYINT            NOT NULL,
    created_at          DATETIME           NOT NULL,
    updated_at          DATETIME           NOT NULL,
    PRIMARY KEY (category_id),
    CONSTRAINT fk_category_parent
        FOREIGN KEY (parent_category_id)
        REFERENCES category (category_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 1.6 Currency lookup (INR, USD, etc.)
CREATE TABLE currency (
    currency_code   VARCHAR(3)   NOT NULL,
    currency_name   VARCHAR(50)  NOT NULL,
    symbol          VARCHAR(10)  NOT NULL,
    PRIMARY KEY (currency_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 1.7 Order status lookup (PENDING, PAID, SHIPPED, etc.)
CREATE TABLE order_status (
    order_status_code   VARCHAR(20)  NOT NULL,
    description         VARCHAR(255) NOT NULL,
    PRIMARY KEY (order_status_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 1.8 Order channel lookup (WEB, MOBILE, CALL_CENTER)
CREATE TABLE order_channel (
    order_channel_code  VARCHAR(20)  NOT NULL,
    description         VARCHAR(255) NOT NULL,
    PRIMARY KEY (order_channel_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 1.9 Payment method lookup (CARD, UPI, COD, etc.)
CREATE TABLE payment_method (
    payment_method_code VARCHAR(20)  NOT NULL,
    description         VARCHAR(255) NOT NULL,
    PRIMARY KEY (payment_method_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 1.10 Payment status lookup (PENDING, SUCCESS, FAILED, etc.)
CREATE TABLE payment_status (
    payment_status_code VARCHAR(20)  NOT NULL,
    description         VARCHAR(255) NOT NULL,
    PRIMARY KEY (payment_status_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 1.11 Refund reason lookup
CREATE TABLE refund_reason (
    refund_reason_code  VARCHAR(20)  NOT NULL,
    description         VARCHAR(255) NOT NULL,
    PRIMARY KEY (refund_reason_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 1.12 Refund status lookup
CREATE TABLE refund_status (
    refund_status_code  VARCHAR(20)  NOT NULL,
    description         VARCHAR(255) NOT NULL,
    PRIMARY KEY (refund_status_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 1.13 Shipment carrier lookup
CREATE TABLE carrier (
    carrier_id      INT AUTO_INCREMENT NOT NULL,
    carrier_name    VARCHAR(255)       NOT NULL,
    contact_phone   VARCHAR(50)        NULL,
    website_url     VARCHAR(255)       NULL,
    PRIMARY KEY (carrier_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 1.14 Shipment status lookup (CREATED, IN_TRANSIT, DELIVERED, etc.)
CREATE TABLE shipment_status (
    shipment_status_code    VARCHAR(20)  NOT NULL,
    description             VARCHAR(255) NOT NULL,
    PRIMARY KEY (shipment_status_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 1.15 Promotion master (for coupons/offers)
CREATE TABLE promotion (
    promotion_id        INT AUTO_INCREMENT NOT NULL,
    promotion_code      VARCHAR(50)        NOT NULL,
    promotion_name      VARCHAR(255)       NOT NULL,
    discount_type       VARCHAR(10)        NOT NULL, -- e.g. 'PERCENT' or 'AMOUNT'
    discount_value      DECIMAL(10,2)      NOT NULL,
    start_date          DATETIME           NOT NULL,
    end_date            DATETIME           NULL,
    min_order_amount    DECIMAL(10,2)      NULL,
    max_discount_amount DECIMAL(10,2)      NULL,
    is_active           TINYINT(1)         NOT NULL,
    PRIMARY KEY (promotion_id),
    UNIQUE KEY uk_promotion_code (promotion_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =========================================================
-- 2. CUSTOMER & IDENTITY TABLES
-- =========================================================

-- 2.1 Customer master
CREATE TABLE customer (
    customer_id     INT AUTO_INCREMENT NOT NULL,
    first_name      VARCHAR(100)       NOT NULL,
    last_name       VARCHAR(100)       NOT NULL,
    email           VARCHAR(255)       NOT NULL,
    phone_number    VARCHAR(50)        NULL,
    date_of_birth   DATE               NULL,
    gender          VARCHAR(10)        NULL,
    status_code     VARCHAR(20)        NOT NULL,
    created_at      DATETIME           NOT NULL,
    updated_at      DATETIME           NOT NULL,
    PRIMARY KEY (customer_id),
    UNIQUE KEY uk_customer_email (email),
    CONSTRAINT fk_customer_status
        FOREIGN KEY (status_code)
        REFERENCES customer_status (status_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 2.2 Customer addresses (multiple per customer)
CREATE TABLE customer_address (
    customer_address_id    INT AUTO_INCREMENT NOT NULL,
    customer_id            INT                NOT NULL,
    address_line1          VARCHAR(255)       NOT NULL,
    address_line2          VARCHAR(255)       NULL,
    landmark               VARCHAR(255)       NULL,
    city                   VARCHAR(100)       NOT NULL,
    state                  VARCHAR(100)       NOT NULL,
    postal_code            VARCHAR(20)        NOT NULL,
    country_code           VARCHAR(3)         NOT NULL,
    address_type_code      VARCHAR(20)        NOT NULL,
    is_default             TINYINT(1)         NOT NULL DEFAULT 0,
    created_at             DATETIME           NOT NULL,
    updated_at             DATETIME           NOT NULL,
    PRIMARY KEY (customer_address_id),
    KEY idx_customer_address_customer (customer_id),
    CONSTRAINT fk_customer_address_customer
        FOREIGN KEY (customer_id)
        REFERENCES customer (customer_id),
    CONSTRAINT fk_customer_address_country
        FOREIGN KEY (country_code)
        REFERENCES country (country_code),
    CONSTRAINT fk_customer_address_type
        FOREIGN KEY (address_type_code)
        REFERENCES address_type (address_type_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =========================================================
-- 3. PRODUCT CORE TABLES
-- =========================================================

-- 3.1 Product master (minimal, core details only)
CREATE TABLE product (
    product_id          INT AUTO_INCREMENT NOT NULL,
    product_name        VARCHAR(255)       NOT NULL,
    brand_id            INT                NOT NULL,
    default_category_id INT                NOT NULL,
    short_description   VARCHAR(1000)      NULL,
    created_at          DATETIME           NOT NULL,
    updated_at          DATETIME           NOT NULL,
    is_active           TINYINT(1)         NOT NULL,
    PRIMARY KEY (product_id),
    KEY idx_product_brand (brand_id),
    KEY idx_product_category (default_category_id),
    CONSTRAINT fk_product_brand
        FOREIGN KEY (brand_id)
        REFERENCES brand (brand_id),
    CONSTRAINT fk_product_category
        FOREIGN KEY (default_category_id)
        REFERENCES category (category_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 3.2 Product variants (SKU-level items)
CREATE TABLE product_variant (
    product_variant_id INT AUTO_INCREMENT NOT NULL,
    product_id         INT                NOT NULL,
    sku                VARCHAR(100)       NOT NULL,
    variant_name       VARCHAR(255)       NOT NULL, -- e.g. "Red / L"
    created_at         DATETIME           NOT NULL,
    updated_at         DATETIME           NOT NULL,
    is_active          TINYINT(1)         NOT NULL,
    PRIMARY KEY (product_variant_id),
    UNIQUE KEY uk_product_variant_sku (sku),
    KEY idx_product_variant_product (product_id),
    CONSTRAINT fk_product_variant_product
        FOREIGN KEY (product_id)
        REFERENCES product (product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =========================================================
-- 4. WAREHOUSE & INVENTORY TABLES
-- =========================================================

-- 4.1 Warehouse master
CREATE TABLE warehouse (
    warehouse_id    INT AUTO_INCREMENT NOT NULL,
    warehouse_name  VARCHAR(255)       NOT NULL,
    address_line1   VARCHAR(255)       NOT NULL,
    address_line2   VARCHAR(255)       NULL,
    city            VARCHAR(100)       NOT NULL,
    state           VARCHAR(100)       NOT NULL,
    postal_code     VARCHAR(20)        NOT NULL,
    country_code    VARCHAR(3)         NOT NULL,
    created_at      DATETIME           NOT NULL,
    updated_at      DATETIME           NOT NULL,
    PRIMARY KEY (warehouse_id),
    CONSTRAINT fk_warehouse_country
        FOREIGN KEY (country_code)
        REFERENCES country (country_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 4.2 Inventory level per warehouse & product_variant
CREATE TABLE inventory_level (
    warehouse_id        INT         NOT NULL,
    product_variant_id  INT         NOT NULL,
    on_hand_qty         INT         NOT NULL,
    reserved_qty        INT         NOT NULL,
    last_updated_at     DATETIME    NOT NULL,
    PRIMARY KEY (warehouse_id, product_variant_id),
    KEY idx_inventory_product_variant (product_variant_id),
    CONSTRAINT fk_inventory_warehouse
        FOREIGN KEY (warehouse_id)
        REFERENCES warehouse (warehouse_id),
    CONSTRAINT fk_inventory_product_variant
        FOREIGN KEY (product_variant_id)
        REFERENCES product_variant (product_variant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =========================================================
-- 5. ORDER & ORDER ITEM TABLES
-- =========================================================

-- 5.1 Orders header table
CREATE TABLE orders (
    order_id            INT AUTO_INCREMENT NOT NULL,
    customer_id         INT                NOT NULL,
    order_date          DATETIME           NOT NULL,
    order_status_code   VARCHAR(20)        NOT NULL,
    order_channel_code  VARCHAR(20)        NOT NULL,
    currency_code       VARCHAR(3)         NOT NULL,
    billing_address_id  INT                NOT NULL,
    shipping_address_id INT                NOT NULL,
    total_item_amount   DECIMAL(12,2)      NOT NULL,
    total_discount_amount DECIMAL(12,2)    NOT NULL,
    total_tax_amount    DECIMAL(12,2)      NOT NULL,
    total_shipping_amount DECIMAL(12,2)    NOT NULL,
    grand_total_amount  DECIMAL(12,2)      NOT NULL,
    created_at          DATETIME           NOT NULL,
    updated_at          DATETIME           NOT NULL,
    PRIMARY KEY (order_id),
    KEY idx_orders_customer (customer_id),
    KEY idx_orders_status (order_status_code),
    CONSTRAINT fk_orders_customer
        FOREIGN KEY (customer_id)
        REFERENCES customer (customer_id),
    CONSTRAINT fk_orders_order_status
        FOREIGN KEY (order_status_code)
        REFERENCES order_status (order_status_code),
    CONSTRAINT fk_orders_order_channel
        FOREIGN KEY (order_channel_code)
        REFERENCES order_channel (order_channel_code),
    CONSTRAINT fk_orders_currency
        FOREIGN KEY (currency_code)
        REFERENCES currency (currency_code),
    CONSTRAINT fk_orders_billing_address
        FOREIGN KEY (billing_address_id)
        REFERENCES customer_address (customer_address_id),
    CONSTRAINT fk_orders_shipping_address
        FOREIGN KEY (shipping_address_id)
        REFERENCES customer_address (customer_address_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 5.2 Order line items
CREATE TABLE order_item (
    order_item_id       INT AUTO_INCREMENT NOT NULL,
    order_id            INT                NOT NULL,
    product_variant_id  INT                NOT NULL,
    quantity            INT                NOT NULL,
    base_unit_price     DECIMAL(12,2)      NOT NULL,
    discount_amount     DECIMAL(12,2)      NOT NULL,
    tax_amount          DECIMAL(12,2)      NOT NULL,
    shipping_amount     DECIMAL(12,2)      NOT NULL,
    line_total_amount   DECIMAL(12,2)      NOT NULL,
    created_at          DATETIME           NOT NULL,
    updated_at          DATETIME           NOT NULL,
    PRIMARY KEY (order_item_id),
    KEY idx_order_item_order (order_id),
    KEY idx_order_item_product_variant (product_variant_id),
    CONSTRAINT fk_order_item_order
        FOREIGN KEY (order_id)
        REFERENCES orders (order_id),
    CONSTRAINT fk_order_item_product_variant
        FOREIGN KEY (product_variant_id)
        REFERENCES product_variant (product_variant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =========================================================
-- 6. SHIPMENT TABLES
-- =========================================================

-- 6.1 Shipments
CREATE TABLE shipment (
    shipment_id         INT AUTO_INCREMENT NOT NULL,
    order_id            INT                NOT NULL,
    warehouse_id        INT                NOT NULL,
    carrier_id          INT                NOT NULL,
    shipment_status_code VARCHAR(20)       NOT NULL,
    tracking_number     VARCHAR(100)       NULL,
    shipped_date        DATETIME           NULL,
    delivered_date      DATETIME           NULL,
    created_at          DATETIME           NOT NULL,
    updated_at          DATETIME           NOT NULL,
    PRIMARY KEY (shipment_id),
    KEY idx_shipment_order (order_id),
    CONSTRAINT fk_shipment_order
        FOREIGN KEY (order_id)
        REFERENCES orders (order_id),
    CONSTRAINT fk_shipment_warehouse
        FOREIGN KEY (warehouse_id)
        REFERENCES warehouse (warehouse_id),
    CONSTRAINT fk_shipment_carrier
        FOREIGN KEY (carrier_id)
        REFERENCES carrier (carrier_id),
    CONSTRAINT fk_shipment_status
        FOREIGN KEY (shipment_status_code)
        REFERENCES shipment_status (shipment_status_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 6.2 Shipment items (which order items/qty shipped in each shipment)
CREATE TABLE shipment_item (
    shipment_id     INT NOT NULL,
    order_item_id   INT NOT NULL,
    shipped_quantity INT NOT NULL,
    PRIMARY KEY (shipment_id, order_item_id),
    KEY idx_shipment_item_order_item (order_item_id),
    CONSTRAINT fk_shipment_item_shipment
        FOREIGN KEY (shipment_id)
        REFERENCES shipment (shipment_id),
    CONSTRAINT fk_shipment_item_order_item
        FOREIGN KEY (order_item_id)
        REFERENCES order_item (order_item_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =========================================================
-- 7. PAYMENT & REFUND TABLES
-- =========================================================

-- 7.1 Payments related to orders
CREATE TABLE payment (
    payment_id          INT AUTO_INCREMENT NOT NULL,
    order_id            INT                NOT NULL,
    payment_method_code VARCHAR(20)        NOT NULL,
    payment_status_code VARCHAR(20)        NOT NULL,
    amount              DECIMAL(12,2)      NOT NULL,
    currency_code       VARCHAR(3)         NOT NULL,
    transaction_reference VARCHAR(255)     NULL,
    payment_date        DATETIME           NOT NULL,
    created_at          DATETIME           NOT NULL,
    updated_at          DATETIME           NOT NULL,
    PRIMARY KEY (payment_id),
    KEY idx_payment_order (order_id),
    CONSTRAINT fk_payment_order
        FOREIGN KEY (order_id)
        REFERENCES orders (order_id),
    CONSTRAINT fk_payment_method
        FOREIGN KEY (payment_method_code)
        REFERENCES payment_method (payment_method_code),
    CONSTRAINT fk_payment_status
        FOREIGN KEY (payment_status_code)
        REFERENCES payment_status (payment_status_code),
    CONSTRAINT fk_payment_currency
        FOREIGN KEY (currency_code)
        REFERENCES currency (currency_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 7.2 Refunds linked to payments and orders
CREATE TABLE refund (
    refund_id           INT AUTO_INCREMENT NOT NULL,
    payment_id          INT                NOT NULL,
    order_id            INT                NOT NULL,
    refund_amount       DECIMAL(12,2)      NOT NULL,
    currency_code       VARCHAR(3)         NOT NULL,
    refund_reason_code  VARCHAR(20)        NOT NULL,
    refund_status_code  VARCHAR(20)        NOT NULL,
    refund_date         DATETIME           NOT NULL,
    created_at          DATETIME           NOT NULL,
    updated_at          DATETIME           NOT NULL,
    PRIMARY KEY (refund_id),
    KEY idx_refund_payment (payment_id),
    KEY idx_refund_order (order_id),
    CONSTRAINT fk_refund_payment
        FOREIGN KEY (payment_id)
        REFERENCES payment (payment_id),
    CONSTRAINT fk_refund_order
        FOREIGN KEY (order_id)
        REFERENCES orders (order_id),
    CONSTRAINT fk_refund_currency
        FOREIGN KEY (currency_code)
        REFERENCES currency (currency_code),
    CONSTRAINT fk_refund_reason
        FOREIGN KEY (refund_reason_code)
        REFERENCES refund_reason (refund_reason_code),
    CONSTRAINT fk_refund_status
        FOREIGN KEY (refund_status_code)
        REFERENCES refund_status (refund_status_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =========================================================
-- 8. ORDER-PROMOTION RELATION
-- =========================================================

-- 8.1 Promotions applied at order level
CREATE TABLE order_promotion (
    order_promotion_id  INT AUTO_INCREMENT NOT NULL,
    order_id            INT                NOT NULL,
    promotion_id        INT                NOT NULL,
    discount_amount_applied DECIMAL(12,2)  NOT NULL,
    PRIMARY KEY (order_promotion_id),
    KEY idx_order_promotion_order (order_id),
    KEY idx_order_promotion_promotion (promotion_id),
    CONSTRAINT fk_order_promotion_order
        FOREIGN KEY (order_id)
        REFERENCES orders (order_id),
    CONSTRAINT fk_order_promotion_promotion
        FOREIGN KEY (promotion_id)
        REFERENCES promotion (promotion_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =========================================================
-- END OF SCRIPT
-- =========================================================
