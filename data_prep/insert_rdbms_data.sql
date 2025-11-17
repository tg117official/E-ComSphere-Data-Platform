-- =========================================================
-- DUMMY DATA LOAD SCRIPT FOR E-COMMERCE RDBMS MODEL
-- Assumes DDL already executed (same schema as previous script).
-- =========================================================

USE ecommerce_rdbms;

-- =========================================================
-- 1. LOOKUP TABLES
-- =========================================================

-- 1.1 customer_status
INSERT INTO customer_status (status_code, description) VALUES
    ('ACTIVE',   'Active customer'),
    ('INACTIVE', 'Inactive customer'),
    ('BLOCKED',  'Blocked due to fraud or abuse');

-- 1.2 address_type
INSERT INTO address_type (address_type_code, description) VALUES
    ('HOME',   'Home address'),
    ('OFFICE', 'Office or work address');

-- 1.3 country
INSERT INTO country (country_code, country_name) VALUES
    ('IN', 'India'),
    ('US', 'United States');

-- 1.4 brand
INSERT INTO brand (brand_id, brand_name, description, created_at, updated_at) VALUES
    (1, 'TechNova',  'Electronics and gadgets',      '2025-11-01 10:00:00', '2025-11-01 10:00:00'),
    (2, 'UrbanWear', 'Casual clothing and apparel',  '2025-11-01 10:05:00', '2025-11-01 10:05:00'),
    (3, 'WorkPro',   'Laptops and work accessories', '2025-11-01 10:10:00', '2025-11-01 10:10:00');

-- 1.5 category (with hierarchy)
INSERT INTO category (category_id, category_name, parent_category_id, category_level, created_at, updated_at) VALUES
    (1, 'Electronics',   NULL, 1, '2025-11-01 09:00:00', '2025-11-01 09:00:00'),
    (2, 'Fashion',       NULL, 1, '2025-11-01 09:05:00', '2025-11-01 09:05:00'),
    (3, 'Mobile Phones', 1,    2, '2025-11-01 09:10:00', '2025-11-01 09:10:00'),
    (4, 'Laptops',       1,    2, '2025-11-01 09:15:00', '2025-11-01 09:15:00'),
    (5, 'T-Shirts',      2,    2, '2025-11-01 09:20:00', '2025-11-01 09:20:00');

-- 1.6 currency
INSERT INTO currency (currency_code, currency_name, symbol) VALUES
    ('INR', 'Indian Rupee', '₹'),
    ('USD', 'US Dollar',    '$');

-- 1.7 order_status
INSERT INTO order_status (order_status_code, description) VALUES
    ('PENDING',   'Order created, awaiting payment'),
    ('PAID',      'Payment completed'),
    ('SHIPPED',   'Order shipped to customer'),
    ('DELIVERED', 'Order delivered to customer'),
    ('CANCELLED', 'Order cancelled');

-- 1.8 order_channel
INSERT INTO order_channel (order_channel_code, description) VALUES
    ('WEB',    'Website'),
    ('MOBILE', 'Mobile app'),
    ('CALL',   'Call center order');

-- 1.9 payment_method
INSERT INTO payment_method (payment_method_code, description) VALUES
    ('CARD',  'Credit or debit card'),
    ('UPI',   'UPI payment'),
    ('COD',   'Cash on Delivery'),
    ('NB',    'Net banking');

-- 1.10 payment_status
INSERT INTO payment_status (payment_status_code, description) VALUES
    ('PENDING', 'Payment initiated but not completed'),
    ('SUCCESS', 'Payment successful'),
    ('FAILED',  'Payment failed'),
    ('REFUNDED','Payment refunded');

-- 1.11 refund_reason
INSERT INTO refund_reason (refund_reason_code, description) VALUES
    ('DAMAGED',        'Item received damaged'),
    ('SIZE_ISSUE',     'Size or fit issue'),
    ('CUSTOMER_CANCEL','Customer cancelled before shipping'),
    ('OTHER',          'Other reasons');

-- 1.12 refund_status
INSERT INTO refund_status (refund_status_code, description) VALUES
    ('REQUESTED', 'Refund requested by customer'),
    ('APPROVED',  'Refund approved'),
    ('REJECTED',  'Refund rejected'),
    ('PROCESSED', 'Refund processed and paid');

-- 1.13 carrier
INSERT INTO carrier (carrier_id, carrier_name, contact_phone, website_url) VALUES
    (1, 'BlueDart',  '+91-22-12345678', 'https://www.bluedart.com'),
    (2, 'Delhivery', '+91-11-98765432', 'https://www.delhivery.com');

-- 1.14 shipment_status
INSERT INTO shipment_status (shipment_status_code, description) VALUES
    ('CREATED',    'Shipment created, waiting pickup'),
    ('IN_TRANSIT', 'Shipment in transit'),
    ('DELIVERED',  'Shipment delivered'),
    ('RETURNED',   'Shipment returned to warehouse');

-- 1.15 promotion
INSERT INTO promotion (promotion_id, promotion_code, promotion_name, discount_type, discount_value,
                       start_date, end_date, min_order_amount, max_discount_amount, is_active)
VALUES
    (1, 'NEWUSER10', '10% off for new users', 'PERCENT', 10.00,
     '2025-10-01 00:00:00', '2025-12-31 23:59:59', 500.00, 1000.00, 1),
    (2, 'FESTIVE500', 'Flat ₹500 off on orders above ₹3000', 'AMOUNT', 500.00,
     '2025-11-01 00:00:00', '2025-11-30 23:59:59', 3000.00, 500.00, 1);

-- =========================================================
-- 2. CUSTOMERS & ADDRESSES
-- =========================================================

-- 2.1 customers
INSERT INTO customer (customer_id, first_name, last_name, email, phone_number,
                      date_of_birth, gender, status_code, created_at, updated_at)
VALUES
    (1, 'Rahul', 'Sharma', 'rahul.sharma@example.com', '+91-9876543210',
        '1990-05-15', 'M', 'ACTIVE', '2025-11-01 11:00:00', '2025-11-01 11:00:00'),
    (2, 'Priya', 'Iyer',   'priya.iyer@example.com',   '+91-9876500000',
        '1993-08-22', 'F', 'ACTIVE', '2025-11-01 11:05:00', '2025-11-01 11:05:00'),
    (3, 'Amit',  'Patel',  'amit.patel@example.com',   '+91-9000000001',
        NULL,            NULL, 'INACTIVE', '2025-11-01 11:10:00', '2025-11-01 11:10:00');

-- 2.2 customer_address
INSERT INTO customer_address
    (customer_address_id, customer_id, address_line1, address_line2, landmark,
     city, state, postal_code, country_code, address_type_code,
     is_default, created_at, updated_at)
VALUES
    -- Rahul - home (Mumbai)
    (1, 1, 'Flat 101, Sunrise Apartments', 'Sector 5', 'Near City Mall',
        'Mumbai', 'Maharashtra', '400001', 'IN', 'HOME',
        1, '2025-11-01 11:15:00', '2025-11-01 11:15:00'),
    -- Rahul - office (Mumbai)
    (2, 1, 'TechPark Tower B', '4th Floor', 'Opp. Metro Station',
        'Mumbai', 'Maharashtra', '400002', 'IN', 'OFFICE',
        0, '2025-11-01 11:16:00', '2025-11-01 11:16:00'),
    -- Priya - home (Bengaluru)
    (3, 2, 'Villa 23, Green Meadows', NULL, 'Near IT Park',
        'Bengaluru', 'Karnataka', '560001', 'IN', 'HOME',
        1, '2025-11-01 11:20:00', '2025-11-01 11:20:00'),
    -- Amit - home (Pune)
    (4, 3, 'Row House 9, Lake View', NULL, 'Near Central School',
        'Pune', 'Maharashtra', '411001', 'IN', 'HOME',
        1, '2025-11-01 11:25:00', '2025-11-01 11:25:00');

-- =========================================================
-- 3. PRODUCTS & VARIANTS
-- =========================================================

-- 3.1 product
INSERT INTO product
    (product_id, product_name, brand_id, default_category_id,
     short_description, created_at, updated_at, is_active)
VALUES
    (1, 'TechNova X1 Smartphone', 1, 3,
        '6.5 inch display, 8GB RAM, 128GB storage, 5000mAh battery',
        '2025-11-01 12:00:00', '2025-11-01 12:00:00', 1),
    (2, 'WorkPro Ultra 14" Laptop', 3, 4,
        '14 inch FHD, 16GB RAM, 512GB SSD, Intel i7',
        '2025-11-01 12:05:00', '2025-11-01 12:05:00', 1),
    (3, 'UrbanWear Classic T-Shirt', 2, 5,
        '100% cotton, regular fit, unisex',
        '2025-11-01 12:10:00', '2025-11-01 12:10:00', 1);

-- 3.2 product_variant (simple size/color variants)
INSERT INTO product_variant
    (product_variant_id, product_id, sku, variant_name,
     created_at, updated_at, is_active)
VALUES
    -- Smartphone: color variants
    (1, 1, 'TNX1-BLK-128', 'Black / 128GB',
        '2025-11-01 12:15:00', '2025-11-01 12:15:00', 1),
    (2, 1, 'TNX1-BLU-128', 'Blue / 128GB',
        '2025-11-01 12:16:00', '2025-11-01 12:16:00', 1),

    -- Laptop: single variant
    (3, 2, 'WPU14-I7-16-512', 'Grey / 16GB / 512GB',
        '2025-11-01 12:20:00', '2025-11-01 12:20:00', 1),

    -- T-Shirt: size variants
    (4, 3, 'UW-TS-CLS-M', 'Classic / Size M',
        '2025-11-01 12:25:00', '2025-11-01 12:25:00', 1),
    (5, 3, 'UW-TS-CLS-L', 'Classic / Size L',
        '2025-11-01 12:26:00', '2025-11-01 12:26:00', 1);

-- =========================================================
-- 4. WAREHOUSES & INVENTORY
-- =========================================================

-- 4.1 warehouse
INSERT INTO warehouse
    (warehouse_id, warehouse_name, address_line1, address_line2,
     city, state, postal_code, country_code, created_at, updated_at)
VALUES
    (1, 'Mumbai Fulfilment Center', 'Plot 12, Industrial Area', NULL,
        'Mumbai', 'Maharashtra', '400070', 'IN',
        '2025-11-01 13:00:00', '2025-11-01 13:00:00'),
    (2, 'Delhi Fulfilment Center', 'Warehouse 8, Logistics Park', NULL,
        'New Delhi', 'Delhi', '110020', 'IN',
        '2025-11-01 13:05:00', '2025-11-01 13:05:00');

-- 4.2 inventory_level
INSERT INTO inventory_level
    (warehouse_id, product_variant_id, on_hand_qty, reserved_qty, last_updated_at)
VALUES
    -- Mumbai FC stock
    (1, 1, 100, 10, '2025-11-02 09:00:00'),  -- Smartphone Black
    (1, 2,  50,  5, '2025-11-02 09:00:00'),  -- Smartphone Blue
    (1, 3,  20,  2, '2025-11-02 09:00:00'),  -- Laptop
    (1, 4, 200, 15, '2025-11-02 09:00:00'),  -- T-shirt M
    (1, 5, 150, 20, '2025-11-02 09:00:00'),  -- T-shirt L

    -- Delhi FC stock
    (2, 1,  80,  8, '2025-11-02 09:05:00'),
    (2, 2,  40,  4, '2025-11-02 09:05:00'),
    (2, 3,  10,  1, '2025-11-02 09:05:00'),
    (2, 4, 120, 10, '2025-11-02 09:05:00'),
    (2, 5, 100,  5, '2025-11-02 09:05:00');

-- =========================================================
-- 5. ORDERS & ORDER ITEMS
-- =========================================================
-- We'll model 3 orders to cover scenarios:
--  - Order 1: Prepaid, fully delivered, multiple items.
--  - Order 2: Card payment failed first, then success, later partial refund.
--  - Order 3: COD order, cancelled before shipping.

-- 5.1 orders
INSERT INTO orders
    (order_id, customer_id, order_date, order_status_code, order_channel_code,
     currency_code, billing_address_id, shipping_address_id,
     total_item_amount, total_discount_amount, total_tax_amount,
     total_shipping_amount, grand_total_amount,
     created_at, updated_at)
VALUES
    -- Order 1: Rahul buys 1 smartphone + 2 T-shirts (delivered)
    (1, 1, '2025-11-05 10:00:00', 'DELIVERED', 'WEB', 'INR',
        1, 1,
        16000.00, 1100.00, 1980.00, 150.00, 17030.00,
        '2025-11-05 10:00:00', '2025-11-08 18:00:00'),

    -- Order 2: Priya buys 1 laptop (partially refunded later due to minor issue)
    (2, 2, '2025-11-06 11:30:00', 'PAID', 'MOBILE', 'INR',
        3, 3,
        60000.00, 500.00, 7200.00, 200.00, 66500.00,
        '2025-11-06 11:30:00', '2025-11-07 09:00:00'),

    -- Order 3: Amit places COD order for smartphone but cancels
    (3, 3, '2025-11-07 09:15:00', 'CANCELLED', 'WEB', 'INR',
        4, 4,
        15000.00, 0.00, 1800.00, 100.00, 16900.00,
        '2025-11-07 09:15:00', '2025-11-07 10:00:00');

-- 5.2 order_item
INSERT INTO order_item
    (order_item_id, order_id, product_variant_id, quantity,
     base_unit_price, discount_amount, tax_amount,
     shipping_amount, line_total_amount, created_at, updated_at)
VALUES
    -- Order 1 items:
    -- 1 smartphone (Black / 128GB): base 15000, discount 1000, tax 1800, shipping 100 => 15900 line total
    (1, 1, 1, 1,
        15000.00, 1000.00, 1800.00, 100.00, 15900.00,
        '2025-11-05 10:05:00', '2025-11-05 10:05:00'),
    -- 2 T-shirts (Classic M): base 500 each => 1000, discount 100, tax 180, shipping 50 => 1130 line total
    (2, 1, 4, 2,
        500.00, 100.00, 180.00, 50.00, 1130.00,
        '2025-11-05 10:06:00', '2025-11-05 10:06:00'),

    -- Order 2 items:
    -- 1 Laptop: base 60000, discount 500 (promotion), tax 7200, shipping 200 => 66500 total
    (3, 2, 3, 1,
        60000.00, 500.00, 7200.00, 200.00, 66500.00,
        '2025-11-06 11:35:00', '2025-11-06 11:35:00'),

    -- Order 3 items:
    -- 1 Smartphone (Black): order cancelled after placement
    (4, 3, 1, 1,
        15000.00, 0.00, 1800.00, 100.00, 16900.00,
        '2025-11-07 09:20:00', '2025-11-07 09:20:00');

-- =========================================================
-- 6. SHIPMENTS & SHIPMENT ITEMS
-- =========================================================
-- Order 1: Fully shipped and delivered from Mumbai warehouse.
-- Order 2: Shipped but not yet marked as delivered.
-- Order 3: Cancelled before shipment (no shipment records).

-- 6.1 shipment
INSERT INTO shipment
    (shipment_id, order_id, warehouse_id, carrier_id, shipment_status_code,
     tracking_number, shipped_date, delivered_date, created_at, updated_at)
VALUES
    -- Shipment for Order 1
    (1, 1, 1, 1, 'DELIVERED',
        'BLD123456789IN', '2025-11-06 09:00:00', '2025-11-08 18:00:00',
        '2025-11-06 08:00:00', '2025-11-08 18:00:00'),

    -- Shipment for Order 2 (in transit)
    (2, 2, 2, 2, 'IN_TRANSIT',
        'DLV987654321IN', '2025-11-07 10:00:00', NULL,
        '2025-11-07 09:30:00', '2025-11-07 10:00:00');

-- 6.2 shipment_item
INSERT INTO shipment_item
    (shipment_id, order_item_id, shipped_quantity)
VALUES
    -- Order 1: both items fully shipped
    (1, 1, 1),
    (1, 2, 2),

    -- Order 2: laptop shipped
    (2, 3, 1);

-- =========================================================
-- 7. PAYMENTS & REFUNDS
-- =========================================================
-- Scenarios:
--  - Order 1: UPI payment success.
--  - Order 2: Card payment failed first, then success. Partial refund later.
--  - Order 3: COD order, considered "pending" then cancelled without payment.

-- 7.1 payment
INSERT INTO payment
    (payment_id, order_id, payment_method_code, payment_status_code,
     amount, currency_code, transaction_reference, payment_date,
     created_at, updated_at)
VALUES
    -- Order 1: single successful UPI payment
    (1, 1, 'UPI', 'SUCCESS',
        17030.00, 'INR', 'UPI-TXN-20251105-0001',
        '2025-11-05 10:02:00', '2025-11-05 10:02:00', '2025-11-05 10:02:00'),

    -- Order 2: first payment attempt failed (CARD)
    (2, 2, 'CARD', 'FAILED',
        66500.00, 'INR', 'CARD-TXN-20251106-FAIL1',
        '2025-11-06 11:32:00', '2025-11-06 11:32:00', '2025-11-06 11:40:00'),

    -- Order 2: second payment attempt success (CARD)
    (3, 2, 'CARD', 'SUCCESS',
        66500.00, 'INR', 'CARD-TXN-20251106-SUCC1',
        '2025-11-06 11:45:00', '2025-11-06 11:45:00', '2025-11-06 11:45:00'),

    -- Order 3: COD - considered pending, later cancelled; no success payment
    (4, 3, 'COD', 'PENDING',
        16900.00, 'INR', NULL,
        '2025-11-07 09:16:00', '2025-11-07 09:16:00', '2025-11-07 10:00:00');

-- 7.2 refund
-- Order 2: Priya gets a small refund due to minor cosmetic damage.
INSERT INTO refund
    (refund_id, payment_id, order_id, refund_amount, currency_code,
     refund_reason_code, refund_status_code, refund_date,
     created_at, updated_at)
VALUES
    (1, 3, 2, 1500.00, 'INR',
        'DAMAGED', 'PROCESSED', '2025-11-10 15:00:00',
        '2025-11-10 15:00:00', '2025-11-10 15:00:00');

-- =========================================================
-- 8. ORDER-PROMOTION RELATION
-- =========================================================
-- Order 1 used NEWUSER10, Order 2 used FESTIVE500

INSERT INTO order_promotion
    (order_promotion_id, order_id, promotion_id, discount_amount_applied)
VALUES
    (1, 1, 1, 1100.00),  -- 10% discount spread across items
    (2, 2, 2, 500.00);   -- Flat 500 off

-- =========================================================
-- DATA LOAD COMPLETE
-- =========================================================
