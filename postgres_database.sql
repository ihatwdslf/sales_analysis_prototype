CREATE TABLE Products (
    product_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    unit_price DECIMAL(10,2) DEFAULT 0.00,
    unit_cost DECIMAL(10,2) DEFAULT 0.00,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Losses (
    loss_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id BIGINT NOT NULL REFERENCES Products(product_id) ON DELETE CASCADE,
    date DATE NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity >= 0),
    reason TEXT,
    total_cost DECIMAL(10,2) DEFAULT 0.00,
    notes TEXT
);

CREATE TABLE Sales (
    sale_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id BIGINT NOT NULL REFERENCES Products(product_id) ON DELETE CASCADE,
    date DATE NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity >= 0),
    price DECIMAL(10,2) NOT NULL,
    discount_amount DECIMAL(10,2) DEFAULT 0.00
);

CREATE TABLE Promotions (
    promo_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id BIGINT NOT NULL REFERENCES Products(product_id) ON DELETE CASCADE,
    start_date DATE NOT NULL,
    end_date DATE,
    discount_percent DECIMAL(5,2) DEFAULT 0.00,
    description TEXT
);

CREATE TABLE Returns (
    return_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sale_id BIGINT NOT NULL REFERENCES Sales(sale_id) ON DELETE CASCADE,
    date DATE NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity >= 0),
    reason VARCHAR(50),
    refund_amount DECIMAL(10,2) DEFAULT 0.00
);

CREATE TABLE Uploads (
    upload_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    file_name VARCHAR(255) NOT NULL,
    upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'pending',
    metadata JSONB
);

CREATE INDEX idx_losses_date ON Losses(date);
CREATE INDEX idx_sales_date ON Sales(date);