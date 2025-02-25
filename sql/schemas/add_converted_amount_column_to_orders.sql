ALTER TABLE 
    orders
ADD COLUMN IF NOT EXISTS 
    converted_amount_eur DECIMAL(10, 2);
