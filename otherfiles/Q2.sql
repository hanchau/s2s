Question 2:
    Get the average price of order.
Table:
    [ date, sku, price, quantity, tax_rate, shipping_rate]
Solution:
    SELECT SUM(price * quantity) / SUM(quantity) avg_price
    FROM order