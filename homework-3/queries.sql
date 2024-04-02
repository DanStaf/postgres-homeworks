-- Напишите запросы, которые выводят следующую информацию:
-- 1. Название компании заказчика (company_name из табл. customers) и ФИО сотрудника, работающего над заказом этой компании (см таблицу employees),
-- когда и заказчик и сотрудник зарегистрированы в городе London, а доставку заказа ведет компания United Package (company_name в табл shippers)
SELECT company_name, CONCAT(employees.first_name, ' ', employees.last_name) as FIO
FROM customers
INNER JOIN orders USING(customer_id)
INNER JOIN employees USING(employee_id)
INNER JOIN shippers ON (orders.ship_via = shippers.shipper_id)
WHERE shippers.company_name = 'United Package' AND employees.city = 'London' and customers.city = 'London'

-- 2. Наименование продукта, количество товара (product_name и units_in_stock в табл products),
-- имя поставщика и его телефон (contact_name и phone в табл suppliers) для таких продуктов,
-- которые не сняты с продажи (поле discontinued) и которых меньше 25 и которые в категориях Dairy Products и Condiments.
-- Отсортировать результат по возрастанию количества оставшегося товара.
SELECT products.product_name, products.units_in_stock, suppliers.contact_name, suppliers.phone
FROM products
INNER JOIN suppliers USING(supplier_id)
INNER JOIN categories USING(category_id)
WHERE products.discontinued != 1
AND products.units_in_stock < 25
AND categories.category_name IN ('Dairy Products', 'Condiments')
ORDER BY products.units_in_stock

-- 3. Список компаний заказчиков (company_name из табл customers), не сделавших ни одного заказа
SELECT company_name FROM customers
LEFT JOIN orders USING(customer_id)
WHERE order_id is NULL

-- 4. уникальные названия продуктов, которых заказано ровно 10 единиц (количество заказанных единиц см в колонке quantity табл order_details)
-- Этот запрос написать именно с использованием подзапроса.
SELECT DISTINCT product_name FROM products
INNER JOIN order_details USING(product_id)
WHERE order_details.quantity = 10

SELECT product_name FROM products
INNER JOIN (SELECT DISTINCT product_id FROM order_details
WHERE quantity = 10) USING(product_id)

