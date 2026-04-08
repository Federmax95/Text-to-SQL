use northwind;

ALTER TABLE `customers` DROP COLUMN `home_phone`;
ALTER TABLE `customers` DROP COLUMN `mobile_phone`;
ALTER TABLE `customers` DROP COLUMN `web_page`;
ALTER TABLE `customers` DROP COLUMN `notes`;
ALTER TABLE `customers` DROP COLUMN `attachments`;

ALTER TABLE `employees` DROP COLUMN `attachments`;
ALTER TABLE `employees` DROP COLUMN `mobile_phone`;

ALTER TABLE `inventory_transactions` DROP COLUMN `comments`;

ALTER TABLE `suppliers` DROP COLUMN `business_phone`;
ALTER TABLE `suppliers` DROP COLUMN `home_phone`;
ALTER TABLE `suppliers` DROP COLUMN `mobile_phone`;
ALTER TABLE `suppliers` DROP COLUMN `fax_number`;
ALTER TABLE `suppliers` DROP COLUMN `address`;
ALTER TABLE `suppliers` DROP COLUMN `country_region`;
ALTER TABLE `suppliers` DROP COLUMN `web_page`;
ALTER TABLE `suppliers` DROP COLUMN `notes`;
ALTER TABLE `suppliers` DROP COLUMN `attachments`;

ALTER TABLE `shippers` DROP COLUMN `job_title`;
ALTER TABLE `shippers` DROP COLUMN `business_phone`;
ALTER TABLE `shippers` DROP COLUMN `home_phone`;
ALTER TABLE `shippers` DROP COLUMN `mobile_phone`;
ALTER TABLE `shippers` DROP COLUMN `fax_number`;
ALTER TABLE `shippers` DROP COLUMN `web_page`;
ALTER TABLE `shippers` DROP COLUMN `notes`;
ALTER TABLE `shippers` DROP COLUMN `attachments`;

ALTER TABLE `purchase_orders` DROP COLUMN `expected_date`;
ALTER TABLE `purchase_orders` DROP COLUMN `payment_date`;
ALTER TABLE `purchase_orders` DROP COLUMN `payment_method`;

ALTER TABLE `products` DROP COLUMN `description`;
ALTER TABLE `products` DROP COLUMN `attachments`;

ALTER TABLE `order_details` DROP COLUMN `date_allocated`;

ALTER TABLE `invoices` DROP COLUMN `due_date`;
