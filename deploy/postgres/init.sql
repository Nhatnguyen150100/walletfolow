-- Mỗi service một database riêng (nguyên tắc "database per service").
-- Postgres chạy file này 1 lần khi khởi tạo volume lần đầu.
-- (authdb dùng ngay ở Milestone 0; các DB còn lại dành cho service ra đời ở milestone sau.)
CREATE DATABASE authdb;
CREATE DATABASE walletdb;
CREATE DATABASE ledgerdb;
CREATE DATABASE txndb;
CREATE DATABASE railsdb;
