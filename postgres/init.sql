-- ไฟล์นี้รันอัตโนมัติครั้งแรกที่ container start
-- PostgreSQL จะ scan /docker-entrypoint-initdb.d/ และรันทุก .sql ตามลำดับชื่อ

-- เปิด pgvector
CREATE EXTENSION IF NOT EXISTS vector;

-- เปิด Apache AGE
CREATE EXTENSION IF NOT EXISTS age;

-- โหลด AGE library เข้า session (จำเป็นสำหรับ AGE)
LOAD 'age';

-- ตั้ง search_path ให้ AGE ทำงานได้
SET search_path = ag_catalog, "$user", public;
