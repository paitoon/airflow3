# เริ่มจาก Apache AGE (มี AGE built-in แล้ว)
# PG16 = PostgreSQL version 16
FROM apache/age:release_PG16_1.6.0

# ติดตั้ง pgvector จาก PostgreSQL apt repository
RUN apt-get update \
    && apt-get install -y \
        curl \
        gnupg \
        ca-certificates \
    # เพิ่ม PostgreSQL official apt repo
    && curl https://www.postgresql.org/media/keys/ACCC4CF8.asc \
        | gpg --dearmor -o /etc/apt/trusted.gpg.d/postgresql.gpg \
    && echo "deb http://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" \
        > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update \
    # ติดตั้ง pgvector สำหรับ PostgreSQL 16
    && apt-get install -y postgresql-16-pgvector \
    && rm -rf /var/lib/apt/lists/*
