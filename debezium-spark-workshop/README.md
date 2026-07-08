# Debezium & Spark Integration — Workshop Lab

> **2h Workshop · Data Engineering Series**
> Real-time CDC Processing cho hệ thống Thương mại điện tử

---

## 📁 Cấu trúc project

```
debezium-spark-workshop/
├── docker/
│   └── docker-compose.yml        # Full stack: MySQL · Kafka · Debezium · Spark
├── data/
│   └── init/
│       └── 01_init.sql           # Tạo bảng + seed data tự động
├── config/
│   └── connector.json            # Debezium MySQL connector config
├── spark/
│   ├── schemas/
│   │   └── debezium_schemas.py   # PySpark schemas cho từng bảng
│   ├── jobs/
│   │   ├── task1_read_events.py       # Task 1: Đọc raw events
│   │   ├── task2_revenue_dashboard.py # Task 2: Tumbling Window
│   │   ├── task3_fraud_detection.py   # Task 3: Sliding Window + Kafka output
│   │   └── bonus_inventory_alert.py   # Bonus: Stream-Static Join
│   └── checkpoints/              # Tự động tạo khi chạy job
└── scripts/
    ├── setup.sh           # Khởi động toàn bộ environment
    ├── run_job.sh         # Chạy Spark jobs
    ├── mysql.sh           # MySQL console + demo data helpers
    ├── check_status.sh    # Health check tất cả services
    └── teardown.sh        # Dừng environment
```

---

## 🚀 Khởi động nhanh

### Yêu cầu
- Docker Desktop (hoặc Docker Engine + Compose plugin)
- RAM: tối thiểu 6GB cho Docker

### Bước 1 — Start environment
```bash
bash scripts/setup.sh
```
Script sẽ tự động:
1. Start tất cả Docker containers
2. Đợi MySQL và Kafka Connect sẵn sàng
3. Đăng ký Debezium connector
4. Verify connector đang RUNNING

> ⏱ Lần đầu chạy mất khoảng 2–3 phút để pull images

### Bước 2 — Verify
```bash
bash scripts/check_status.sh
```

Kiểm tra các services:
| Service | URL | Ghi chú |
|---------|-----|---------|
| **Kafka UI** | http://localhost:8080 | Xem topics, messages, consumer lag |
| **Kafka Connect** | http://localhost:8083 | REST API của Debezium |
| **Schema Registry** | http://localhost:8081 | |
| **Spark UI** | http://localhost:8888 | Streaming tab khi job đang chạy |
| **MySQL** | localhost:3306 | user: root / pass: root123 |

---

## 🎯 Lab Tasks

### Task 1 — Đọc Debezium Events (Beginner)

**Mục tiêu:** Kết nối Spark → Kafka, parse JSON, in ra console.

```bash
bash scripts/run_job.sh task1
```

Sau khi job chạy, mở terminal khác và tạo data mới:
```bash
bash scripts/mysql.sh console
```
```sql
-- Tạo đơn hàng mới → op="c"
INSERT INTO orders (user_id, status, total_amount)
VALUES (9, 'pending', 500000);

-- Cập nhật status → op="u"
UPDATE orders SET status='paid' WHERE id = LAST_INSERT_ID();
```

**Kết quả mong đợi:**
```
+-------------------+---------+----------+----------+...
|kafka_timestamp    |op_label |after_id  |after_stat|...
+-------------------+---------+----------+----------+...
|2024-01-15 10:00:01|INSERT   |7         |pending   |...
|2024-01-15 10:00:03|UPDATE   |7         |paid      |...
```


Nếu show lỗi: 26/03/20 11:24:55 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable


Chạy lệnh: docker restart workshop-spark-master




---

### Task 2 — Revenue Dashboard (Intermediate)

**Mục tiêu:** Tính tổng doanh thu theo từng phút dùng Tumbling Window.

```bash
bash scripts/run_job.sh task2
```

Tạo nhiều đơn paid:
```bash
bash scripts/mysql.sh demo_orders
```

Hoặc thủ công:
```sql
INSERT INTO orders (user_id, status, total_amount) VALUES
(1, 'paid', 500000),
(2, 'paid', 300000),
(3, 'paid', 800000),
(4, 'paid', 150000);
```

**Kết quả mong đợi** (sau 1–2 phút):
```
+-------------------+-------------+-----------+-----------------+
|minute             |total_revenue|order_count|avg_order_value  |
+-------------------+-------------+-----------+-----------------+
|2024-01-15 10:00:00|    1750000.0|          4|         437500.0|
```

> **Lưu ý:** Với watermark 2 phút, kết quả xuất hiện sau khi window đóng.
> Để test nhanh, tạo events liên tục trong 1 phút rồi đợi.

---

### Task 3 — Fraud Detection (Advanced)

**Mục tiêu:** Phát hiện user đặt > 3 đơn trong 5 phút dùng Sliding Window.

```bash
bash scripts/run_job.sh task3
```

Tạo fraud pattern:
```bash
bash scripts/mysql.sh demo_fraud
```

Hoặc thủ công (user_id=6 là "Bot Account"):
```sql
-- Chạy lặp lại 4–5 lần
INSERT INTO orders (user_id, status, total_amount)
VALUES (6, 'pending', 200000);
```

**Đọc fraud alerts từ Kafka:**
```bash
docker exec workshop-kafka \
  kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic fraud_alerts \
  --from-beginning
```

**Kết quả mong đợi:**
```json
{
  "user_id": 6,
  "window_start": "2024-01-15T10:00:00",
  "window_end":   "2024-01-15T10:05:00",
  "order_count":  4,
  "total_amount": 800000.0,
  "alert_level":  "HIGH"
}
```


Nếu muốn sửa code và chạy lại: 
  docker exec -u root workshop-spark-master bash -c 'rm -rf /opt/checkpoints/task3 && mkdir -p /opt/checkpoints/task3 && chmod -R 777 /opt/checkpoints/task3'


---

### Bonus — Inventory Alert (Stream-Static Join)

**Mục tiêu:** Alert khi sản phẩm tồn kho thấp.

```bash
bash scripts/run_job.sh bonus
```

Trigger alert:
```bash
bash scripts/mysql.sh demo_inventory
```

---

## 🏗️ Part 2 — Lakehouse ETL (Medallion Architecture)

Part 1 (Tasks 1–3) only *processes* the CDC stream — nothing is persisted.
**Part 2 is a self-study lab** that builds a full **Bronze → Silver → Gold**
pipeline on Delta Lake, **split one task per entity**: fact tables sync
bronze→silver via **streaming** (auto-CDC), dimension tables via **batch**.

> 📖 **Full lab guide (English):** [`docs/LAKEHOUSE_LAB.md`](docs/LAKEHOUSE_LAB.md)

Starter code (with `# TODO`s) lives in `spark/jobs/medallion/`; reference
solutions in `spark/jobs/solutions/`; shared helpers in
`spark/jobs/medallion/lake_utils.py`.

| Task | Entity | Kind | Focus | Run |
|------|--------|------|-------|-----|
| **4** | all | 🥉 Bronze | Streaming CDC → Delta (append-only) | `run_job.sh bronze` |
| **5** | `orders` | 🔵 fact | **Streaming** bronze→silver (foreachBatch + MERGE) | `run_job.sh orders` |
| **6** | `order_items` | 🔵 fact | **Streaming** bronze→silver | `run_job.sh items` |
| **7** | `products` | 🟢 dim | **Batch** dedup + MERGE | `run_job.sh products` |
| **8** | `users` | 🟢 dim | **Batch** dedup + MERGE | `run_job.sh users` |
| **9** | `dim_users` | 🟢 dim | **Batch** SCD Type 2 (tier history) | `run_job.sh scd2` |
| **10** | gold | 🥇 mart | Revenue mart + customer RFM | `run_job.sh gold` |
| **11** | gold | 🥇 mart | Reorder report + Spark SQL serving | `run_job.sh serving` |

Add `solution` to run the reference version, e.g. `bash scripts/run_job.sh orders solution`.

Quick end-to-end:
```bash
bash scripts/run_job.sh bronze solution     # terminal A: leave running
bash scripts/mysql.sh demo_cdc_changes      # terminal B: generate CDC changes
# Ctrl-C bronze, then run the silver jobs per entity + gold:
bash scripts/run_job.sh orders   solution   # fact,  streaming
bash scripts/run_job.sh items    solution   # fact,  streaming
bash scripts/run_job.sh products solution   # dim,   batch
bash scripts/run_job.sh users    solution   # dim,   batch
bash scripts/run_job.sh scd2     solution   # dim,   SCD2
bash scripts/run_job.sh gold     solution
bash scripts/run_job.sh serving  solution
```

Delta tables are written to `spark/warehouse/` (bronze/silver/gold).

---

## 🔧 Các lệnh thường dùng

### MySQL
```bash
bash scripts/mysql.sh console         # Mở MySQL console
bash scripts/mysql.sh show            # Xem trạng thái hiện tại
bash scripts/mysql.sh update_status   # Update pending → paid
bash scripts/mysql.sh reset           # Reset về seed data
```

### Kafka
```bash
# Xem danh sách topics
docker exec workshop-kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

# Xem messages trong topic orders
docker exec workshop-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic dbserver1.ecommerce.orders \
  --from-beginning \
  --max-messages 5

# Xem consumer group lag
docker exec workshop-kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --all-groups
```

### Debezium Connector
```bash
# Kiểm tra status
curl http://localhost:8083/connectors/mysql-ecommerce-connector-v1/status | python3 -m json.tool

# Restart connector nếu bị FAILED
curl -X POST http://localhost:8083/connectors/mysql-ecommerce-connector-v1/restart

# Xem danh sách connectors
curl http://localhost:8083/connectors

# Xóa và re-create connector
curl -X DELETE http://localhost:8083/connectors/mysql-ecommerce-connector-v1
bash scripts/setup.sh
```

### Spark
```bash
# Xem Spark logs
docker logs workshop-spark-master -f

# Xem Spark Streaming metrics (khi job đang chạy)
# → Mở http://localhost:4040/StreamingQuery
```

### Dọn dẹp
```bash
bash scripts/teardown.sh           # Dừng, giữ data
bash scripts/teardown.sh --clean   # Dừng + xóa hết
```

---

## 🐛 Troubleshooting

| Lỗi | Nguyên nhân | Fix |
|-----|-------------|-----|
| `Connection refused` khi curl Kafka Connect | Service chưa start xong | Chờ thêm 30s, re-run `check_status.sh` |
| Spark không thấy topic | Topic chưa có message | Insert 1 record vào MySQL |
| `AnalysisException: Append output mode not supported` | Aggregation không có watermark | Thêm `.withWatermark(...)` |
| Checkpoint error khi restart | Schema thay đổi | Xóa checkpoint: `rm -rf spark/checkpoints/task*` |
| `ts_ms` cho ra năm 51970 | Quên chia 1000 | `to_timestamp(col("ts_ms") / 1000)` |
| `before` field là null | Event là INSERT (`op=c`) | Check `col("op")` trước khi access `before.*` |
| Connector ở state FAILED | MySQL chưa có binlog hoặc user thiếu quyền | Check `docker logs workshop-connect` |
| RAM không đủ | Docker cần 6GB | Giảm `SPARK_WORKER_MEMORY` xuống 1G trong docker-compose |

---

## 📚 Tài liệu tham khảo

- [Debezium MySQL Connector](https://debezium.io/documentation/reference/connectors/mysql.html)
- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Watermarks in Spark](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-late-data-and-watermarking)
- [Kafka Connect REST API](https://docs.confluent.io/platform/current/connect/references/restapi.html)
