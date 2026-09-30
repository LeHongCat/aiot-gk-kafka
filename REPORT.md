# Báo cáo benchmark: Apache Kafka - Tải sensor request qua Consumer Group (Nhóm 2, MDL26)

Kafka 4.2.0 (1 broker, 3 partition) nhận dữ liệu cảm biến giả lập; 1 hoặc 3 consumer cùng group xử lý. Đo thông lượng (msg/s), consumer lag, CPU/RAM và kiểm tra không mất/không trùng.

## Kịch bản

Mỗi lượt gửi 60 giây, bỏ 5 giây đầu khi tính. T1–T4 lặp 3 lần, S1/S3 chạy 1 lần. Dữ liệu: 100 sensor, key = `sensor_id`.

| Kịch bản | Tải gửi | Consumer | Mục đích |
|---|---|---:|---|
| T1 | 1.000 msg/s | 1 | Baseline |
| T2 | 5.000 msg/s | 1 | Một consumer có nghẽn không? |
| T3 | 5.000 msg/s | 3 | Thêm consumer có nhanh hơn không? |
| T4 | 10.000 msg/s | 3 | Mức tải cao nhất của đề |
| S1 / S3 | 1.000 msg/s, tăng lên 10.000 trong giây 20–30 | 1 / 3 | Tải tăng đột biến (consumer chậm giả lập 0,2 ms/message) |

## Cách chạy

Cài đặt và bật Kafka theo [README.md](README.md), rồi:

```bash
python -m monitoring.run_scenario --run-id T3-r1 --rate 5000 --consumers 3 --duration 60
python -m monitoring.run_scenario --run-id S3-r1 --rate 1000 --spike-rate 10000 --spike-start 20 --spike-duration 10 --consumers 3 --duration 60 --delay-ms 0.2 --drain-timeout 240
python -m monitoring.summary
```

Đổi `--rate`, `--consumers` và `--run-id` cho các kịch bản khác. Mỗi lượt tự kiểm tra mất/trùng và lưu kết quả trong `output/<run-id>/`.

## Kết quả

Trung bình 3 lần (S1/S3: 1 lần). Khoảng min–max: [results/summary.csv](results/summary.csv); số gốc từng lượt: [results/all_runs.csv](results/all_runs.csv).

| | Consumer | Producer (msg/s) | Consumer (msg/s) | Max lag | Drain (s) | Kafka CPU / RAM | Python CPU / RAM |
|---|---:|---:|---:|---:|---:|---|---|
| T1 | 1 | 1.000 | 1.002 | 1.035 | 1 | 92% / 1.179 MB | 29% / 92 MB |
| T2 | 1 | 5.000 | 5.001 | 4.003 | 1 | 108% / 1.195 MB | 62% / 93 MB |
| T3 | 3 | 5.000 | 4.997 | 3.983 | 1 | 120% / 1.212 MB | 63% / 155 MB |
| T4 | 3 | 10.000 | 9.994 | 7.306 | 1 | 119% / 1.236 MB | 85% / 156 MB |
| S1 | 1 | 2.667 | 1.467 | 85.163 | 42 | 74% / 1.314 MB | 33% / 142 MB |
| S3 | 3 | 2.667 | 2.667 | 55.430 | 1 | 91% / 1.315 MB | 57% / 183 MB |

Lỗi gửi: 0 ở mọi lượt. Drain = thời gian đọc hết backlog sau khi producer dừng. CPU: 100% = một nhân. Ở S1/S3, tốc độ là trung bình trong cửa sổ đo (đã bỏ 5 giây đầu tải thấp), nên cao hơn trung bình cả lượt 2.500 msg/s.

**Kiểm tra mất/trùng:** Missing = 0, Duplicates = 0 ở cả 14 lượt.

**Chia tải (T3):** mỗi consumer giữ đúng một partition.

| Consumer | Partition | Số message | Tỷ lệ |
|---|---|---:|---:|
| C1 | 0 | 117.392 | 39,1% |
| C2 | 1 | 98.638 | 32,9% |
| C3 | 2 | 83.969 | 28,0% |

![Thông lượng T4](results/T4-r1_throughput.png)
![Tải theo consumer, T3](results/T3-r1_distribution.png)
![Lag S1 (1 consumer)](results/S1-r1_consumer_lag.png)
![Lag S3 (3 consumer)](results/S3-r1_consumer_lag.png)

## Kết luận

- Producer đạt đúng 1.000, 5.000 và 10.000 msg/s, consumer theo kịp (≥ 99,9%), không lỗi gửi, không mất, không trùng.
- Một consumer đã đủ cho 5.000 msg/s, nên T3 không nhanh hơn T2. Thêm consumer chỉ có ích khi consumer là điểm nghẽn: khi tải tăng đột biến và xử lý chậm, 1 consumer mất 42 giây đọc hết backlog, 3 consumer mất khoảng 1 giây.
- Tải chia theo partition (39% / 33% / 28%), không đều tuyệt đối vì 100 sensor được băm vào 3 partition.
- Max lag ở T1, T2, T4 tương ứng khoảng 1 giây tải, phù hợp với chu kỳ commit offset 1 giây, không phải backlog tích tụ.

## Giới hạn

- Một broker, replication factor 1: chứng minh được chịu lỗi ở mức consumer, chưa ở mức broker.
- S1/S3 chỉ đo một lần; T4 chưa đo với một consumer; độ trễ consumer là mô phỏng.
- Kết quả phụ thuộc máy chạy.
