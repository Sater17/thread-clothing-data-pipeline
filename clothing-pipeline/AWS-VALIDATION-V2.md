# Kiểm chứng Glue Spark V2 — 15/10/2026

V2 đã được triển khai tại ap-southeast-1 và hoàn tất smoke test 24 sự kiện. Đây là kiểm chứng hạ tầng, runtime và một chuỗi mua–bán–trả nhỏ; chưa phải kiểm thử tải 500 đơn/ngày, 30 ngày lịch sử hoặc connector nguồn thật.

| Kiểm tra | Bằng chứng |
| --- | --- |
| Runtime đã triển khai | Source `334c52f74dfa38492e2e92e41b464e40c45ee4f6`; Glue 5.0, Spark 3.5.4, Iceberg; 2 G.1X, timeout 15 phút, retry 0 |
| CI | [Push CI](https://github.com/Sater17/thread-clothing-data-pipeline/actions/runs/38711967221) và [PR CI](https://github.com/Sater17/thread-clothing-data-pipeline/actions/runs/38712219680) thành công; 80 unit/reference, 8 Spark/Iceberg/entrypoint và 3 DynamoDB Local tests |
| Triển khai | Hai stack THREAD UPDATE_COMPLETE; bootstrap và pipeline được áp dụng qua change set đã xem xét bằng CLI operator; dữ liệu/catalog V1 giữ lại |
| Input | 33 file, 16 manifest, 24 sự kiện; raw/catalog gửi trước manifest |
| Tiếp nhận | S3 → EventBridge → SQS → Lambda đăng ký đủ 16 batch READY |
| Workflow cuối | `smoke-v2-274c931d3244448485f9c0013d220811`, SUCCEEDED |
| Glue cuối | `jr_1f78b8dd8df33fc619d67f18270339bc07c950915a66cbdbd311add583df878c`, SUCCEEDED; ExecutionTime 243 giây |
| Run token | `404760dab3794ea0967b2ca6513ee0ec` |
| Ledger | Cả 16 batch COMPLETED; đọc consistent xác nhận lease đã giải phóng |
| Công bố | `format_version=2`, `quality=passed`, 24 bảng trong snapshot map; pending/quarantine/quality issues đều 0 |
| Chống trùng khi retry | Bronze vẫn đúng 24 bản ghi sau các lần nhận lại cùng batch |
| Kiểm chứng độc lập | Đọc metadata, manifest list, data manifest của snapshot được publication chỉ tới, rồi đọc Parquet và so với oracle; không cộng tất cả file trong thư mục |
| Cảnh báo | SNS subscription đã xác nhận; ledger ghi SENT và MessageId cho lỗi Glue thực tế; người nhận xác nhận đã nhận thư lỗi |
| Schedule | DISABLED |

Kết quả đọc từ snapshot: kho có 57 hàng bán được và 2 hàng lỗi; cửa hàng có 20; lượng giữ hàng bằng 0. Thu 54,00 SGD, hoàn 27,00 SGD. Mua hàng: nhận 80, còn chờ 20, kiểm định 78 tốt/2 lỗi. Hoàn tiền cho hàng trả: 27,00 SGD. Stock lineage có 5 bản ghi.

Fixture cloud không có settlement hoặc inventory snapshot nên các bảng đó rỗng, vẫn có schema. Đối soát settlement, snapshot tồn và dữ liệu hai thị trường được kiểm thử bằng lịch sử sinh trong Spark integration; chưa được load test trên Glue.

## Các lỗi được phát hiện và sửa

Lần cloud đầu thất bại do gọi `GlueContext.spark_context`, thuộc tính không có trên Glue. Entry point hiện giữ SparkContext thật và gọi `setCheckpointDir` trên đối tượng đó; có regression test với SparkContext thật và Glue facade.

Lần tiếp theo thất bại khi analyzer của Glue xử lý hai nhánh checkpoint cùng giữ Catalyst attribute ID của `sku_id`. Các domain hiện alias cột đầu vào độc lập. Regression test kiểm tra ID khác nhau qua checkpoint và giữ các đối chiếu nghiệp vụ. Không tắt kiểm tra analyzer.

Hai lần thất bại đã đánh dấu batch FAILED và không công bố curated output. Sau sửa, cùng 16 batch được chạy lại thành công; Bronze không bị nhân đôi. Lỗi thật cũng dùng để kiểm chứng email, nên không cần tạo thêm job lỗi riêng.

## Tái kiểm tra

Trên máy Windows đang dùng WSL AWS profile `aws-elt`, chạy từ `clothing-pipeline`:

```powershell
python tools/validation/aws_smoke_v2.py inspect
python tools/validation/aws_smoke_v2.py subscriptions
python tools/validation/aws_smoke_v2.py alerts
python tools/validation/verify_publication.py
```

Verifier cần `pip install -e '.[validation]'`. Lệnh verifier chỉ đọc AWS, giới hạn warehouse download 20 MiB; kết quả, raw evidence và cache nằm dưới `data/aws-smoke-v2/`, được gitignore. Các lệnh upload/start/retry là thao tác chủ động, không được CI gọi.

V2 CD qua GitHub Actions chưa được áp dụng end-to-end trong lần chuyển đổi này; GitHub CI đã kiểm tra release, còn migration dùng CLI operator với change set. [Runbook](DEPLOYMENT.md) giữ các giới hạn hiện tại: curated rebuild toàn Bronze, recovery PROCESSING và quarantine disposition cần operator, maintenance Iceberg chưa tự động hóa, schedule chưa bật. Credits trước lần thử là 136,85 USD; số này không phải chi phí cuối vì billing cập nhật trễ.
