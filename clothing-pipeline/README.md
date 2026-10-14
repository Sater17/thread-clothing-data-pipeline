# THREAD clothing data pipeline

V2 đang được chuẩn bị trên nhánh `codex/production-spark`. ETL dùng PySpark 3.5.4, Apache Iceberg và Glue 5.0; chưa triển khai hay chạy V2 trên AWS. Kết quả [AWS V1](AWS-VALIDATION.md) chỉ áp dụng cho bản Python Shell cũ.

Pipeline ưu tiên dữ liệu chuẩn hóa của brand quần áo ở Việt Nam và Singapore: website, sàn, POS, kho/cửa hàng, mua hàng, bán hàng, trả hàng và đối soát. Adapter nguồn thực tế chưa được xây dựng; đầu vào hiện dùng contract chuẩn và simulator.

## Cấu trúc

| Đường dẫn | Trách nhiệm |
| --- | --- |
| `jobs/etl.py` | Entry point Glue: cấu hình Spark, SDK, input plan và publication |
| `src/thread_etl/contracts.py` | Contract sự kiện, tiền, số lượng và thời gian |
| `src/thread_etl/intake.py` | Manifest, checksum, kiểm tra file phân tán và đọc raw |
| `src/thread_etl/events.py` | Chuẩn hóa, chống trùng, chọn phiên bản, mapping SKU |
| `src/thread_etl/sales.py` | Đơn, thanh toán, giao hàng, giữ hàng và tồn |
| `src/thread_etl/documents.py` | Mua hàng, nhận hàng, trả hàng, kiểm định và stock lineage |
| `src/thread_etl/settlements.py` | Đối soát sàn và ngân hàng theo từng tiền tệ |
| `src/thread_etl/frames.py`, `transform.py` | Hàm Spark dùng chung, quality rules và điều phối transformations |
| `src/thread_etl/schema.py` | Schema công khai của 24 bảng |
| `src/thread_etl/storage.py` | Iceberg merge, ghi bảng và đọc snapshot được công bố |
| `src/thread_etl/pipeline.py` | Audit, quality gate và công bố sau khi tất cả bảng ghi xong |
| `src/thread_etl/observability.py` | Log JSON, stage/run ID, metrics |
| `src/cloud/control.py` | Batch ledger, query GSI, lease, plan bất biến và hoàn tất run |
| `src/cloud/notify.py` | Lấy lỗi Glue/workflow, chống gửi trùng và gửi SNS |
| `infra/` | CloudFormation pipeline và bootstrap OIDC/permissions boundary |
| `scripts/` | Build, lint, deploy có chủ đích, upload và monitor chỉ đọc |
| `tools/simulator/` | Sinh input và oracle độc lập; không đóng gói runtime |
| `tests/unit/` | Contract vận hành, release, alert và artifact |
| `tests/integration/` | Spark, Iceberg và DynamoDB Local |
| `tests/reference/` | Engine V1 và kiểm thử tham chiếu; không đóng gói runtime |
| `docs/history/` | Tài liệu V1, phục vụ xem lại lịch sử |

## Luồng xử lý

S3 manifest → EventBridge → SQS → Lambda đăng ký batch. Step Functions chốt input và lấy lease → Glue Spark đọc raw → Bronze Iceberg → transformations và quality gate → ghi curated tables → đổi `latest.json` → hoàn tất ledger.

Spark xử lý dữ liệu sự kiện bằng DataFrame/window/join. Driver chỉ giữ catalog, danh sách file có giới hạn và metadata kết quả. Python UDF được dùng tại biên parse/validation. DynamoDB V2 query theo trạng thái, không scan toàn ledger.

Bronze merge bổ sung event history, replay không nhân đôi nội dung. Curated hiện được dựng lại từ toàn bộ Bronze bằng Spark mỗi run; chưa phải incremental ETL hoàn chỉnh. Mapping thay đổi có thể yêu cầu chạy force rebuild.

`latest.json` chứa snapshot ID và schema của toàn bộ bảng. Consumer cần dùng `read_publication` để đọc đúng bộ snapshot. Query trực tiếp bảng Catalog đang được ghi không có tính nhất quán nhiều bảng. Quality gate thất bại giữ audit/quarantine và không đổi publication; raw/bronze hợp lệ đã nhận vẫn được giữ.

Tiền dùng decimal(20,2), không trộn VND/SGD; timestamp dùng UTC. `business_date` hiện lấy ngày trong timestamp nguồn, nên connector cần cung cấp đúng múi giờ nghiệp vụ.

## Kiểm thử và build

Python 3.11+ cho công cụ; Java 17 cho Spark. Chạy từ thư mục này:

```sh
python -m pip install -e '.[test,dev]'
python scripts/dev.py test --suite unit
python scripts/dev.py test --suite reference
python scripts/build_deploy.py
python scripts/lint_infra.py

docker build -f Dockerfile.test -t thread-pipeline-spark-tests:3.5.4 .
docker compose -p thread-pipeline-tests -f compose.test.yml run --rm tests
docker run --rm --memory 4g -v "$PWD:/workspace:ro" -w /workspace \
  -e PYTHONPATH=/workspace/src:/workspace \
  -e 'PYSPARK_SUBMIT_ARGS=--driver-memory 2g pyspark-shell' \
  thread-pipeline-spark-tests:3.5.4 \
  python -m pytest tests/integration -m spark -q -p no:cacheprovider
docker compose -p thread-pipeline-tests -f compose.test.yml down
```

Docker dùng runtime kiểm thử riêng và không nhận credentials AWS. GitHub CI chạy unit/reference, Spark/Iceberg/DynamoDB và lint/build. CD thủ công tạo change set trước, chỉ apply từ main sau khi CI qua. Artifact chỉ chứa code production và checksum.

Sinh dữ liệu thử riêng: `python scripts/dev.py history --output data/demo/input --days 2 --orders-per-day 20 --skus 12 --daily-snapshots`. Không upload oracle lên input production.

Xem [runbook triển khai và monitoring](DEPLOYMENT.md). Schedule mặc định tắt; V2 dùng tối thiểu hai G.1X, nên chi phí khác V1 Python Shell.
