# THREAD V2: triển khai và vận hành

V2 chưa triển khai AWS. [AWS-VALIDATION.md](AWS-VALIDATION.md) ghi bằng chứng V1; [runbook V1](docs/history/DEPLOYMENT-v1.md) chỉ dùng để xem lại. Không chạy fixture hoặc uploader V1 với ledger V2.

## Trước triển khai

1. CI phải qua unit/reference, Spark/Iceberg, DynamoDB Local và lint/build.
2. Kiểm tra credits/quyền tài khoản thực tế. V2 dùng Glue 5.0, hai G.1X, timeout 15 phút, retry 0, concurrency 1. CloudWatch metrics/logs/alarms/dashboard, SNS và DynamoDB PITR cũng có thể phát sinh chi phí.
3. Xem xét bootstrap permissions boundary và deploy role: có thêm SNS, Glue Catalog V2, DynamoDB GSI, metrics/logs và quyền đọc trạng thái phục vụ alert.
4. Bootstrap cần cập nhật trước pipeline; V1 giữ quyền Scan chỉ trên bảng cũ để hỗ trợ giai đoạn chuyển đổi. Data bucket, ledger V1 và database V1 được giữ; input/output V2 nằm ở prefix riêng.
5. Tạo change set với release bất biến và xem thay đổi tài nguyên trước apply. Schedule phải giữ DISABLED.

```sh
python scripts/build_deploy.py
python scripts/deploy_release.py --stack thread-clothing-pipeline \
  --bucket <artifact-bucket> --prefix thread-runtime/<revision> \
  --profile aws-elt --region ap-southeast-1 --mode plan-only \
  --notification-email <recipient>
```

GitHub workflow `AWS deployment manual` cho nhập email, hoặc dùng repository variable `AWS_ALERT_EMAIL`. Địa chỉ cá nhân không được hardcode trong repository. Khi cập nhật và không truyền email, script giữ subscription hiện tại. Muốn xóa cấu hình email phải truyền chuỗi rỗng có chủ đích.

SNS gửi email xác nhận; người nhận bấm **Confirm subscription**. `PendingConfirmation` nghĩa là chưa nhận được cảnh báo. Không coi việc tạo resource thành công là email đã hoạt động.

## Monitor thành công/thất bại

```sh
python scripts/monitor.py --profile aws-elt
python scripts/monitor.py --profile aws-elt --run-token <token>
```

Lệnh chỉ đọc, hiển thị stack, năm Glue runs/workflow executions gần nhất, subscription, publication và ledger của run được chỉ định. Trên máy này profile nằm trong WSL; chạy bằng môi trường Python/boto3 có quyền truy cập profile đó.

| Bằng chứng | Ý nghĩa |
| --- | --- |
| Workflow SUCCEEDED, không có batch/Glue run | No-op; chưa chứng minh ETL xử lý dữ liệu |
| Glue SUCCEEDED | Compute hoàn tất; cần xem publication và completion |
| RUN COMPLETED + quality passed | Coordinator xác nhận publication của run |
| `latest.json.attempt` khớp token + snapshot map | Bộ dữ liệu gần nhất được công bố thuộc run đó |
| Workflow FAILED/TIMED_OUT/ABORTED | Lỗi workflow hoặc hậu xử lý; xem execution history |
| Glue FAILED/TIMEOUT/STOPPED | Lỗi Glue; email có job/run ID và lỗi từ GetJobRun |
| Email subscription PendingConfirmation | Cần người nhận xác nhận |
| Không có metrics | Có thể chưa chạy/telemetry lỗi; không chứng minh thành công |

CloudWatch dashboard `<stack>-operations` hiển thị trạng thái workflow, quality, thời lượng và DLQ. Log Glue tại `/aws-glue/<stack>/error` và `/output`; log JSON có run ID/stage. Audit tại `thread/output/v2/runs/<token>/audit.json`, kèm quarantine và quality issues khi được tạo. Lỗi khởi động/intake sớm có thể chưa có audit.

Glue/workflow failure events → Lambda alert → SNS email. Cùng Glue run và terminal state dùng một khóa chống gửi trùng trong DynamoDB; SNS standard vẫn có thể giao trùng. Lambda alert lỗi được retry và có SNS failure destination, kèm alarm Errors. DLQ/backlog quá một giờ cũng có alarm gửi SNS.

## Xử lý lỗi

- Lỗi contract/mapping/liên kết/đối soát: xem audit và dòng lỗi, sửa dữ liệu nguồn hoặc mapping đúng quy trình. Không công bố dữ liệu chưa qua quality gate.
- Glue thất bại: workflow cố gắng đánh dấu batch FAILED và giải phóng lease. Lần chạy sau có thể chọn lại batch; không tự retry Glue trong cùng workflow.
- Complete lỗi sau khi publication thành công: xác minh snapshot/publication/token trước khi redrive bước Complete, tránh chạy lại Glue không cần thiết.
- Workflow bị abort/timeout trước cleanup: batch có thể còn PROCESSING. Lease hết hạn không tự chuyển batch này về READY. Operator phải xác minh không còn Glue run hoạt động, khôi phục ledger có kiểm soát rồi mới chạy lại; chưa có lệnh recovery tự động.
- Một batch bị quality gate chặn không tự được bỏ qua. Cần quy trình sửa hoặc disposition có audit; hiện chưa có công cụ quản trị quarantine hoàn chỉnh.

Không xóa warehouse/snapshot còn được publication tham chiếu. Checkpoint scratch hết hạn sau một ngày; snapshot Iceberg, Spark event logs và raw cần chính sách retention/compaction riêng trước khi chạy dài hạn.

## Giới hạn cần phát triển tiếp

Connector thực tế, canonical market timezone, incremental curated materialization, recovery/disposition tooling, catalog validation sâu hơn và maintenance Iceberg còn cần hoàn thiện. Local integration dùng Hadoop Catalog; Glue Catalog/S3 IAM và email end-to-end phải được kiểm chứng riêng trên AWS. Lịch daily chỉ bật sau smoke test cloud và xem ngân sách.
