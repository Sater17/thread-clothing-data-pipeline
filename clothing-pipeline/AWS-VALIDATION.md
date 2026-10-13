# Kết quả triển khai AWS ngày 13/10/2026

Pipeline đã được triển khai và kiểm chứng với fixture nhỏ trên Free plan tại ap-southeast-1. Đây là kết quả smoke test 24 sự kiện, chưa phải kiểm chứng tải 500 đơn/ngày hoặc kết nối nguồn bán hàng thật.

| Kiểm tra | Kết quả |
| --- | --- |
| Stack bootstrap | thread-deployment-bootstrap, tạo bucket code + GitHub OIDC + deploy role + runtime boundary |
| Stack pipeline | thread-clothing-pipeline, UPDATE_COMPLETE |
| CD release cuối có code sửa | [GitHub Actions run #4](https://github.com/Sater17/thread-clothing-data-pipeline/actions/runs/38378624157), Success |
| Input | 33 file, 16 manifests, 24 events; catalog/raw gửi trước manifest |
| Tiếp nhận | EventBridge → SQS → Lambda đăng ký đủ 16 batch READY |
| Glue | Một run SUCCEEDED, ExecutionTime 33 giây, MaxCapacity 0,0625 DPU, retry 0 |
| Workflow | smoke-v1 SUCCEEDED, redriveCount 1 |
| Ledger cuối | 16 batch COMPLETED, không còn LOCK |
| Công bố | quality=passed, kết quả khớp oracle, 21 bảng có trong Glue Catalog |
| Schedule | DISABLED |
| Bộ test tại release | 69 tests pass, Python 3.9/3.12 và smoke/lint/build đều pass trong CD |

Kết quả nghiệp vụ: kho 57 sản phẩm bán được và 2 hàng lỗi; cửa hàng 20; reserved 0; collected 54,00 SGD; refunded 27,00 SGD.

Lần chạy đầu gặp lỗi completion do token là DynamoDB reserved keyword. Glue đã thành công và công bố output trước lỗi này. Release cuối dùng expression alias để giải phóng lease; Step Functions redrive chạy lại riêng Complete và giữ kết quả Glue, đã kiểm tra tổng số Glue runs vẫn bằng 1. Regression tests bao gồm completion gián đoạn và không xóa lease của run mới.

Lần tạo stack đầu bị rollback do tên Step Functions tự sinh nằm ngoài prefix quyền và thiếu ARN userDefinedFunction khi dọn Glue database. Đã đặt tên workflow cố định, bổ sung đúng ARN trong deploy policy, xác minh tài nguyên thử trống rồi dọn phần thất bại. Stack ELT cũ không bị sửa.

Credit Free plan được đọc sau thử nghiệm: 137,13 USD, so với 137,17 USD ở đầu phiên. Chênh lệch là số dư toàn tài khoản tại hai thời điểm; không coi là hóa đơn cuối cùng hoặc chi phí riêng của pipeline, vì billing có độ trễ và tài khoản có dự án khác. Không đổi gói tài khoản hay bật lịch.

Evidence local nằm dưới data/aws-smoke/: execution-status.json, latest.json, result.json, catalog-tables.json và cloud-checks.json. Thư mục data không được commit. Data bucket hiện tại: thread-clothing-pipeline-databucket-yusspcghuskl. Workflow ARN và Glue job đọc từ stack outputs; không hardcode bucket này trong code pipeline.

Bước tiếp theo phù hợp là chạy một bộ lịch sử nhỏ có snapshots và tình huống lỗi/late-arrival trên cloud, đo tài nguyên trước khi nâng MaxRecords hoặc bật lịch. Chưa chạy thêm bộ dữ liệu nào sau smoke test này.
