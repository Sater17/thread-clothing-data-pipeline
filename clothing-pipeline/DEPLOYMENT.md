# Triển khai thử pipeline THREAD trên AWS

Đây là runbook cho bước triển khai sau khi xem xét tài khoản và ngân sách. Các tài nguyên chưa được tạo. Chuẩn bị code và lint không đồng nghĩa Glue đã chạy thành công trên AWS.

## Phạm vi lần thử đầu

Dùng fixture nhỏ 24 sự kiện để kiểm tra tiếp nhận và Glue. Lịch tự chạy mặc định DISABLED; gọi Step Functions thủ công một lần, đọc kết quả và chi phí rồi mới tăng dữ liệu. Bộ bảy ngày đã kiểm chứng local có hơn 10.000 sự kiện, không phù hợp với giới hạn cloud mặc định trước khi điều chỉnh có chủ đích.

Tham số mặc định: Glue Python Shell 3.9, capacity 0.0625 DPU, timeout 10 phút, retry 0, một job cùng lúc, giới hạn ledger cộng input chọn tối đa 10.000 bản ghi. Đây là hàng rào cho lần thử, không phải cam kết miễn phí. Credits, quyền lợi Free plan và dịch vụ được phép dùng phải được kiểm tra trên tài khoản thực tế.

Dependency cloud dự kiến: numpy 1.26.4 và pyarrow 12.0.1. Core dùng Python 3.9 trở lên; Lambda dùng Python 3.12. Việc cài dependency và quyền runtime còn cần kiểm chứng khi chạy thử cloud.

## Tài nguyên được tạo bởi stack

| Thành phần | Trách nhiệm |
| --- | --- |
| S3 bucket riêng | Input bất biến, state, output và run plans; chặn public và HTTP |
| SQS và DLQ | Nhận thông báo manifest, giữ lỗi tiếp nhận |
| EventBridge rule | Chỉ chuyển sự kiện tạo object dưới thread/input/manifests/ vào queue |
| Lambda | Kiểm tra/đăng ký đợt, chốt input và hoàn tất lần chạy |
| DynamoDB | Batch ledger, run ledger, lease và dấu catalog |
| Glue job | Chuẩn hóa, tích hợp và xuất Parquet |
| Glue database | Bảng riêng của từng run, đăng ký trước công bố |
| Step Functions Standard | Plan → Glue sync → complete, hoặc mark failed |
| Daily rule | 02:00 giờ VN; mặc định tắt |
| CloudWatch | Log Lambda giữ bảy ngày, alarm ExecutionsFailed |
| IAM roles | Quyền theo bucket/prefix/table/job của stack |

Artifact bucket là bucket code có sẵn cùng region, truyền bằng parameter. Nếu chưa có, việc tạo bucket đó là một phần riêng của bước triển khai. Dùng prefix release mới cho code; không ghi đè code của release cũ.

## Chuẩn bị offline

```powershell
python scripts/build_deploy.py
python scripts/lint_infra.py
python scripts/dev.py test
python scripts/dev.py sample --output data/aws-smoke/input
python scripts/upload_inputs.py --input data/aws-smoke/input --bucket REVIEW_BEFORE_DEPLOY
```

Lệnh upload trên chỉ in kế hoạch; không gọi AWS khi không có --apply. Dist gồm glue_job.py, control.zip, cloudformation.json, release.json. Release ghi SHA256 các asset. Runtime bundled dùng scripts/dev.py để đọc dependency trong .deps. Không đưa credentials hoặc dữ liệu sinh ra vào Git.

Uploader ở bước apply cần boto3, cài bằng `python -m pip install '.[cloud]'` trong môi trường Python riêng, hoặc cài vào .deps khi dùng runtime bundled. Build/lint không cần AWS credentials. Glue và Lambda dùng SDK từ runtime AWS.

## CI/CD trên GitHub

CI chạy khi push hoặc mở pull request: unit/integration tests trên Python 3.9 và 3.12, cfn-lint, build release và smoke test hai ngày có snapshot. Python 3.9 dùng numpy 1.26.4 và pyarrow 12.0.1 tương ứng cấu hình Glue. CI không truy cập AWS.

CD chỉ chạy bằng workflow_dispatch trên main, sau khi toàn bộ CI pass. Chế độ mặc định plan-only upload code và tạo change set; apply mới thực thi. Deploy không upload dữ liệu nguồn hoặc chạy Glue, và luôn đặt ScheduleState=DISABLED. Release được kiểm tra SHA256 trước khi upload vào prefix theo commit.

Trước khi chạy CD, cần cấu hình repository variables AWS_ARTIFACT_BUCKET và AWS_DEPLOY_ROLE_ARN. Bucket phải thuộc tài khoản triển khai, cùng region ap-southeast-1. Role dùng GitHub OIDC, trust giới hạn audience sts.amazonaws.com và subject repo:Sater17@79832416/thread-clothing-data-pipeline@1416576645:ref:refs/heads/main. Không lưu access key dài hạn trên GitHub.

infra/bootstrap.py sinh stack riêng thread-deployment-bootstrap gồm bucket code, OIDC provider (hoặc dùng lại provider có sẵn), role thread-github-deploy và managed policy thread-runtime-boundary. Pipeline stack bắt buộc boundary đó tồn tại trước khi tạo runtime roles. Role deploy chỉ tạo runtime role có boundary đúng, không có quyền sửa/xóa boundary. Boundary không cho runtime quản lý IAM. Quyền deploy giới hạn tên tài nguyên của dự án, database thread_pipeline và prefix thread-runtime; API Lambda event-source-mapping có một số quyền không giới hạn resource vì mapping dùng UUID. Đây là quyền quản lý hạ tầng, không cấp cho connector bán hàng.

scripts/aws_cli.py là helper tùy chọn cho môi trường Windows có AWS profile aws-elt trong WSL Ubuntu. Helper chuyển file:// sang đường dẫn /mnt, gọi CLI bằng argv và không đọc credentials. Các thao tác tạo change set, execute và upload phải gọi tường minh; import/build không gọi AWS.

scripts/aws_smoke.py upload dùng đúng fixture data/aws-smoke/input (24 sự kiện, tối đa 100 KB), gửi catalog/raw trước manifest và kiểm tra SHA256 khi object đã tồn tại. start chỉ gọi workflow khi mọi batch đã READY trong DynamoDB, đặt execution name smoke-v1 và lưu ARN để tránh chạy trùng. inspect tải marker/result và so với oracle local sau khi workflow SUCCEEDED. Các lệnh này gọi AWS thật, không chạy trong CI.

Lệnh local tương đương: python scripts/deploy_release.py --stack thread-clothing-pipeline --bucket BUCKET --prefix thread-runtime/COMMIT --profile aws-elt --mode plan-only. Lệnh mặc định không thực thi change set, nhưng vẫn ghi release assets lên S3 và tạo change set trên AWS.

## Trình tự sau khi được phép triển khai

1. Xác nhận profile, account, region (dự kiến ap-southeast-1), plan và credits; kiểm tra quyền tạo IAM/Glue và ngân sách thử.
2. Upload code vào artifact bucket/prefix mới. Tạo CloudFormation change set từ dist/cloudformation.json, với ScheduleState=DISABLED.
3. Xem change set, triển khai stack và lấy outputs. Không thay stack của dự án ELT cũ.
4. Upload catalog, raw rồi manifest theo thứ tự; upload_inputs.py --apply dùng conditional put và SHA256 metadata để không ghi đè input khác nội dung.
5. Kiểm tra DynamoDB: các đợt đã đủ file phải READY. Nếu WAITING, bổ sung file; nếu DLQ có message, kiểm tra lỗi manifest.
6. StartExecution của Workflow với input {}. Tránh StartJobRun trực tiếp vì sẽ bỏ qua sổ đợt.
7. Đọc Workflow, Glue run, latest.json, audit và quality. Bảng được query theo mapping catalog trong latest.json, không đoán tên bảng mới nhất.
8. Kiểm tra phí/credits sau lần thử. Chỉ bật lịch hoặc nâng capacity/MaxRecords sau số đo thực tế.

Không có lệnh deploy tự chạy trong build scripts. Các lệnh AWS tại bước này cần được thực hiện trong phiên triển khai đã được người dùng cho phép.

## Công bố và phục hồi

Một run chọn danh sách manifest và hash catalog cố định. File hoặc catalog đổi sau khi plan làm run thất bại. Những manifest đến sau chờ run tiếp theo. Đợt WAITING/FAILED được kiểm tra lại khi plan. Catalog thay đổi có thể kích hoạt replay ledger khi không có batch mới.

Job tải các input đã chọn, khôi phục SQLite checkpoint, replay lịch sử và ghi kết quả dưới prefix mới. Đăng ký bảng Glue theo tên table_runToken trước khi đổi latest.json. Marker liên kết checkpoint, audit, Parquet và catalog cùng một run. Table của run trước không bị cập nhật giữa chừng.

Một Glue job cùng lúc và lease hai giờ bảo vệ đường điều phối. Workflow timeout 15 phút, Glue timeout 10 phút. Nếu workflow bị dừng thủ công sau khi có publication nhưng trước completion, kiểm tra token trong marker và RUN record trước khi phục hồi. Không tự xóa lease hay đổi trạng thái khi job còn chạy.

Nếu Glue đã SUCCEEDED nhưng task Complete thất bại, sửa nguyên nhân và dùng Step Functions redrive để chạy lại task bị lỗi, giữ kết quả Glue đã thành công. complete nhận biết RUN terminal và chỉ giải phóng lease nếu token vẫn thuộc run đó; không giải phóng lease của run mới. Các biểu thức DynamoDB dùng alias cho thuộc tính token để tránh reserved keyword. Không start execution mới hoặc force replay chỉ để khắc phục lỗi completion.

Quality issues được lưu để điều tra, không có nghĩa dữ liệu đã nghiệm thu. Batch bị blocked ở Glue được đánh FAILED để xử lý lại. Bản ghi chờ mapping/liên kết được replay khi input mới hoặc catalog mới xuất hiện.

## Hiệu chỉnh có kiểm soát

Không sửa raw đã tiếp nhận. Với sai khác cùng entity/version, xác nhận bản sai rồi thêm event_rejection từ nguồn pipeline_control, có target source/scope/event ID và reason. Bản bị loại vẫn còn trong raw/standardized với rejected=true; bản thay thế dùng event ID mới. Chỉ operator có quyền viết input quản trị được dùng luồng này, không cấp quyền đó cho connector bán hàng.

Control event hiện xử lý các event hợp lệ đã vào ledger; không tự làm sạch quarantine hoặc suy diễn sửa tồn. Các kiểm tra nguồn gốc/chất lượng vẫn chạy sau hiệu chỉnh.

## Giới hạn v1

V1 dùng snapshot Parquet và replay ledger, chưa dùng Iceberg/Spark hay xử lý phân tán. Sổ đợt chọn input tăng dần, nhưng việc dựng integrated vẫn đọc lịch sử đã nhận. Tăng dữ liệu cần đo RAM/thời gian và chuyển compute nếu vượt Python Shell.

Đối soát sàn hiện là bản chốt toàn vòng đời mỗi đơn, một lần trong dữ liệu chuẩn. Không hỗ trợ delta settlement nhiều kỳ, FX, thuế thực, carry settlement thanh toán âm hay thanh toán nhà cung cấp. Phí 5% trong simulator là giả định thử, không phải phí của sàn thật.

Source fixtures dùng contract chung. Adapter cho schema/API của nền tảng thật cần cấu hình theo nền tảng được chọn. Bộ mô phỏng không kết nối cửa hàng/sàn thật.

## Giữ dữ liệu và dọn tài nguyên

S3 và DynamoDB dùng Retain để xóa stack không làm mất dữ liệu. Điều này cũng có nghĩa xóa stack chưa dừng mọi phí lưu trữ: phải dọn bucket/table riêng sau khi lưu kết quả cần giữ. Lịch tắt, không có EC2/NAT/Redshift chạy thường trực.

Các run cũ và catalog tables cần được dọn theo chính sách được chọn; không xóa run mà latest.json còn tham chiếu. V1 chưa tự xóa raw/state theo tuổi vì có thể mất khả năng chạy lại. CloudWatch alarm hiện hiển thị trong console, chưa có SNS/email thông báo.

Tham khảo: [Glue Python Shell](https://docs.aws.amazon.com/glue/latest/dg/add-job-python.html), [Glue sync với Step Functions](https://docs.aws.amazon.com/step-functions/latest/dg/connect-glue.html), [S3 Event Notifications](https://docs.aws.amazon.com/AmazonS3/latest/userguide/EventNotifications.html).
