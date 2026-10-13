# THREAD clothing data pipeline

Code v1.0 của [thiết kế v1](../clothing-data-pipeline-design-v1.md): simulator hai thị trường, mua/bán/trả, đối soát sàn và hiệu chỉnh có audit. Đã triển khai AWS qua GitHub OIDC/CD và chạy thử 24 sự kiện thành công ngày 13/10/2026. [Kết quả kiểm chứng](AWS-VALIDATION.md) và [runbook](DEPLOYMENT.md) ghi rõ phạm vi, cấu hình và giới hạn; lịch tự chạy vẫn tắt.

## Chạy trên máy

Python 3.9 trở lên, không cần thư viện ngoài cho pipeline local. Chạy từ thư mục `clothing-pipeline`:

```powershell
python -m thread_pipeline sample --output data/demo/input
python -m thread_pipeline run --input data/demo/input --state data/demo/ledger.sqlite --output data/demo/output --check-sample
python -m unittest discover -s tests -v
python scripts/build_glue.py
```

Nếu không có `python` trong PATH, dùng đường dẫn Python đầy đủ. Trong workspace này đã kiểm tra bằng runtime:

```powershell
& 'C:\Users\SATER\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe' -m unittest discover -s tests -v
```

Generator từ chối ghi đè thư mục input đã tồn tại. Chạy lại lệnh `run` với cùng state là kiểm tra replay; không chạy lại generator vào cùng đường dẫn.

## Đã có

- 24 sự kiện cố định, gom theo giờ/nguồn; SKU/location mapping, manifest, SHA256 và oracle riêng.
- Kiểm tra contract, tiền thập phân, thời gian có múi giờ, số lượng và công thức dòng đơn.
- SQLite giữ ledger và từng lần nhận, tránh trùng event theo nguồn/phạm vi/mã.
- Chặn đợt thiếu file/checksum sai; cách ly dòng lỗi hoặc xung đột; giữ bản ghi chờ mapping/liên kết.
- Replay lịch sử dựng tồn, lượng giữ, đơn và giao dịch tiền; sửa mapping có thể được áp dụng mà không gửi lại raw.
- Snapshot kết quả và con trỏ công bố local; adapter S3 ghi checkpoint/output vào prefix mới rồi đổi con trỏ sau cùng.
- Entry point Glue Python Shell đóng gói cùng core, không cần cài project từ mạng.
- Simulator có cấu hình số ngày/đơn/SKU/seed, bảy địa điểm, website/sàn/POS, nhiều dòng hàng, thanh toán thử lại, trả một phần và hàng trả lỗi.
- Kết quả đối chiếu toàn lịch sử tách tiền theo VND/SGD và tồn theo SKU–địa điểm–tình trạng.
- Xuất 21 bảng Parquet có schema cố định, tiền decimal128(20,2), thời gian UTC và ngày nghiệp vụ địa phương; bảng rỗng vẫn có schema.

## Chạy lịch sử và Parquet

```powershell
python -m pip install '.[parquet]'
python -m thread_pipeline history --output data/history/input --days 30 --orders-per-day 500 --skus 500 --seed 42
python -m thread_pipeline run --input data/history/input --state data/history/ledger.sqlite --output data/history/output --check-history --parquet
```

30 ngày là khoảng phát sinh đơn mới. Seed hàng trước ngày đầu; giao hàng, trả hàng và hoàn tiền tiếp tục sau ngày cuối để khép vòng đời. SKU được phân bố thành các mẫu với hai màu và năm size khi dùng 500 SKU. Các khoản tiền/thuế trong simulator là giả định thử, không phải mô hình thuế từng nước.

Để dùng runtime bundled và thư viện cài riêng trong `.deps`, chạy `scripts/dev.py` thay cho `-m thread_pipeline`:

```powershell
python -m pip install --target .deps pyarrow
python scripts/dev.py history --output data/history/input
python scripts/dev.py run --input data/history/input --state data/history/ledger.sqlite --output data/history/output --check-history --parquet
python scripts/dev.py test
```

`config.json` giữ tham số mô phỏng; `expected-history.json` giữ số đơn/dòng, tiền và tồn được tính từ các hành động kinh doanh của simulator. Pipeline không đọc oracle khi xử lý, chỉ CLI kiểm chứng sau khi hoàn tất. Cùng seed/cấu hình tạo cùng input, gồm cả checksum.

Output Parquet nằm dưới `runs/<run-id>/tables/`; `latest.json` chứa tên file, số dòng và SHA256 từng bảng. Chỉ đọc phiên bản mà con trỏ chỉ tới. Standardized giữ payload/provenance dạng JSON để bảo toàn dữ liệu nguồn; bảng nghiệp vụ có cột được định kiểu. Đây là snapshot Parquet, chưa dùng Iceberg. Local không đăng ký Catalog; adapter cloud đăng ký bảng Glue riêng theo run trước khi đổi latest.json.

## Phiên bản dữ liệu và snapshot v0.3

`order`, `payment` và `inventory_snapshot` hỗ trợ bản cập nhật toàn bản ghi qua `source_version`, số nguyên dương ở envelope. Bản không có trường này được hiểu là phiên bản 1 để đọc dữ liệu cũ. Mỗi revision có `event_id` riêng; mã đối tượng trong payload giữ nguyên.

- Chọn phiên bản lớn nhất theo đối tượng và phạm vi nguồn, không chọn theo thời gian nhận hoặc thời gian xảy ra.
- Giữ mọi revision trong standardized, thêm `source_version` và `is_current`; bảng trạng thái chỉ dùng bản được chọn.
- Bản mới thiếu mapping giữ trạng thái chờ, không dùng bản cũ thay thế để tạo dữ liệu tưởng là hiện tại.
- Hai payload khác nhau cho cùng đối tượng/phiên bản làm job thất bại trước khi công bố; ledger giữ bằng chứng. Cần xử lý xung đột nguồn trước khi xây lại ledger, không tự chọn một bản.
- Giao dịch tiền không được đổi đơn, tiền tệ, loại thu/hoàn hoặc giao dịch gốc giữa các phiên bản.
- Stock movements, reservations và delivery vẫn là sự kiện append-only. Bản sửa loại này chưa hỗ trợ; `source_version` lớn hơn 1 bị cách ly.

Ví dụ transaction PAY-001 có ba sự kiện PAY-V1, PAY-V2, PAY-V3. Dù PAY-V2 tới sau V3, chỉ V3 tạo trạng thái giao dịch hiện tại. Đây là cập nhật cùng transaction; một lần thử thanh toán mới vẫn phải có transaction ID mới.

Snapshot tồn là đối chiếu theo từng dòng SKU–địa điểm–tình trạng, không tự coi dòng vắng là 0. Snapshot dùng `as_of` có múi giờ. Tại mốc T, so số lượng nguồn với tổng biến động có `occurred_at <= T`. Ánh xạ SKU cũng chọn theo `as_of`, không theo lúc file đến.

`inventory_reconciliation` ghi expected, actual, delta = actual − expected, passed/failed, thời điểm, phiên bản, event ID, file raw và batch. Snapshot không tạo biến động tồn. Một chênh lệch có thể do nguồn sai hoặc dữ liệu chưa đến đủ; trạng thái đợt/pending phải được xem cùng kết quả đối chiếu.

```powershell
python scripts/dev.py history --output data/snapshots/input --days 7 --orders-per-day 50 --skus 50 --daily-snapshots
python scripts/dev.py run --input data/snapshots/input --state data/snapshots/ledger.sqlite --output data/snapshots/output --check-history --parquet
```

Cờ `--daily-snapshots` tạo snapshot từ sổ hàng phía simulator tại mốc cuối ngày của từng thị trường, gồm cả các ngày hoàn tất giao/đổi trả sau cửa sổ bán. Manifest dùng `snapshot_rows` cho các file chỉ chứa dòng snapshot; không tuyên bố full snapshot ngoài phạm vi các dòng đã gửi.

Kiểm chứng v0.3: 30 tests đạt, gồm revision đến sai thứ tự, xung đột cùng phiên bản, mapping của bản mới còn thiếu, snapshot tại cùng timestamp với biến động và phục hồi khi biến động đến muộn. Bộ chạy local bảy ngày, 50 đơn/ngày, 50 SKU có 350 đơn, 9.909 sự kiện/dòng snapshot; cả 4.495 dòng đối soát snapshot đều đạt. Chín bảng Parquet được xuất; không còn pending/quarantine/blocked/check thất bại. Dữ liệu minh chứng ở `data/snapshots-v1/`, không đưa vào Git.

## Chứng từ trả hàng và hoàn tiền v0.4

Chuỗi liên kết: dòng đơn → dòng yêu cầu trả → các lần nhận → các lần kiểm tra. Giao dịch hoàn tiền → các dòng phân bổ → dòng yêu cầu trả. Biến động tồn → chứng từ nhận/kiểm tra → dòng trả → dòng đơn.

| Loại sự kiện | Mã và tham chiếu chính |
| --- | --- |
| return_request | return, return_line, order_source/scope/order/line, SKU, quantity, reason |
| return_receipt | receipt, return_source/scope/return_line, SKU, location, quantity |
| return_inspection | inspection, receipt_source/scope/receipt, SKU, location, quantity, condition |
| refund_allocation | allocation, transaction trong nguồn/phạm vi thanh toán, return_source/scope/return_line, amount, currency |

Các chứng từ mới append-only, chưa hỗ trợ sửa/xóa. Mã chỉ duy nhất trong nguồn/phạm vi tương ứng. Một dòng trả có thể nhận nhiều đợt; một lần nhận có thể kiểm tra nhiều phần. Một giao dịch hoàn có thể phân bổ cho nhiều dòng trả, và một dòng trả có thể nhận nhiều khoản hoàn.

Stock movement liên quan ghi đủ `document_type`, `document_source`, `document_scope`, `document_id`. Nhận trả phải chuyển từ khách tới chờ kiểm tra; kết quả kiểm tra phải chuyển từ chờ kiểm tra sang sellable/defective tại cùng địa điểm. Chứng từ không trực tiếp tác động tồn; pipeline chỉ đối chiếu với các biến động riêng để tránh cộng hai lần.

Kiểm tra gồm SKU/địa điểm, thứ tự nhận–kiểm tra, lượng yêu cầu so với đã giao, lượng nhận so với yêu cầu, lượng kiểm tra so với thực nhận, lượng biến động so với chứng từ, đơn/tiền tệ của phân bổ và tổng phân bổ so với khoản hoàn. Thiếu tham chiếu được giữ chờ; phân bổ thiếu tiền là waiting, vượt tiền là failed. Các kết quả này đi vào chất lượng công bố.

Khoản hoàn cho trả hàng ghi `refund_reason=return` để bắt buộc đối chiếu phân bổ. Input cũ chưa có nhãn này vẫn đọc được; khoản hoàn chưa được phân loại không được coi là đã kiểm chứng liên kết với trả hàng.

Năm bảng mới: `return_lines`, `return_receipts`, `return_inspections`, `refund_allocations`, `stock_lineage`. Dòng trả có `received_quantity`, `inspected_quantity`, `refunded_amount` (chỉ cộng giao dịch succeeded). Bảng giữ event ID, file raw, batch, mã nguồn và mã chuẩn để truy ngược.

```powershell
python scripts/dev.py history --output data/returns/input --days 7 --orders-per-day 50 --skus 50 --daily-snapshots
python scripts/dev.py run --input data/returns/input --state data/returns/ledger.sqlite --output data/returns/output --check-history --parquet
```

Dùng thư mục mới để tái tạo fixture/simulator có chứng từ mới. Không sửa payload hoặc manifest của các đợt cũ đã được tiếp nhận. Giá trị tiền–tồn kỳ vọng giữ nguyên với cùng cấu hình/seed; số sự kiện tăng do thêm chứng từ.

Kiểm chứng unit v0.4: 40 tests đạt, bao gồm split receipt, kết quả kiểm tra lẫn hàng bán được/hàng lỗi, split allocation, thiếu chứng từ đến muộn, gửi trùng, sai tiền tệ và vượt số lượng.

Chạy local bộ bảy ngày/350 đơn/50 SKU có 10.109 sự kiện và dòng snapshot, gồm 50 dòng trả, 50 lần nhận, 50 kiểm tra, 50 phân bổ hoàn tiền và 100 liên kết biến động tồn. Tiền–tồn khớp oracle; cả 14 bảng Parquet được xuất, chất lượng passed, không còn pending/quarantine/blocked/check thất bại. Dữ liệu minh chứng ở `data/returns-v1/`, không đưa vào Git. Chưa chạy trên AWS.

## Mua hàng và nhận nhiều đợt v0.5

Chuỗi liên kết: dòng đơn mua → các lần nhận → các phần kiểm tra → biến động tồn. Ba bảng mới là `purchase_lines`, `purchase_receipts`, `purchase_inspections`; `stock_lineage` dùng chung cho trả hàng và mua hàng, với khóa `return_line_id` hoặc `purchase_line_id` tương ứng.

| Loại sự kiện | Mã và thông tin chính |
| --- | --- |
| purchase_order_line | purchase_order, purchase_line, supplier, SKU, destination, quantity, unit_cost, currency, expected_at |
| purchase_receipt | receipt, purchase_source/scope/purchase_line, SKU, location, quantity |
| purchase_inspection | inspection, receipt_source/scope/receipt, SKU, location, quantity, condition |

Payload dùng `location` cho destination. Mã `purchase_line` phải duy nhất trong nguồn/phạm vi; adapter nguồn có mã dòng chỉ duy nhất trong đơn phải tạo mã kết hợp đơn/dòng trước khi vào contract. Các mã receipt/inspection cũng được xác định theo nguồn/phạm vi.

Dòng đơn mua hỗ trợ revision toàn bản ghi bằng source_version; mã chuẩn ổn định. Có thể sửa quantity, unit_cost và expected_at, nhưng không đổi supplier, SKU, location hoặc currency trong cùng danh tính. `created_at` giữ thời điểm đầu tiên của lịch sử đã nhận; `occurred_at` của dòng hiện tại là thời điểm revision, không dùng để kết luận một lần nhận cũ xảy ra trước đơn mua. Receipt và inspection vẫn append-only.

Theo dõi riêng số đặt, đã nhận, còn chờ giao, nhận vượt, đã kiểm tra, chưa kiểm tra, hàng đạt và hàng lỗi. Hàng lỗi đã nhận vẫn được tính vào số đã giao từ nhà cung cấp; chưa có quy tắc tự đặt bù/thay thế. Nhận vượt và kiểm tra vượt là lỗi chất lượng, không cắt bớt số thực tế để làm khớp.

Stock movement của purchase_receipt phải đi từ supplier tới inspection; purchase_inspection phải đổi từ inspection sang sellable/defective tại địa điểm đã nhận. Chứng từ không cộng tồn lần nữa. Đối chiếu tổng movement của mỗi chứng từ, SKU/địa điểm và thứ tự mua–nhận–kiểm tra. Thiếu chứng từ được giữ chờ và liên kết lại khi dữ liệu đến.

Bộ mẫu đặt 100, nhận 80, kiểm tra 78 đạt và 2 lỗi: outstanding 20, uninspected 0. Simulator lịch sử khởi tạo hàng qua hai lần nhận cho mỗi dòng mua, kiểm tra đạt rồi mới chuyển tới cửa hàng; đặt thêm 20 chiếc chưa giao cho mỗi dòng ban đầu để kiểm chứng tồn chờ. Nếu có replenishment, simulator tạo thêm chứng từ mua/nhận/kiểm tra.

`expected-purchases.json` chứa kết quả nghiệp vụ kỳ vọng của từng dòng mua; pipeline không đọc file này. CLI `--check-history` đối chiếu thêm với `purchase_reconciliation` sau khi xử lý, bên cạnh oracle tiền–tồn.

```powershell
python scripts/dev.py history --output data/purchases/input --days 7 --orders-per-day 50 --skus 50 --daily-snapshots
python scripts/dev.py run --input data/purchases/input --state data/purchases/ledger.sqlite --output data/purchases/output --check-history --parquet
```

Kiểm chứng v0.5: 48 tests đạt. Bộ chạy local bảy ngày/350 đơn/50 SKU có 11.939 sự kiện và dòng snapshot, 100 dòng mua, 200 lần nhận, 200 kiểm tra mua hàng, 2.000 chiếc còn chờ giao và 500 liên kết biến động tồn (mua và trả). Cả oracle tiền–tồn và oracle mua hàng đều khớp, 17 bảng Parquet được xuất, không còn pending/quarantine/blocked/check thất bại. Dữ liệu minh chứng ở `data/purchases-v1/`, không đưa vào Git. Chưa triển khai AWS.

## Kết quả lịch sử v0.2

Đã chạy local cấu hình 30 ngày, 500 đơn/ngày, 500 SKU, seed 42: 15.000 đơn, 207.873 sự kiện trong 5.861 đợt hourly. Tiền theo từng tiền tệ và tồn theo SKU–địa điểm–tình trạng khớp oracle; không còn pending, quarantine, blocked hoặc kiểm tra thất bại. Tám bảng Parquet được xuất thành công. Bộ tests có 18 bài đạt, không bỏ qua bài Parquet khi chạy với dependency đã cài.

Lần chạy minh chứng nằm trong `data/history-v1/` và bị loại khỏi Git. Đây là kiểm chứng local, không phải benchmark hay xác nhận job chạy trên AWS.

Kết quả mẫu: kho 57 áo bán được, 2 lỗi; cửa hàng 20 áo; giữ 0; thu 54 SGD, hoàn 27 SGD. `--check-sample` so oracle riêng với kết quả và từ chối nếu còn vấn đề chất lượng.

## Đọc kết quả

`data/demo/output/latest.json` trỏ tới `runs/<run-id>/result.json`. Result chứa standardized, orders, order_lines, source_links, payments, inventory, pending, quarantine, checks và receptions. `audit.json` có phiên bản xử lý, hash catalog, input batches và thống kê lần tiếp nhận.

Chất lượng `issues` khác `passed`; kết quả có vấn đề vẫn được xuất để kiểm tra, không được coi là dữ liệu đã nghiệm thu. Summary hiện là phép đối chiếu riêng cho fixture SG, không phải bảng tổng hợp toàn brand.

## Chuẩn bị chạy Glue

`python scripts/build_glue.py` tạo `dist/glue_job.py`, chứa package core trong bundle nội bộ. Script không tạo tài nguyên và không gửi dữ liệu lên AWS.

Khi triển khai, dự kiến dùng Glue Python Shell Python 3.9, `MaxCapacity=0.0625`, `MaxConcurrentRuns=1`, retry 0 ban đầu. Đây là cấu hình thử, phải xác nhận quyền lợi tài khoản và đo chi phí. AWS mô tả runtime và capacity tại [Python Shell job properties](https://docs.aws.amazon.com/glue/latest/dg/add-job-python.html).

Tham số job:

```text
--bucket <bucket-thử>
--input-prefix thread/input
--output-prefix thread/output
```

Upload nội dung input dưới `thread/input/`, giữ nguyên đường dẫn tương đối. Đầu ra có `thread/output/latest.json` chứa `state_key`, `result_key` và `audit_key`. Runtime Glue cung cấp boto3; tests mô phỏng S3, không dùng credentials.

Thêm cờ `--parquet` để adapter upload bảng Parquet trước khi công bố. Runtime job phải có pyarrow tương thích; bundle core không chứa thư viện native pyarrow. Cấu hình dependency cho runtime cloud cần xác nhận trước khi chạy thật.

Một writer duy nhất, kể cả caller ngoài Glue. Đường cloud dùng plan từ Lambda/DynamoDB: chốt manifest/hash catalog rồi truyền plan-key và run-token vào Glue. Adapter chỉ tải các manifest của plan, khôi phục checkpoint, replay ledger và công bố run mới. Khi không truyền plan, adapter có chế độ liệt kê manifest cho thử nghiệm trực tiếp. Input được chọn tăng dần; dựng integrated vẫn replay lịch sử trong bộ nhớ.

## Đối soát và chuẩn bị AWS v1.0

Bốn bảng mới: settlements, settlement_lines, payout_transfers, settlement_reconciliation. Đối soát hiện dùng bản chốt toàn vòng đời mỗi đơn: so gross/refunds với các giao dịch thành công, cộng phí và payout về header, so payout với chuyển khoản ngân hàng. Mỗi đơn chỉ có một dòng chốt trong dữ liệu chuẩn; không coi các khoản thực chuyển là doanh thu mới. Phí 5% của simulator chỉ là giả định thử.

Control event event_rejection cho phép operator loại một event hợp lệ nhưng xác nhận sai khỏi phần tích hợp mà vẫn giữ raw và lịch sử rejected=true. Bản mới vẫn được kiểm tra số lượng, tiền và nguồn gốc sau hiệu chỉnh. Đây là luồng quản trị riêng, không nhận chỉ dẫn tự sửa từ dữ liệu bán hàng.

69 tests đã đạt, gồm đăng ký đợt → chốt input → Glue adapter → công bố → hoàn tất bằng AWS boundary mô phỏng, và regression tests cho phục hồi completion. Có kiểm tra guard số bản ghi, catalog theo run và các manifest đến sau không lọt vào input đã chốt. CloudFormation đã lint tại ap-southeast-1, không có findings. Bộ 24 sự kiện đã chạy trên AWS, Glue SUCCEEDED trong 33 giây và workflow SUCCEEDED sau redrive bước completion.

Bộ local bảy ngày/350 đơn/50 SKU mới có 12.539 event/dòng snapshot, mua/bán/trả và snapshot đều đối chiếu đạt; statements và bank payout cũng đạt, không còn pending/quarantine/blocked/check thất bại. Xuất 21 bảng Parquet dưới data/release-v1/. Bộ aws-smoke có 24 event, upload plan 33 file khoảng 16,7 KB, dùng cho lần thử cloud đầu.

Các asset đã build tại dist/: glue_job.py, control.zip, cloudformation.json, release.json. Lịch AWS mặc định tắt, timeout/retry/giới hạn dữ liệu đã đặt cho lần thử nhỏ. Xem DEPLOYMENT.md trước khi tạo change set hoặc upload dữ liệu.

## Giới hạn và công việc tiếp theo

- Simulator bao phủ mua, bán, giao, trả, tồn và đối soát chốt đơn ở hai quốc gia; hủy đơn, FX/thuế thực và mọi nhánh nghiệp vụ chưa được mô phỏng đầy đủ.
- Payload fixture đã biểu diễn nghiệp vụ theo contract chung. Adapter cho schema thực của từng nguồn còn cần xây.
- Các sự kiện stock_move thể hiện sổ hàng chuẩn trong fixture, kể cả chuyển sang đã giao khách. Khi xây adapter nguồn thực, chuyển trạng thái giao nhận phải được suy ra từ sự kiện carrier, không yêu cầu WMS phát sự kiện thay carrier.
- Order/payment/snapshot/dòng mua có cập nhật toàn bản ghi theo phiên bản. Hiệu chỉnh hàng, patch một phần, xóa đối tượng và lịch sử hiệu lực đầy đủ chưa hỗ trợ. Xung đột bị cách ly hoặc làm job thất bại rõ ràng.
- Có đối soát sàn chốt toàn vòng đời: gross, refunds, fees, net payout và bank transfer; không cộng bank payout vào doanh thu lần hai. Delta settlement nhiều kỳ và thanh toán nhà cung cấp chưa hỗ trợ.
- Có JSON/Parquet, Glue Catalog theo run, batch/run ledger và điều phối cloud trong CloudFormation; Iceberg/Spark chưa dùng. Cloud smoke đã xác nhận 16 batch COMPLETED và 21 bảng Catalog.
- Đã chạy một Glue job thật với 0,0625 DPU. Chưa kiểm chứng tải 500 đơn/ngày hoặc quy kết chi phí riêng từ billing toàn tài khoản.

Phần chuẩn bị cloud: build_deploy.py tạo template và assets; lint_infra.py kiểm tra offline; upload_inputs.py mặc định chỉ in kế hoạch. Chi tiết ở DEPLOYMENT.md. Core vẫn replay trong bộ nhớ; capacity mặc định 0.0625 chỉ dành cho bộ mẫu nhỏ. Chưa đo phí/RAM trên Glue.
