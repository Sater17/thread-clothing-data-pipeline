# Thiết kế pipeline dữ liệu brand thời trang trên AWS v1

Mục tiêu là xây pipeline gom dữ liệu đa kênh, đa quốc gia thành dữ liệu chuẩn, có lịch sử, truy được nguồn gốc và mở rộng được. AWS Glue là công cụ xử lý chính. Đầu ra giai đoạn đầu là dữ liệu cùng bằng chứng chất lượng; dashboard và định hướng khai thác được quyết định sau.

Trạng thái: thiết kế để bắt đầu triển khai. Các cấu hình hạ tầng và cơ chế công bố bảng còn cần kiểm chứng bằng một job mẫu. Tài liệu này không ghi nhận việc đã tạo tài nguyên AWS hoặc viết code.

## Phạm vi nghiệp vụ

Brand giả định THREAD hoạt động tại Việt Nam và Singapore.

| Thành phần | Quy mô ban đầu |
| --- | --- |
| Thị trường | Việt Nam và Singapore |
| Kho | Một kho mỗi nước |
| Cửa hàng | Ba tại VN, hai tại SG; mỗi cửa hàng có tồn riêng |
| Kênh bán | Website theo thị trường, gian hàng sàn tại mỗi nước, POS từng cửa hàng |
| Sản phẩm | Khoảng 50 mẫu và 500 SKU màu–size |
| Đơn hàng | Trung bình 500 đơn/ngày toàn brand |
| Lịch sử | 30 ngày, khoảng 15.000 đơn; mỗi đơn có nhiều sự kiện |
| Hạ tầng | Dự kiến một AWS Region cho môi trường thử |

Thị trường, kênh bán và địa điểm chứa hàng là các chiều độc lập. Website có thể dùng chung một nền tảng. Một cửa hàng vừa bán tại quầy vừa có thể thực hiện đơn online.

Pipeline ghi nhận và hợp nhất quyết định của các hệ thống nghiệp vụ. OMS quyết định phân bổ/giữ hàng; kho xác nhận thực nhập/xuất. Pipeline không trực tiếp điều hành bán hàng hay ngăn bán vượt tồn.

Các luồng cần mô hình hóa đầy đủ: mua và nhận hàng, chuyển hàng, bán online, bán tại quầy, thực hiện đơn, giao nhận, hủy một phần, đổi trả, hoàn tiền, kiểm kê và đối soát.

## Nguồn dữ liệu và trách nhiệm

| Nguồn | Thông tin chịu trách nhiệm |
| --- | --- |
| Danh mục | Product, SKU, thuộc tính, ánh xạ mã nguồn |
| Quản lý giá | Giá theo thị trường/kênh và thời gian hiệu lực |
| Website | Đơn gốc, dòng hàng, giá và giảm giá tại thời điểm đặt |
| Sàn | Đơn gốc trên sàn, cập nhật từ sàn, phí và đối soát |
| POS | Giao dịch bán/đổi trả tại quầy, chứng từ bàn giao |
| OMS | Liên kết đơn nguồn, phân bổ, giữ/giải phóng/sử dụng lượng giữ |
| WMS và tồn cửa hàng | Thực nhập/xuất, chuyển, kiểm kê, tình trạng hàng |
| Thanh toán | Từng lần thử thu tiền, hoàn tiền và kết quả |
| Vận chuyển | Vận đơn và lịch sử giao nhận |
| Mua hàng | Đơn mua, số đặt, lịch giao dự kiến |

Nguồn quyết định được chọn theo từng thông tin. Khoản do sàn quản lý cần dùng dữ liệu giao dịch/đối soát tương ứng của sàn. Không dùng một nguồn ưu tiên cho toàn bộ đơn hàng.

Ví dụ OMS yêu cầu xuất hai áo nhưng WMS xác nhận một: giữ cả yêu cầu và thực tế, ghi nhận phần thiếu. Website báo đã thanh toán nhưng nguồn thanh toán chưa xác nhận: giữ hai quan sát, chưa kết luận thu tiền thành công.

Giá hiện tại không thay thế giá của giao dịch cũ. Khách hàng được giữ theo định danh nguồn; chưa tự hợp nhất chỉ vì giống tên/email.

## Nhịp gửi dữ liệu và chạy Glue

Đây là nhịp mô phỏng đã thống nhất để giảm số lần chạy trên AWS. Khi kết nối hệ thống thật, nhịp thu thập được điều chỉnh theo khả năng và giới hạn của nguồn.

| Dữ liệu | Nhịp gửi v1 |
| --- | --- |
| Sự kiện đơn, tiền, phân bổ, tồn, giao nhận | Gom hourly theo nguồn |
| Thay đổi danh mục và mapping | Gửi trong đợt hourly; daily snapshot |
| Snapshot tồn | Daily theo mốc thời gian địa phương |
| Đối soát | Daily hoặc khi nguồn phát hành |
| Backfill | Đợt riêng, khai báo phạm vi thời gian |

Sự kiện giữ thời điểm nghiệp vụ dù gửi sau. POS mất mạng giữ dữ liệu rồi gửi bù. Dữ liệu trễ được xử lý theo ngày xảy ra, không đổi sang ngày tiếp nhận.

- Đợt hourly bao phủ khoảng [đầu giờ, đầu giờ kế tiếp), ghi rõ múi giờ. Ví dụ đợt 10:00–11:00 dự kiến đến trước 11:10 trong bộ thử.
- Không có giao dịch: gửi manifest số bản ghi bằng 0; không cần file dữ liệu rỗng.
- Snapshot cuối ngày ghi rõ mốc 00:00 ngày kế tiếp và trạng thái mà nó đại diện.
- Glue dự kiến chạy lúc 02:00 giờ VN, tương đương 03:00 SG, hoặc được gọi chủ động trong phiên thử.
- Lần chạy chọn mọi đợt sẵn sàng chưa xử lý, gồm đợt cũ đến muộn; không chỉ chọn file của hôm nay.
- Lịch sử 30 ngày được tải và xử lý bằng một hoặc vài đợt lớn, không bắt buộc 30 lần chạy.

Kiến trúc tiếp nhận theo sự kiện, xử lý theo batch. File mới đăng ký công việc; không khởi động Glue cho từng file. Tần suất có thể tăng sau khi đo chi phí và xác định nhu cầu độ trễ.

## Bộ mô phỏng nguồn

Simulator chạy trên máy người dùng, dùng chung mô hình nghiệp vụ cho ba chế độ:

1. Kịch bản cố định: ít giao dịch, biết chính xác kết quả.
2. Lịch sử 30 ngày: đầy đủ nguồn, quan hệ và các nhánh nghiệp vụ.
3. Phiên liên tục: tiếp tục từ trạng thái cuối lịch sử, chạy trong thời gian giới hạn.

Simulator tách ba lớp: hoạt động thực tế; biểu diễn bằng mã/schema tại từng nguồn; truyền dữ liệu. Gửi trùng, trễ, thiếu hoặc sai thứ tự tác động vào lớp truyền, không thay đổi kết quả nghiệp vụ kỳ vọng.

Mỗi lần chạy giữ cấu hình, mốc bắt đầu và seed để tái tạo bộ giao dịch. Kết quả chuẩn được giữ riêng làm dữ liệu nghiệm thu, không cung cấp cho pipeline như input nghiệp vụ. Bộ dữ liệu phải có quan hệ nhân quả: có hàng mới xuất, giao hàng rồi mới trả trong kịch bản tương ứng.

## Kiến trúc AWS dự kiến

Luồng: simulator → S3 raw → thông báo → SQS → Lambda kiểm tra/đăng ký đợt → sổ đợt → điều phối theo lịch/chủ động → Glue → standardized → integrated → kiểm tra và công bố.

| Thành phần | Trách nhiệm |
| --- | --- |
| S3 | Raw, manifest, dữ liệu chuẩn, dữ liệu tích hợp, quarantine và audit |
| SQS | Giữ thông báo/công việc tiếp nhận |
| Lambda | Kiểm tra manifest, đăng ký đợt, xử lý thông báo lặp |
| DynamoDB | Sổ đợt, trạng thái xử lý, quyền nhận đợt |
| Step Functions | Dự kiến điều phối Glue và theo dõi kết quả |
| Bộ kích hoạt theo lịch | Gọi bộ điều phối daily; lựa chọn dịch vụ chốt khi triển khai |
| Glue | Chuẩn hóa, tích hợp, dựng trạng thái và kiểm tra |
| Glue Data Catalog | Metadata bảng, tách biệt với chi phí chạy Glue ETL |
| Athena | Truy vấn kiểm chứng khi cần |

API Gateway và Firehose không thuộc đường tiếp nhận cần thiết của v1 gửi file. Có thể bổ sung khi chuyển sang nguồn phát sự kiện trực tiếp hoặc tăng lưu lượng.

Parquet là định dạng dự kiến cho dữ liệu chuẩn. Iceberg là lựa chọn dự kiến cho bảng cần cập nhật, phải thử cơ chế ghi/đọc và phục hồi trước khi chốt. Không sửa trực tiếp các file dữ liệu bên dưới bảng Iceberg.

## Các lớp dữ liệu và contract

| Lớp | Nội dung |
| --- | --- |
| Raw | Payload/file nguyên bản cùng metadata tiếp nhận |
| Standardized | Chuẩn kiểu, thời gian, tiền tệ, định danh và cấu trúc; giữ góc nhìn nguồn |
| Integrated | Quan hệ xuyên nguồn, lịch sử nghiệp vụ và trạng thái hiện tại |

Contract của mỗi đối tượng định nghĩa mức chi tiết, khóa, trường bắt buộc, đơn vị, quy tắc tiền/số lượng, phiên bản, cách cập nhật và xử lý lỗi. Không ép mọi nguồn về bộ trường tối thiểu rồi làm mất thông tin riêng.

Metadata tiếp nhận gồm nguồn/phạm vi nguồn, loại dữ liệu/schema, mã sự kiện hoặc bản ghi, thời điểm nghiệp vụ, thời điểm cập nhật nguồn, thời điểm nhận, phiên bản/thứ tự nguồn nếu có, batch ID và vị trí raw. Bản ghi chuẩn thêm phiên bản quy tắc và lần xử lý.

Tiền dùng kiểu thập phân và luôn kèm tiền tệ. Thiếu thông tin khác với giá trị 0. Thời gian chuẩn hóa về UTC nhưng giữ thời điểm/múi giờ gốc và ngày nghiệp vụ. Không cộng tiền khác tiền tệ khi chưa có quy tắc quy đổi. Khử trùng và thứ tự cập nhật được định nghĩa riêng cho từng nguồn.

### Danh mục và ánh xạ

| Đối tượng | Trường cốt lõi |
| --- | --- |
| Product | product_id, product_code, product_name, category_code |
| SKU | sku_id, sku_code, product_id, color_code, size_code, unit_of_measure, status |
| Location | location_id, location_code, location_name, location_type, country_code, timezone, status |
| Mapping | mapping_id, source_system, source_scope, entity_type, source_entity_id, canonical_entity_id, valid_from, valid_to |

Khóa chuẩn ổn định khi tên hoặc mã hiển thị đổi. SKU ngừng bán vẫn được giữ cho lịch sử. Mapping chọn theo thời điểm nghiệp vụ, không được có khoảng hiệu lực chồng nhau cho cùng mã và phạm vi nguồn. Nhiều mã nguồn có thể trỏ về cùng mã chuẩn.

### Đơn hàng và các đối tượng liên quan

| Đối tượng | Mức chi tiết và quan hệ |
| --- | --- |
| Order | Một đơn chuẩn, có liên kết tới các mã đơn nguồn |
| Order Line | Một dòng đặt hàng: SKU, số lượng, đơn giá, giảm giá, thuế, tổng tiền |
| Payment Transaction | Một giao dịch thu/hoàn, giữ các phiên bản trạng thái; mỗi lần thử có mã riêng |
| Payment Allocation | Phân bổ giao dịch tiền cho dòng đơn/dòng trả khi có dữ liệu |
| Fulfillment Line | Một phần dòng đơn thực hiện từ một địa điểm |
| Goods Issue Line | Một lần thực xuất hàng, liên kết phân bổ/dòng đơn |
| Package Line | Một dòng trong kiện, liên kết hàng xuất |
| Shipment | Vận đơn và các sự kiện trạng thái |
| Return Line | Một dòng yêu cầu trả |
| Return Receipt Line | Một lần thực nhận hàng trả |
| Return Inspection | Kết quả kiểm tra một phần số lượng nhận |
| Purchase Order Line | Một dòng đặt mua, liên kết các lần nhận từ kho |

Một dòng đơn có thể giao nhiều lần, từ nhiều nơi, trả và hoàn nhiều lần. Website và OMS có mã khác nhưng tham chiếu rõ về nhau chỉ tạo một đơn chuẩn. Thiếu liên kết được giữ ở trạng thái chờ, không đoán từ tên/số tiền.

Kiểm tra xuyên đối tượng gồm công thức tổng tiền theo nguồn; xuất so với lượng đặt hợp lệ; nhận trả so với lượng giao; hoàn so với thu thành công; kiểm tra hàng so với lượng nhận. Kết quả có ba mức: đạt, không đạt, chờ đủ dữ liệu.

### Tồn và lượng giữ

Một biến động hàng ghi SKU, số lượng dương, địa điểm/tình trạng đi và đến, loại biến động, thời điểm, chứng từ liên quan và quan hệ hiệu chỉnh. Các điểm ngoài phạm vi như nhà cung cấp hoặc khách hàng được biểu diễn rõ. Chuyển hàng dùng trạng thái/vị trí đang chuyển gắn với phiếu chuyển.

Giữ hàng lưu riêng: mã giữ, dòng đơn/phân bổ, SKU, địa điểm, số lượng và hành động giữ/giải phóng/sử dụng. Giữ không làm hàng rời kho.

- Tồn tại T = tồn đầu kỳ + lượng vào − lượng ra đến T.
- Có thể bán = hàng bán được đang có − lượng đang giữ, theo chính sách v1.
- Đổi địa điểm/tình trạng không tạo thêm hàng.
- Xuất bán giảm kho và đưa hàng sang đang giao; giao thành công không giảm kho lần nữa.
- Hàng trả vào chờ kiểm tra; chỉ chuyển sang bán được sau kiểm tra đạt.
- Snapshot đối chiếu tại cùng thời điểm/phạm vi; không cộng vào biến động và không tự ghi đè tồn.
- Chênh lệch kiểm kê chỉ tác động tồn khi có điều chỉnh nghiệp vụ được xác nhận.

## Quản lý đợt và phục hồi

Manifest ghi batch ID, nguồn, loại dữ liệu, khoảng thời gian/múi giờ, danh sách file, số bản ghi, checksum, schema và quan hệ thay thế nếu có. Khai báo rõ full snapshot hay dữ liệu thay đổi; quy định ý nghĩa bản ghi vắng mặt và dòng số lượng 0.

Thiếu file/checksum sai/cấu trúc không đọc được chặn đợt. Lỗi dòng được cách ly; cho phép xử lý phần hợp lệ chỉ khi contract cho phép. SKU chưa mapping hoặc đối tượng chưa đến được giữ chờ. Đợt không có giao dịch vẫn có xác nhận hoàn tất.

Gửi lại cùng đợt/nội dung không tạo tác động mới. Cùng mã nhưng khác nội dung không khai báo sửa là xung đột. Sửa hợp lệ có phiên bản hoặc tham chiếu bản gốc. Tránh trùng cả cấp đợt lẫn cấp bản ghi.

Một lần Glue chạy:

1. Nhận quyền xử lý các đợt sẵn sàng và cố định danh sách input.
2. Chuẩn hóa, kiểm tra contract, tách hợp lệ/lỗi/chờ.
3. Khử trùng, liên kết và cập nhật lịch sử.
4. Dựng lại trạng thái trong phạm vi bị ảnh hưởng.
5. Kiểm tra kết quả, lưu audit và công bố nếu đạt điều kiện.

Bản đầu dự kiến một Glue job với các bước logic, một luồng ghi integrated tại một thời điểm. Quyền xử lý có mã lần chạy và thời hạn để phục hồi. Các đợt mới đến trong khi chạy chờ lần sau.

Nếu lỗi sau ghi lịch sử nhưng trước tính tồn, chạy lại dùng cùng khóa ổn định và dựng lại trạng thái; không cộng tác động lần hai. Phục hồi dùng cùng phiên bản quy tắc; tái xử lý với quy tắc mới tạo kết quả phiên bản mới.

Trạng thái xử lý, chất lượng và công bố tách riêng. Ghi thành công một bảng không đồng nghĩa mọi bảng cập nhật đồng thời. Mốc công bố phải trỏ tới phiên bản bảng cụ thể; cơ chế thực hiện còn cần chốt và thử.

## Tổ chức S3 và bảng

| Vùng | Nội dung |
| --- | --- |
| raw/ | File nguồn bất biến |
| manifests/ | Mô tả các đợt |
| standardized/ | Bảng chuẩn hóa |
| integrated/ | Bảng tích hợp |
| quarantine/ | Bản ghi có vấn đề, lý do và tham chiếu raw |
| audit/ | Input lần chạy, phiên bản, kiểm tra và mốc công bố |

Raw phân theo nguồn/ngày tiếp nhận/đợt. File chứa sự kiện cũ vẫn nằm theo ngày nhận. Mỗi lần tiếp nhận giữ vị trí riêng; không ghi đè raw.

Glue đọc danh sách input được chọn từ sổ đợt. Bảng đầu ra phân vùng vừa đủ theo đặc điểm dữ liệu; không chia thành thư mục cho từng đơn/SKU/cửa hàng. Bảng nhỏ có thể chưa cần phân vùng. Metadata cho biết số dòng hợp lệ, trùng, cách ly và chờ; không để dữ liệu lỗi biến mất khỏi thống kê.

## Bộ mẫu nhập chuyển bán trả

Mã chuẩn dưới đây là mã minh họa. Cơ chế cấp mã khi triển khai phải ổn định qua các lần chạy lại.

### Danh mục mẫu

Product PRD-0001 có mã TS001, tên Essential T-Shirt. SKU-0001 có mã TS001-BLK-M, màu BLK, size M, đơn vị piece.

| Location ID | Mã | Loại | Quốc gia | Múi giờ |
| --- | --- | --- | --- | --- |
| LOC-0001 | VN-WH-01 | Kho | VN | Asia/Ho_Chi_Minh |
| LOC-0002 | VN-ST-01 | Cửa hàng | VN | Asia/Ho_Chi_Minh |
| LOC-0003 | VN-ST-02 | Cửa hàng | VN | Asia/Ho_Chi_Minh |
| LOC-0004 | VN-ST-03 | Cửa hàng | VN | Asia/Ho_Chi_Minh |
| LOC-0005 | SG-WH-01 | Kho | SG | Asia/Singapore |
| LOC-0006 | SG-ST-01 | Cửa hàng | SG | Asia/Singapore |
| LOC-0007 | SG-ST-02 | Cửa hàng | SG | Asia/Singapore |

| Nguồn và phạm vi | Mã SKU nguồn | SKU chuẩn |
| --- | --- | --- |
| website / sg_web | WEB-TEE-BLACK-M | SKU-0001 |
| marketplace / sg_shop_01 | 88901 | SKU-0001 |
| wms / sg_inventory | TS001-BLK-M | SKU-0001 |
| pos / sg_store_01 | A1042 | SKU-0001 |

### Chuỗi chứng từ

Tồn đầu kỳ bằng 0. Các ngày dưới đây thuộc tháng 09/2026 và thời gian sự kiện theo Singapore.

| Ngày | Nguồn và hoạt động | Chứng từ và liên kết |
| --- | --- | --- |
| 01 | Mua 100 áo; kho nhận 80, kiểm tra 78 đạt và 2 lỗi | PO-001 / POL-001; RCV-001 tham chiếu dòng mua |
| 02 | Kho xuất chuyển 20; cửa hàng nhận đủ | TRF-001; TI-001 xuất, TR-001 nhận |
| 03 10:15 | Website đặt hai áo | WEB-SG-1001 / WL-001 |
| 03 10:16 | OMS nhận đơn | OMS-9001 / OL-9001 tham chiếu đơn/dòng web |
| 03 10:17 | Thu thành công 54 SGD | PAY-001 tham chiếu đơn web |
| 03 10:18 | OMS phân bổ và giữ hai áo ở kho SG | AL-001 / RS-001 |
| 03 14:00 | WMS xuất hai áo; OMS xác nhận sử dụng lượng giữ | GI-001 / GIL-001; liên kết AL-001 và RS-001 |
| 03 15:00 | Vận chuyển nhận kiện | PKG-001 / PKGL-001; SHIP-001 |
| 04 16:30 | Giao đủ hai áo | Sự kiện giao thành công của SHIP-001 |
| 06 | Yêu cầu trả một áo | RET-001 / RL-001 tham chiếu dòng đơn |
| 07 | Kho nhận trả, kiểm tra đạt, đưa lại vào hàng bán được | RR-001; QC-001; biến động chuyển tình trạng |
| 08 | Hoàn thành công 27 SGD | RF-001 tham chiếu PAY-001 và dòng trả |

Ngày nhận/kiểm tra trả và hoàn tiền đã chốt; giờ cụ thể sẽ được đặt khi tạo fixture, bảo đảm thứ tự trong kịch bản bình thường.

Website gửi quantity_ordered = 2, unit_price = 30.00, line_discount_amount = 6.00, line_tax_amount = 0.00, line_total_amount = 54.00, currency = SGD, source_version = 1. Bộ mẫu xác nhận thuế và phí giao hàng bằng 0, không có giảm giá cấp đơn.

Đơn chuẩn ORD-0001 và dòng ORDL-0001 giữ liên kết cả mã web và OMS. SKU chuẩn là SKU-0001. Thời điểm tạo UTC là 2026-09-03T02:15:00Z; ngày nghiệp vụ SG là 2026-09-03. Giao dịch thu chuẩn TXN-0001 là PAY-001 ở trạng thái succeeded.

Kho có 58 áo bán được trước khi giữ; sau giữ vẫn có 58, giữ 2, có thể bán 56. Sau xuất và sử dụng lượng giữ: kho 56, giữ 0, đang giao 2. Giao thành công đưa hai áo sang đã giao khách, không giảm kho lần nữa.

Nhận trả đưa một áo vào chờ kiểm tra; kiểm tra đạt chuyển sang bán được. Khoản hoàn 27 SGD là giá trị nguồn cung cấp theo phân bổ giảm giá của ví dụ, không suy diễn từ số lượng trả cho mọi trường hợp.

| Kết quả cuối | Kỳ vọng |
| --- | --- |
| Kho SG bán được | 57 áo |
| Kho SG hàng lỗi | 2 áo |
| Chờ kiểm tra | 0 |
| Cửa hàng SG bán được | 20 áo |
| Đang chuyển / đang giao / đang giữ | 0 |
| Khách còn giữ | 1 áo |
| Thu thành công | 54 SGD |
| Hoàn thành công | 27 SGD |
| Thu trừ hoàn | 27 SGD, chưa gọi là doanh thu kế toán |
| Đơn mua còn chưa giao | 20 áo |

Kiểm tra hàng: 80 đã nhận = 57 ở kho + 2 lỗi + 20 ở cửa hàng + 1 khách giữ. Snapshot cuối kỳ phải khớp các trạng thái theo cùng mốc thời gian.

## Bộ nghiệm thu

| Tình huống | Kết quả cần đạt |
| --- | --- |
| Luồng bình thường | Tiền, hàng, quan hệ khớp bộ mẫu |
| Trùng sự kiện giữa các file / gửi lại đợt | Một tác động nghiệp vụ, giữ các lần tiếp nhận |
| Sai thứ tự | Trạng thái cuối đúng khi đủ dữ liệu; bản cũ không ghi đè bản mới |
| Dữ liệu đến muộn | Đúng thời điểm nghiệp vụ, tính lại phần ảnh hưởng |
| Thiếu file | Đợt chờ, không tuyên bố đầy đủ |
| Đợt 0 bản ghi | Phân biệt không phát sinh và chưa gửi |
| Thiếu/sửa mapping | Chờ hoặc tái xử lý đúng khoảng hiệu lực |
| Cùng mã nhưng khác payload | Phát hiện xung đột |
| Lỗi dòng | Cách ly, giải thích đủ số bản ghi |
| Job lỗi giữa các bước | Retry không mất/trùng; kết quả bằng chạy thành công từ đầu |
| File sửa hợp lệ | Giữ lịch sử và áp dụng đúng bản sửa |
| Snapshot lệch | Ghi chênh lệch, không tự sửa tồn |
| Tách kiện / hủy / trả / hoàn một phần | Theo dõi đúng ở cấp dòng và lần thực hiện |

Biến thể đầu tiên: gửi trùng GI-001; RR-001 xảy ra ngày 07 nhưng đến ngày 09 sau hoàn ngày 08; đợt nhận chuyển thiếu một file. Giao dịch POS mất mạng được thử bằng kịch bản POS riêng.

Kiểm tra ở cấp bản ghi, đợt và nghiệp vụ xuyên nguồn. Theo dõi nguồn đã đầy đủ đến khoảng nào, không chỉ file mới nhất. Phát hiện thiếu là kết quả đúng; chỉ phục hồi số liệu đầy đủ khi nhận dữ liệu bổ sung.

## Chi phí và thứ tự triển khai

Giữ đầy đủ nghiệp vụ, giảm số lần chạy cloud. Simulator ở máy người dùng, gom file hourly, Glue daily/chủ động, không bật chạy liên tục mặc định. Chọn input có phạm vi, tránh quét lại toàn bộ raw. Gom file giảm overhead nhưng không bảo đảm Glue miễn phí.

Trước khi chạy bộ lớn cần xác định plan, credits và quyền lợi thực tế của tài khoản; đo job mẫu theo region, loại worker, số worker, thời gian chạy và các dịch vụ phụ trợ. Không cam kết chi phí 0 khi chưa có số đo.

Tham khảo chính thức: [AWS Free Tier](https://aws.amazon.com/free/), [Glue Pricing](https://aws.amazon.com/glue/pricing/), [Athena Pricing](https://aws.amazon.com/athena/pricing/). Giá và điều kiện phải được kiểm tra lại lúc triển khai.

Thứ tự công việc:

1. Hoàn thiện contract và fixture của luồng mẫu, gồm cả manifest và kết quả kỳ vọng.
2. Xây simulator tái tạo được, sau đó mở rộng đủ nguồn và lịch sử.
3. Xây tiếp nhận raw, sổ đợt và kiểm tra độ đầy đủ.
4. Xây Glue chuẩn hóa và tích hợp cho luồng mẫu xuyên suốt.
5. Hoàn thiện điều phối, retry, công bố phiên bản và kiểm tra lỗi.
6. Đo chi phí, chạy lịch sử 30 ngày và bộ nghiệm thu đầy đủ.

Mốc đầu tiên là luồng mẫu qua S3 → Glue → standardized → integrated có kết quả đúng bằng kiểm tra thủ công. Đây là thứ tự xây dựng; không giảm phạm vi mô hình đa nguồn đã chốt.

## Các điểm cần chốt khi bắt đầu triển khai

- Tên trường/schema đầy đủ cho từng nguồn và đối tượng; mã/thứ tự sự kiện giữ hàng.
- Cách cấp khóa chuẩn ổn định và quản lý hiệu chỉnh/xóa tại nguồn.
- Glue runtime, worker, quy tắc ghi Parquet/Iceberg và bảo trì file.
- Cơ chế công bố phiên bản nhất quán giữa các bảng và đọc lại phiên bản cũ.
- Lịch/timeout đợt gửi, thời gian chờ dữ liệu và cách cảnh báo.
- Region, quyền lợi tài khoản, giới hạn chi phí thử, IAM, thời gian giữ dữ liệu/log và dọn tài nguyên.

Các điểm trên là chi tiết triển khai còn mở. Khung nghiệp vụ, nhịp gửi/chạy, các lớp dữ liệu và tiêu chí đúng đã thống nhất.
