# Image downloader

Tải xuống hình ảnh thumbnail trong Wordpress và upload vào S3/S3-compatible storage

## Các dạng bài sẽ được tải ảnh

- Attachtment có mimeype dạng `image/*` trong các `post` và `product`
- Link ảnh chứa trong `postmeta`: `_external_images`

## Tính năng

- Checkpoint để có thể chạy recover giữa chừng

## HDSD

### Yêu cầu môi trường:
- UNIX/Linux (đã test trên macOS Sonoma 14.5)
- Python 3.x (đã test trên Python 3.9)
- MySQL/MariaDB (đã test trên MariaDB 10.5.24)

### Chuẩn bị môi trường

1. Copy file `config.template.ini` ra thành `config.ini`:

```sh
cp config.template.ini config.ini
```

2. Điền các thông tin còn thiếu vào `config.ini`, ví dụ như dưới đây:

```ini
[mysql]
host=127.0.0.1
port=3306
user=some_user
password=some_password
db_name=some_db
table_prefix=some_db

[s3]
bucket_name=some-bucket

endpoint_url=https://some-endpoint.com ; để trống nếu dùng AWS S3, điền endpoint nếu dùng dịch vụ S3-compatible
access_key_id=some-access-key
secret_access_key=some-secret-key

cdn_url=https://image-cdn.example.com ; CDN prefix để tạo ra URL cuối cùng
```

3. Tạo virtual environment và cài các thư viện Python cần thiết
```sh
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Chạy index

0. Active virtual environment (nếu như đã active ở bước 3 ở trên thì bỏ qua)

```sh
source venv/bin/activate
```

1. Nếu muốn chạy lại từ đầu, xoá file `checkpoint.txt`.

2. Copy file `ids.txt` vào cùng chỗ với `downloader.py`, file `ids.txt` có định dạng mỗi post id một dòng:

```
12345
12346
12347
12348
12349
...
```

3. Chạy lệnh index:
```sh
python downloader.py
```


## Lưu ý

1. Checkpoint chỉ hoạt động đúng nếu file `ids.txt` không thay đổi.
2. Trong trường hợp chết giữa chừng, chỉ cần chạy lại lệnh như mục 3 ở mục trên.
3. Nếu cần tải lại hết ảnh từ đầu, chạy lệnh sau:
```sh
python downloader.py --all
```

## Giới hạn
- Hiện tại chỉ detect được ảnh gắn trong bài dưới dạng attachment, có mimetype dạng `image/*`. Ngoài ra chưa tìm được những chỗ khác để tải thêm ảnh
- Chưa detect nội dung tải về có phải ảnh thật không để báo lỗi