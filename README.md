# Typesense Wordpress Indexer

Index bài viết trong Wordpress vào Typesense một cách nhanh chóng mà không phải index qua plugin [Search with Typesense](https://wordpress.org/plugins/search-with-typesense/) rất chậm chạp mà không có checkpoint để recover giữa chừng.

## Các nội dung được index:

- Thông tin basic của post (như ID, title, ngày tháng tạo, tác giả, etc)
- Permalink
- Tag (kèm link)
- Category (kèm link)

## Tính năng

- Nhanh hơn index bằng plugin mặc định của Typesense (2 ngày -> 20 phút cho ~350k post)
- Checkpoint để có thể chạy recover giữa chừng

## HDSD

### Yêu cầu môi trường:
- UNIX/Linux (đã test trên macOS Sonoma 14.5)
- Python 3.x (đã test trên Python 3.9)
- MySQL/MariaDB (đã test trên MariaDB 10.5.24)
- Typesense (đã test trên Typesense v26.0)

### Chuẩn bị môi trường

1. Copy file `config.template.ini` ra thành `config.ini`:

```sh
cp config.template.ini config.ini
```

2. Điền các thông tin còn thiếu vào `config.ini`, ví dụ như dưới đây:

```ini
[wordpress]
host=https://example.com

[mysql]
host=mysql.example.com
port=3306

user=admin
password=some_password

db_name=dbname

[typesense]
host=typesense.example.com
port=443
protocol=https
api_key=some_api_key ; điền api key có thể index được document
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

2. Copy file `ids.txt` vào cùng chỗ với `indexer.py`, file `ids.txt` có định dạng mỗi post id một dòng:

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
python indexer.py
```

## Lưu ý

1. Checkpoint chỉ hoạt động đúng nếu file `ids.txt` không thay đổi.
2. Trong trường hợp chết giữa chừng, chỉ cần chạy lại lệnh như mục 3 ở mục trên.

## Giới hạn
Code này chỉ blackbox reverse engineering kết quả đầu ra và database đầu vào, kèm theo code có sẵn của Wordpress và plugin [Search with Typesense](https://wordpress.org/plugins/search-with-typesense/) mà không biết config của server và các nội dung được index thêm của các plugin khác, nên kết quả thực tế khi index qua plugin [Search with Typesense](https://wordpress.org/plugins/search-with-typesense/) có thể sẽ khác so với kết quả của code này.

## TODO
- Better CLI
- Create permalink base on `wp_options.option_name='permalink_structure'`. Currently permalink is hard-coded to structure which is equivalent to `%category%/%postname%/%author`