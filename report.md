### Báo cáo Lab8  
**Case-Study 1: cào dữ liệu từ web: https://www.google.com/finance/quote. Sau đó trực quan hóa dữ liệu đó bằng thư viện matplotlib.**
  - Hình ảnh minh chứng pipeline: https://drive.google.com/file/d/1L0zyotP7SDwJOIrI9s5DNOFpqv6yBUBL/view?usp=drive_link
**Case-Study 2: cào dữ liệu từ file pima_lab8.csv. Sau đó huấn luyện mô hình LogisticRegression và đánh giá mô hình**
  - Hình ảnh minh chứng pipeline: https://drive.google.com/file/d/16CLvmhQjL2LNk6Jjt1hhjOkDjcKnlsya/view?usp=drive_link
- Tự động hóa pipeline bằng airflow: https://drive.google.com/file/d/1HhQr5t6a5nERmGbyvJ-Od-bTOf6k4W8C/view?usp=drive_link

### Báo cáo Lab9
**Exercise-1:**
1. Tạo thư mục downloads nếu nó không tồn tại
2. Tải xuống từng tập tin một.
3. Tách tên tệp ra khỏi uri để tệp giữ nguyên tên tệp gốc.
4. Mỗi tập tin là một zip, trích xuất csvtừ zip ​​và xóa ziptập tin.
   - Hình ảnh minh chứng cho 1,2,3,4: https://drive.google.com/file/d/1q1W367nPJ6eJRBTZt8uTZhTwWZ7g4zEb/view?usp=drive_link
   - HÌnh ảnh minh chứng download về thư mục downloads: https://drive.google.com/file/d/1j-Sh5qrVr6S8RFmUD-zpbAgjtRqroim1/view?usp=drive_link
**Exercise-2:**
1. Cố gắng thu thập/kéo xuống nội dung củahttps://www.ncei.noaa.gov/data/local-climatological-data/access/2021/
2. Phân tích cấu trúc của nó, xác định cách tìm tệp tương ứng 2024-01-19 10:27	bằng Python.
  - Hình ảnh minh chứng pipeline cào web phân tích cấu trúc: https://drive.google.com/file/d/1AG596iWfkSJJg-I0Gzvzfm_NeO4VZCi0/view?usp=drive_link
3. Xây dựng các URL yêu cầu để tải xuống tệp này và ghi tệp cục bộ.
  - https://drive.google.com/file/d/1Qp2QI52Ww-54EYrRENRjZaSBksull_rv/view?usp=drive_link
4. Mở tệp bằng Pandas và tìm các bản ghi có cao nhất HourlyDryBulbTemperature.
  - Hình ảnh minh chứng: https://drive.google.com/file/d/1Dz7q6KLvmz__zHN_nNLgIuMj9sHXB9TE/view?usp=drive_link
5. In nội dung này ra stdout/dòng lệnh/terminal.
  - https://drive.google.com/file/d/1EtNYDi87iz5q-DkO9INBqA1Q7yMhx4xO/view?usp=drive_link

**Exercise-3:**
1. requests tải xuống tệp từ s3 nằm ở bucket commoncrawlvà keycrawl-data/CC-MAIN-2022-05/wet.paths.gz
2. Giải nén và mở tệp này bằng Python (gợi ý, đây chỉ là văn bản). Kéo uri từ dòng đầu tiên của tập tin này.
3. Một lần nữa, hãy tải lại tệp đó uribằng cách s3sử dụng requests lại.
4. In từng dòng, lặp lại cho đến stdout/dòng lệnh/thiết bị đầu cuối.
kết quả đạt được: https://drive.google.com/drive/folders/1juT05s9NQTzy752SmLKYPob95MjC3bb8?usp=drive_link
**Exercise-4:**
1.Thu thập data thư mục Python và xác định tất cả jsoncác tập tin.
2.Tải tất cả json các tập tin.
3.Làm phẳng json cấu trúc dữ liệu.
https://drive.google.com/drive/folders/1KBuZ0EabE8qA1gFdWWB9muAR9-ww4GlF?usp=drive_link
4.Ghi kết quả vào một csv tệp, một đối một với tệp json, bao gồm tên tiêu đề.
Kết quả thu được là một cây thư mục mới với các file csv được chuyển thành từ các file JSON tương ứng và ghi bên cạnh các file đó
https://drive.google.com/file/d/1XhgYRStaoDGdQHL7U5txaq32JmWUNbAE/view?usp=drive_link
**Exercise-5:**
1.Kiểm tra từng csv tệp trong data thư mục. Thiết kế CREATE câu lệnh cho từng tệp.
2.Đảm bảo bạn có chỉ mục, khóa chính và khóa giả.
3.Sử dụng psycopg2 để kết nối với Postgresbật localhostvà mặc định port.
4.Tạo các bảng dựa trên cơ sở dữ liệu.
5.Nhập csv các tập tin vào các bảng bạn đã tạo bằng cách sử dụng psycopg2.
Chúng tôi đã hoàn thành các yêu cầu trên với đoạn code sau:
https://drive.google.com/file/d/1YrPzZGqjvLfHhEMavY7EQbtZz9yVBxgF/view?usp=drive_link
Sau khi chạy container và đã hoàn tất việc chèn dữ liệu thì chúng tôi sử dụng pgAdmin4 để kiểm tra và thấy toàn bộ dữ liệu từ bảng data đã được chèn vào trong database.
https://drive.google.com/drive/folders/1v016JqH1vce--NaXALi2HaSg8UHZ_gUT?usp=drive_link
