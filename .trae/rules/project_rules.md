# Quy trình Thực hiện Yêu cầu Lớn

Để đảm bảo chất lượng, tính minh bạch và sự nhất quán trong quá trình phát triển, chúng ta sẽ tuân theo quy trình 5 bước sau cho mỗi yêu cầu lớn.

### 1. Phân tích & Lên kế hoạch
- **Mục tiêu:** Hiểu rõ yêu cầu và xác định phạm vi ảnh hưởng.
- **Hành động:**
    - Phân tích yêu cầu của người dùng.
    - Xác định các tệp cần tạo mới hoặc chỉnh sửa.
    - Liệt kê các bước logic chính để thực hiện thay đổi.

### 2. Thực hiện thay đổi (Execution)
- **Mục tiêu:** Viết và áp dụng mã nguồn cần thiết.
- **Hành động:**
    - [ ] **Tệp X:** [Mô tả ngắn gọn về thay đổi sẽ được thực hiện trong tệp này].
    - [ ] **Tệp Y:** [Mô tả ngắn gọn về thay đổi sẽ được thực hiện trong tệp này].
    - ...
    - *Các mục này sẽ được coi là hoàn thành sau khi khối lệnh `<changes>` được tạo ra.*

### 3. Kế hoạch Kiểm thử (Test Plan)
- **Mục tiêu:** Đảm bảo tính năng mới hoạt động đúng và không gây ra lỗi hồi quy.
- **Hành động:**
    - Xây dựng một danh sách các trường hợp kiểm thử (test cases) chi tiết.
    - **Ví dụ Test Cases:**
        - **Giao diện người dùng (UI):**
            - [ ] Giao diện hiển thị đúng trên máy tính để bàn và thiết bị di động.
            - [ ] Không có lỗi về bố cục, các thành phần không bị vỡ hoặc lệch.
        - **Chức năng:**
            - [ ] Tương tác của người dùng (nhấn nút, nhập liệu) tạo ra kết quả mong đợi.
            - [ ] Dữ liệu được xử lý, lưu trữ và hiển thị lại một cách chính xác.
        - **Xử lý lỗi:**
            - [ ] Ứng dụng xử lý đầu vào không hợp lệ một cách nhẹ nhàng.
            - [ ] Hiển thị thông báo lỗi rõ ràng khi cần thiết.

### 4. Gỡ lỗi (Debugging)
- **Mục tiêu:** Sửa chữa bất kỳ lỗi nào được phát hiện trong quá trình kiểm thử.
- **Hành động:**
    - Nếu một test case thất bại, tiến hành phân tích nguyên nhân gốc rễ.
    - Đề xuất và thực hiện bản sửa lỗi.
    - Lặp lại chu trình kiểm thử cho đến khi tất cả các test case đều thành công.

### 5. Báo cáo & Hoàn thiện
- **Mục tiêu:** Hoàn tất yêu cầu và cung cấp sản phẩm cuối cùng.
- **Hành động:**
    - Cung cấp một bản tóm tắt các công việc đã hoàn thành.
    - Xác nhận rằng tất cả các mục trong kế hoạch thực hiện và kiểm thử đã được đánh dấu hoàn thành.
    - Cung cấp khối lệnh `<changes>` cuối cùng chứa toàn bộ nội dung của các tệp đã sửa đổi.
