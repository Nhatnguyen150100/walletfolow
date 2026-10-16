package com.walletflow.auth.dto;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record RegisterRequest(
    @NotBlank(message = "Email không được để trống")
    @Email(message = "Email không hợp lệ")
    String email,

    // 12 ký tự thay vì 6: với một ứng dụng giữ tiền, 6 ký tự nằm trong tầm dò của một máy tính
    // thông thường trong thời gian ngắn. Độ dài là yếu tố ảnh hưởng mạnh nhất tới chi phí dò, mạnh
    // hơn nhiều so với yêu cầu "phải có ký tự đặc biệt" (vốn chủ yếu đẩy người dùng tới những biến
    // thể dễ đoán như "Password1!").
    @NotBlank(message = "Mật khẩu không được để trống")
    @Size(min = 12, max = 72, message = "Mật khẩu phải từ 12 đến 72 ký tự")
    String password,

    @NotBlank(message = "Họ tên không được để trống")
    String fullName) {
}
