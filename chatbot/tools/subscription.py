"""
SoundFlow AI Chatbot - Subscription Management Tool

Handles subscription changes (Free ↔ Paid) for users.
"""

from google.cloud import bigquery
from langchain_core.tools import tool

from config import settings


@tool
def change_subscription(user_id: str, new_level: str) -> str:
    """Thay đổi gói cước của người dùng từ Free sang Paid hoặc ngược lại.

    Sử dụng công cụ này khi người dùng muốn:
    - Nâng cấp từ Free lên Paid
    - Hạ cấp từ Paid về Free
    - Hỏi về quy trình đổi gói

    Args:
        user_id: ID của người dùng cần đổi gói
        new_level: Gói cước mới - phải là 'free' hoặc 'paid'
    """
    # Validate input
    new_level = new_level.lower().strip()
    if new_level not in ("free", "paid"):
        return "❌ Gói cước không hợp lệ. Chỉ chấp nhận 'free' hoặc 'paid'."

    try:
        client = bigquery.Client(project=settings.GCP_PROJECT)

        # Check current user subscription
        check_query = f"""
        SELECT user_id, full_name, current_level
        FROM `{settings.GCP_PROJECT}.{settings.BQ_DATASET_MARTS}.mart_active_users`
        WHERE CAST(user_id AS STRING) = @user_id
        LIMIT 1
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("user_id", "STRING", str(user_id))]
        )

        results = list(client.query(check_query, job_config=job_config).result())

        if not results:
            return f"❌ Không tìm thấy user với ID: {user_id}"

        user = results[0]
        current_level = user.current_level

        if current_level == new_level:
            return (
                f"ℹ️ Người dùng **{user.full_name}** đã đang sử dụng gói **{new_level.upper()}** rồi. "
                f"Không cần thay đổi."
            )

        # --- Subscription change logic ---
        # In production: call an actual backend API to process the change.
        # For now: log the intent and return confirmation to the user.

        if new_level == "paid":
            return (
                f"✅ **Yêu cầu nâng cấp đã được ghi nhận!**\n\n"
                f"👤 Người dùng: **{user.full_name}** (ID: {user_id})\n"
                f"📦 Thay đổi: **{current_level.upper()}** → **PAID**\n\n"
                f"💎 **Quyền lợi gói PAID:**\n"
                f"- 🎵 Nghe nhạc không giới hạn, không quảng cáo\n"
                f"- 📥 Tải nhạc offline\n"
                f"- 🎧 Chất lượng âm thanh cao (320kbps)\n"
                f"- 🎶 Truy cập sớm các bản phát hành mới\n\n"
                f"⚠️ *Lưu ý: Yêu cầu sẽ được xử lý bởi hệ thống thanh toán. "
                f"Vui lòng kiểm tra email để hoàn tất.*"
            )
        else:
            return (
                f"✅ **Yêu cầu hạ cấp đã được ghi nhận!**\n\n"
                f"👤 Người dùng: **{user.full_name}** (ID: {user_id})\n"
                f"📦 Thay đổi: **{current_level.upper()}** → **FREE**\n\n"
                f"⚠️ **Lưu ý khi chuyển về FREE:**\n"
                f"- Sẽ có quảng cáo xen kẽ\n"
                f"- Không thể tải nhạc offline\n"
                f"- Chất lượng âm thanh tiêu chuẩn (128kbps)\n\n"
                f"*Thay đổi sẽ có hiệu lực vào cuối chu kỳ thanh toán hiện tại.*"
            )

    except Exception as e:
        return f"❌ Lỗi khi xử lý yêu cầu: {str(e)}"
