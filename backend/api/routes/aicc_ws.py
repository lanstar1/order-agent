"""
AICC WebSocket 핸들러
"""
from fastapi import WebSocket, WebSocketDisconnect
from services.aicc_session_manager import session_manager
from services.aicc_ai_service import get_ai_response


async def customer_ws_handler(websocket: WebSocket, session_id: str):
    """고객 채팅 WebSocket"""
    await websocket.accept()

    params = dict(websocket.query_params)
    name = params.get("name", "")
    model = params.get("model", "")
    erp_code = params.get("erp_code", "")
    menu = params.get("menu", "제품문의")

    # 세션 생성
    actual_sid = session_manager.create(name, model, erp_code, menu)
    s = session_manager.get(actual_sid)
    s["customer_ws"] = websocket

    # 관리자에게 신규 알림
    await session_manager.broadcast_admins({
        "type": "new_session",
        "session": session_manager.serialize(s)
    })

    # 첫 인사 메시지
    # 모델명이 있으면 표시, 없으면 메뉴만 표시
    model_text = f"{model} " if model else ""
    greeting = f"안녕하세요{', ' + name + '님' if name else ''}! 랜스타 AI 상담사입니다.\n{model_text}{menu} 상담을 시작합니다. 궁금하신 점을 편하게 말씀해 주세요."
    await websocket.send_json({"type": "ai_message", "content": greeting})

    try:
        while True:
            data = await websocket.receive_json()
            msg_type = data.get("type", "")
            content = str(data.get("content", "")).strip()

            if not content and msg_type == "chat":
                continue  # 빈 메시지 무시

            if msg_type == "chat":
                # 메시지 저장
                session_manager.add_message(actual_sid, "user", content)

                # 관리자에게 포워딩
                await session_manager.send_admin(actual_sid, {
                    "type": "customer_message",
                    "role": "user",
                    "content": content
                })

                # 개입 중이면 AI 응답 안 함
                if s["is_admin_intervened"]:
                    continue

                # AI 응답 생성
                try:
                    ai_reply = await get_ai_response(s, content)
                except Exception as e:
                    ai_reply = "일시적인 오류가 발생했습니다. 잠시 후 다시 시도해 주세요."
                    print(f"[AICC WS] AI 오류: {e}")

                session_manager.add_message(actual_sid, "assistant", ai_reply)
                await websocket.send_json({"type": "ai_message", "content": ai_reply})
                await session_manager.send_admin(actual_sid, {
                    "type": "ai_message",
                    "role": "assistant",
                    "content": ai_reply
                })

            elif msg_type == "request_admin":
                s["status"] = "waiting_admin"
                await session_manager.broadcast_admins({
                    "type": "session_update",
                    "session": session_manager.serialize(s)
                })
                await websocket.send_json({
                    "type": "system",
                    "content": "담당자 연결을 요청했습니다. 잠시만 기다려 주세요."
                })

            elif msg_type == "close":
                session_manager.close(actual_sid)
                break

    except WebSocketDisconnect:
        pass
    finally:
        s["customer_ws"] = None


async def admin_ws_handler(websocket: WebSocket, session_id: str):
    """관리자 모니터링 + 개입 WebSocket"""
    await websocket.accept()

    s = session_manager.get(session_id)
    if not s:
        await websocket.close(code=4004)
        return
    s["admin_ws"] = websocket

    try:
        while True:
            data = await websocket.receive_json()
            msg_type = data.get("type", "")
            content = str(data.get("content", "")).strip()

            if msg_type == "admin_message" and content:
                session_manager.add_message(session_id, "admin", content)
                await session_manager.send_customer(session_id, {
                    "type": "admin_message",
                    "content": content
                })

            elif msg_type == "intervene":
                session_manager.intervene(session_id)
                await session_manager.send_customer(session_id, {
                    "type": "admin_joined",
                    "content": "담당자가 연결되었습니다."
                })

            elif msg_type == "close_session":
                session_manager.close(session_id)
                await session_manager.send_customer(session_id, {
                    "type": "session_closed",
                    "content": "상담이 종료되었습니다."
                })

    except WebSocketDisconnect:
        pass
    finally:
        s["admin_ws"] = None


async def admin_list_ws_handler(websocket: WebSocket):
    """관리자 세션 목록 실시간 업데이트용 WebSocket"""
    await websocket.accept()
    session_manager.admin_list_sockets.append(websocket)
    try:
        # 연결 즉시 현재 세션 목록 전송
        await websocket.send_json({
            "type": "sessions_list",
            "sessions": session_manager.all_serialized()
        })
        while True:
            await websocket.receive_text()  # ping 유지
    except WebSocketDisconnect:
        pass
    finally:
        if websocket in session_manager.admin_list_sockets:
            session_manager.admin_list_sockets.remove(websocket)
