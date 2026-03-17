"""
AICC WebSocket 핸들러 (고객 / 관리자)
"""
from fastapi import WebSocket, WebSocketDisconnect
from services.aicc_session_manager import session_manager
from services.aicc_ai_service import get_ai_response


async def ws_customer_handler(websocket: WebSocket, session_id: str):
    """고객 WebSocket 핸들러"""
    await websocket.accept()

    # URL 파라미터 파싱
    params = dict(websocket.query_params)
    customer_name = params.get("name", "")
    model_name = params.get("model", "")
    erp_code = params.get("erp_code", "")
    menu = params.get("menu", "제품문의")

    # 세션 생성 또는 재연결
    s = session_manager.get_session(session_id)
    if not s:
        session_manager.create_session(session_id, customer_name, model_name, erp_code, menu)
        s = session_manager.get_session(session_id)
    s["customer_ws"] = websocket

    # 관리자에게 신규 세션 알림
    await session_manager.broadcast_session_update(s)

    try:
        while True:
            data = await websocket.receive_json()
            msg_type = data.get("type")
            content = data.get("content", "")

            if msg_type == "chat":
                session_manager.add_message(session_id, "user", content)
                # 관리자에게 포워딩
                await session_manager.send_to_admin(session_id, {
                    "type": "customer_message", "role": "user", "content": content
                })

                if s["is_admin_intervened"]:
                    # 개입 중 → AI 응답 안 함
                    continue

                # AI 응답
                ai_reply = await get_ai_response(s, content)
                session_manager.add_message(session_id, "assistant", ai_reply)
                await websocket.send_json({"type": "ai_message", "content": ai_reply})
                await session_manager.send_to_admin(session_id, {
                    "type": "ai_message", "role": "assistant", "content": ai_reply
                })

            elif msg_type == "request_admin":
                s["status"] = "waiting_admin"
                await session_manager.broadcast_session_update(s)
                await websocket.send_json({
                    "type": "system",
                    "content": "담당자 연결을 요청했습니다. 잠시만 기다려 주세요."
                })

            elif msg_type == "close":
                session_manager.close_session(session_id)
                break

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"[AICC WS] 고객 핸들러 오류: {e}")
    finally:
        if s:
            s["customer_ws"] = None


async def ws_admin_handler(websocket: WebSocket, session_id: str):
    """관리자 WebSocket 핸들러"""
    await websocket.accept()

    s = session_manager.get_session(session_id)
    if not s:
        await websocket.close()
        return
    s["admin_ws"] = websocket

    try:
        while True:
            data = await websocket.receive_json()
            msg_type = data.get("type")
            content = data.get("content", "")

            if msg_type == "admin_message":
                session_manager.add_message(session_id, "admin", content)
                await session_manager.send_to_customer(session_id, {
                    "type": "admin_message", "content": content
                })
            elif msg_type == "intervene":
                session_manager.intervene(session_id)
                await session_manager.send_to_customer(session_id, {
                    "type": "admin_joined",
                    "content": "담당자가 연결되었습니다."
                })
            elif msg_type == "close_session":
                session_manager.close_session(session_id)
                await session_manager.send_to_customer(session_id, {
                    "type": "session_closed",
                    "content": "상담이 종료되었습니다."
                })

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"[AICC WS] 관리자 핸들러 오류: {e}")
    finally:
        if s:
            s["admin_ws"] = None


async def ws_admin_list_handler(websocket: WebSocket):
    """관리자 세션 목록 실시간 수신 WebSocket"""
    await websocket.accept()
    session_manager.admin_list_ws.append(websocket)
    try:
        while True:
            await websocket.receive_text()  # keep-alive
    except WebSocketDisconnect:
        pass
    finally:
        if websocket in session_manager.admin_list_ws:
            session_manager.admin_list_ws.remove(websocket)
