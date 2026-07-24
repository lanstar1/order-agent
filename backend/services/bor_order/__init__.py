"""
중국오더(BOR) 자동화 모듈

- db.py             : bor_* 테이블 생성 + bor_config 설정 관리
- mail_collector.py : IMAP(UID) 메일 수집 — marked Rest List / new order xlsx / kyu 텍스트 오더
- erp_ext.py        : ECOUNT 재고·안전재고 조회 + sales_records 월별 판매 집계
- engine.py         : 수요 산정(스파이크÷12/수요소멸/이상치 캡) + 발주 초안 생성
- xlsx_writer.py    : new order-YYYY-MMDD.xlsx 생성
- sender.py         : 승인된 초안 SMTP 발송

주의: 하위 모듈은 순환 import 방지를 위해 필요 시점에 지연 import 한다.
"""
