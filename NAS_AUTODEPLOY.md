# NAS 자동배포 (nas 브랜치)

이 브랜치(nas)는 사무실 NAS의 order-agent **smartstore 미러 전용**이다.

- Render는 `main` 브랜치를 사용한다. `nas` 브랜치는 NAS 전용으로 coupang/gmarket/로젠 OpenAPI 등 NAS 고유 기능을 포함한다.
- NAS 폴러(`order-agent-deploy-poller`)가 90초마다 `origin/nas`를 확인해 자동 빌드·재배포한다.
- 운영 서비스: http://10.0.0.152:8001

## 배포
```
git push origin nas      # 약 1~2분 뒤 자동 반영
```

## 주의
- `main`(Render)과 `nas`(NAS)는 **의도적으로 분리**되어 있다. 서로 merge하지 말 것.
- `.env`, `data/` 는 git 제외(NAS에만 존재). 비밀키는 NAS `/volume1/lanstar/order-agent/.env`.
