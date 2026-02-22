# Deposit Comparison Pipeline

금융감독원 `finlife` 예·적금 API 데이터를 수집/가공하고, Supabase에 적재한 뒤 프로시저 결과를 이메일로 전송하는 Airflow 기반 데이터 파이프라인 프로젝트입니다.

## 1. 프로젝트 소개

- Airflow(Docker)로 데이터 파이프라인을 스케줄링/오케스트레이션합니다.
- 예금/적금 API 응답을 수집하고 `pandas`로 변환합니다.
- Supabase 테이블에 upsert 후, 추천 프로시저(`get_new_better_products_v3`)를 실행합니다.
- 결과가 있을 때만 활성 사용자(`user_emails.is_active = true`)에게 메일을 전송합니다.

## 기술 스택

- Language  
  ![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
- Orchestration  
  ![Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
- Data Processing  
  ![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)
- Database / Storage  
  ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
  ![Supabase](https://img.shields.io/badge/Supabase-3ECF8E?style=for-the-badge&logo=supabase&logoColor=white)
- Runtime / Infra  
  ![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
  ![Docker Compose](https://img.shields.io/badge/Docker%20Compose-1D63ED?style=for-the-badge&logo=docker&logoColor=white)

주요 파일:
- `dags/finance_products_pipeline.py`: 메인 DAG
- `docker-compose.yaml`: Airflow + Postgres 실행 환경
- `.env`: API 키, Supabase, SMTP 환경변수
- `scripts/test_smtp_task.py`: SMTP 단독 테스트 스크립트

## 2. DAG 흐름

`finance_products_pipeline` DAG 흐름:

1. `check_deposit_url_available` (sensor)
2. `check_saving_url_available` (sensor)
3. `fetch_finance_data`
4. `transform_finance_data`
5. `validate_supabase_credentials`
6. `upsert_finance_data`
7. `get_new_better_products_v3`
8. `branch_on_procedure_result`
9. 결과가 `None`이 아니면 `send_result_to_active_users`, 아니면 `skip_email_notification`

요약 플로우:

```text
[deposit sensor]   [saving sensor]
       \             /
        \           /
       fetch_finance_data
              |
      transform_finance_data
              |
  validate_supabase_credentials
              |
       upsert_finance_data
              |
  get_new_better_products_v3
              |
  branch_on_procedure_result
        /                 \
send_result_to_active_users  skip_email_notification
```

## 3. 프로젝트 수행 방법

### 3-1. `.env` 준비

`.env`에 최소한 아래 값들을 설정합니다.

```env
FIN_API="..."
SUPABASE_URL="..."
SUPABASE_KEY="..."

SMTP_HOST="smtp.gmail.com"
SMTP_PORT="587"
SMTP_USER="..."
SMTP_PASSWORD="..."
SMTP_FROM="..."

_PIP_ADDITIONAL_REQUIREMENTS="supabase pandas"
```

### 3-2. Airflow 실행

프로젝트 루트에서 실행:

```bash
docker compose up -d
```

초기화/재생성이 필요할 때:

```bash
docker compose down
docker compose up -d
```

### 3-3. Airflow 접속

브라우저에서 접속:

```text
http://localhost:8080
```

기본 계정(별도 변경하지 않은 경우):

```text
ID: airflow
PW: airflow
```

### 3-4. DAG 실행

1. Airflow UI에서 `finance_products_pipeline` DAG 활성화
2. `Trigger DAG` 클릭
3. Graph/Log에서 단계별 성공 여부 확인