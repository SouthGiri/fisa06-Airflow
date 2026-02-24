import os
import json
import smtplib
from email.message import EmailMessage
from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.parse import urlencode

import pandas as pd
import requests
from airflow.sdk import dag, task
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.operators.empty import EmptyOperator
from supabase import create_client


KST = ZoneInfo("Asia/Seoul")
FINLIFE_BASE_URL = "http://finlife.fss.or.kr/finlifeapi"


def build_product_url(endpoint: str, fin_api_key: str) -> str:
    query = urlencode(
        {
            "auth": fin_api_key,
            "topFinGrpNo": "020000",
            "pageNo": "1",
        }
    )
    return f"{FINLIFE_BASE_URL}/{endpoint}?{query}"


def fetch_product_data(url: str) -> dict:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def transform_data(api_res: dict, p_type: str, target_date: str | None = None) -> pd.DataFrame:
    base_list = api_res.get("result", {}).get("baseList", [])
    option_list = api_res.get("result", {}).get("optionList", [])

    if not base_list or not option_list:
        print(f"[{p_type}] 수집된 데이터가 없습니다.")
        return pd.DataFrame()

    df_base = pd.DataFrame(base_list)
    df_opt = pd.DataFrame(option_list)
    df = pd.merge(df_base, df_opt, on="fin_prdt_cd", how="left", suffixes=("", "_dup"))

    df["product_type"] = p_type
    df["collected_at"] = target_date if target_date else datetime.now(KST).strftime("%Y-%m-%d")

    if "rsrv_type_nm" not in df.columns:
        df["rsrv_type_nm"] = "-"

    df["intr_rate"] = pd.to_numeric(df["intr_rate"], errors="coerce")
    df["intr_rate2"] = pd.to_numeric(df["intr_rate2"], errors="coerce")
    df["save_trm"] = pd.to_numeric(df["save_trm"], errors="coerce").fillna(0).astype(int)
    df["spcl_cnd"] = df["spcl_cnd"].fillna("해당사항 없음")

    cols = [
        "collected_at",
        "product_type",
        "fin_co_no",
        "fin_prdt_cd",
        "save_trm",
        "intr_rate_type",
        "rsrv_type_nm",
        "kor_co_nm",
        "fin_prdt_nm",
        "intr_rate",
        "intr_rate2",
        "dcls_strt_day",
        "spcl_cnd",
    ]

    for col in cols:
        if col not in df.columns:
            df[col] = None

    return df[cols]


def upsert_to_supabase(
    supabase_url: str,
    supabase_key: str,
    table_name: str,
    records: list[dict],
) -> int:
    if not records:
        return 0

    supabase = create_client(supabase_url, supabase_key)
    response = supabase.table(table_name).upsert(records).execute()
    data = getattr(response, "data", None)
    return len(data) if isinstance(data, list) else 0


@dag(
    schedule='0 9 * * *',
    catchup=False
)
def finance_products_pipeline():
    @task.sensor(poke_interval=30, timeout=300)
    def check_deposit_url_available() -> PokeReturnValue:
        fin_api = os.getenv("FIN_API")
        if not fin_api:
            raise ValueError("FIN_API is not set in environment.")

        deposit_url = build_product_url("depositProductsSearch.json", fin_api)
        response = requests.get(deposit_url, timeout=10)
        return PokeReturnValue(is_done=(response.status_code == 200), xcom_value=deposit_url)


    @task.sensor(poke_interval=30, timeout=300)
    def check_saving_url_available() -> PokeReturnValue:
        fin_api = os.getenv("FIN_API")
        if not fin_api:
            raise ValueError("FIN_API is not set in environment.")

        saving_url = build_product_url("savingProductsSearch.json", fin_api)
        response = requests.get(saving_url, timeout=10)
        return PokeReturnValue(is_done=(response.status_code == 200), xcom_value=saving_url)


    @task
    def fetch_finance_data(deposit_url: str, saving_url: str) -> dict:
        return {
            "deposit": fetch_product_data(deposit_url),
            "saving": fetch_product_data(saving_url),
        }


    @task
    def transform_finance_data(api_payload: dict) -> list[dict]:
        collected_at = datetime.now(KST).strftime("%Y-%m-%d")
        df_dep = transform_data(api_payload["deposit"], "DEPOSIT", target_date=collected_at)
        df_sav = transform_data(api_payload["saving"], "SAVING", target_date=collected_at)
        final_df = pd.concat([df_dep, df_sav], ignore_index=True)
        return final_df.to_dict(orient="records")


    @task
    def validate_supabase_credentials(records: list[dict]) -> list[dict]:
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")
        table_name = os.getenv("SUPABASE_TABLE", "finance_data")

        if not supabase_url or not supabase_key:
            raise ValueError("SUPABASE_URL or SUPABASE_KEY is not set in environment.")

        supabase = create_client(supabase_url, supabase_key)
        # Minimal query to verify URL/KEY are valid before write tasks.
        supabase.table(table_name).select("*").limit(1).execute()
        print("Supabase credential validation passed.")
        return records
    
    
    @task
    def upsert_finance_data(records: list[dict]) -> int:
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")
        table_name = os.getenv("SUPABASE_TABLE", "finance_data")

        inserted_count = upsert_to_supabase(supabase_url, supabase_key, table_name, records)
        print(f"Upserted rows: {inserted_count}")
        return inserted_count


    @task
    def get_new_better_products_v3(_: int):
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")

        supabase = create_client(supabase_url, supabase_key)
        response = supabase.rpc("get_new_better_products_v3").execute()
        result = getattr(response, "data", None)
        print("Executed procedure: get_new_better_products_v3()")
        return result


    @task.branch
    def branch_on_procedure_result(procedure_result: object) -> str:
        if procedure_result is None:
            return "skip_email_notification"
        return "send_result_to_active_users"


    @task
    def send_result_to_active_users(procedure_result: object) -> int:
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")
        smtp_host = os.getenv("SMTP_HOST")
        smtp_port = int(os.getenv("SMTP_PORT", "587"))
        smtp_user = os.getenv("SMTP_USER")
        smtp_password = os.getenv("SMTP_PASSWORD")
        smtp_from = os.getenv("SMTP_FROM", smtp_user)

        if not all([smtp_host, smtp_user, smtp_password, smtp_from]):
            raise ValueError("SMTP_HOST, SMTP_USER, SMTP_PASSWORD, SMTP_FROM must be set.")

        supabase = create_client(supabase_url, supabase_key)
        users_response = (
            supabase.table("user_emails")
            .select("email")
            .eq("is_active", True)
            .execute()
        )
        users = getattr(users_response, "data", []) or []
        recipients = [row.get("email") for row in users if row.get("email")]

        if not recipients:
            print("No active recipients found in user_emails.")
            return 0

        detail_url = "https://tech-semina-o9fy4ztqcxvobomlnzychy.streamlit.app/"
        text_footer = (
            "\n\n자세한 사항은 아래 링크를 통해 확인할 수 있습니다.\n"
            f"{detail_url}"
        )
        html_footer = (
            '<p>자세한 사항은 아래 링크를 통해 확인할 수 있습니다.</p>'
            f'<p><a href="{detail_url}">{detail_url}</a></p>'
        )
        display_columns = [
            "상품 타입",
            "비교 우리 은행 상품",
            "저축 기간",
            "타행명",
            "타행 상품명",
            "우리은행 기본금리",
            "우리은행 최대금리",
            "타행 기본금리",
            "타행 최대금리",
            "최대 금리차",
        ]
        excluded_columns = {"spcl_cnd", "우대 조건 상세", "우대조건 상세"}

        if isinstance(procedure_result, list) and procedure_result:
            df = pd.DataFrame(procedure_result)
            df = df[[col for col in df.columns if col not in excluded_columns]]
            ordered_cols = [col for col in display_columns if col in df.columns]
            if ordered_cols:
                df = df[ordered_cols]
            text_body = (
                f"Rows: {len(df)}\n\n"
                f"{df.to_string(index=False)}"
            ) + text_footer
            html_body = (
                f"<p>Rows: {len(df)}</p>"
                + df.to_html(index=False, border=1, justify="left")
                + html_footer
            )
        elif isinstance(procedure_result, dict):
            filtered = {k: v for k, v in procedure_result.items() if k not in excluded_columns}
            df = pd.DataFrame([filtered])
            ordered_cols = [col for col in display_columns if col in df.columns]
            if ordered_cols:
                df = df[ordered_cols]
            text_body = (
                "Rows: 1\n\n"
                f"{df.to_string(index=False)}"
            ) + text_footer
            html_body = (
                "<p>Rows: 1</p>"
                + df.to_html(index=False, border=1, justify="left")
                + html_footer
            )
        else:
            text_body = (
                f"Result:\n{procedure_result}"
            ) + text_footer
            html_body = (
                f"<pre>{procedure_result}</pre>"
                + html_footer
            )

        sent_count = 0
        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            for recipient in recipients:
                message = EmailMessage()
                message["Subject"] = "[Airflow] New Better Products Result"
                message["From"] = smtp_from
                message["To"] = recipient
                message.set_content(text_body)
                message.add_alternative(html_body, subtype="html")
                server.send_message(message)
                sent_count += 1

        print(f"Sent result emails: {sent_count}")
        return sent_count


    deposit_url = check_deposit_url_available()
    saving_url = check_saving_url_available()
    api_payload = fetch_finance_data(deposit_url, saving_url)
    transformed_records = transform_finance_data(api_payload)
    validated_records = validate_supabase_credentials(transformed_records)
    upserted_count = upsert_finance_data(validated_records)
    procedure_result = get_new_better_products_v3(upserted_count)
    branch = branch_on_procedure_result(procedure_result)
    send_email = send_result_to_active_users(procedure_result)
    skip_email = EmptyOperator(task_id="skip_email_notification")
    branch >> [send_email, skip_email]

    # shift 연산자로 실행 순서 표현
    [deposit_url, saving_url] >> api_payload >> transformed_records >> validated_records >> upserted_count >> procedure_result >> branch
    branch >> [send_email, skip_email]


finance_products_pipeline()
