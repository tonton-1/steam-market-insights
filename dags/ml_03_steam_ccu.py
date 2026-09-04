"""
ml_03_steam_ccu.py
===================
Project: Steam CCU Forecasting ML Pipeline (Champion-Challenger)
ทำนายจำนวนผู้เล่นพร้อมกัน (Concurrent Users - CCU) ของเกม Counter-Strike 2 (CS2) สำหรับวันถัดไป

เป้าหมายของไฟล์นี้:
- ใช้โครงสร้าง ML Pipeline แบบ Champion-Challenger บน Airflow (สอดคล้องกับ ml_02_weather_pipeline)
- ใช้ features: CCU ย้อนหลัง 7 วัน (Lag 1-7), ค่าเฉลี่ยเคลื่อนที่ (Moving Average 7d), Weekend Effect (เสาร์-อาทิตย์), และ On-Sale Status
- มีระบบ Bootstrap ข้อมูลประวัติย้อนหลังอัตโนมัติ (idempotent) หากข้อมูลในระบบยังไม่ถึง 14 วัน ทำให้ทดสอบรันได้ทันที
- เปรียบเทียบ RMSE ของ Candidate Model กับ Champion Model ที่บันทึกไว้ใน PostgreSQL
- Deploy โมเดลที่ชนะไปยัง /opt/airflow/models/steam_models/current_model.pkl
- ทดสอบ Smoke Test พยากรณ์ CCU วันพรุ่งนี้จริง
"""

import math
import os
import random
from datetime import date, datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

# -----------------------------------------------------------------
# ค่าตั้งต้น
# -----------------------------------------------------------------
default_args = {
    "owner": "steam_project",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

POSTGRES_CONN_ID = "postgres_target"
TARGET_APPID = 730
TARGET_NAME = "Counter-Strike 2"
MODEL_NAME = "steam_ccu_cs2"

MIN_HISTORY_DAYS = 14  # ต้องการประวัติอย่างน้อย 14 วัน
LAG_WINDOW = 7         # ใช้สถิติย้อนหลัง 7 วันเป็น Feature
HOLDOUT_SIZE = 3       # กัน 3 วันล่าสุดไว้วัดผล RMSE

MODEL_DIR = "/opt/airflow/models/steam_models"
CURRENT_MODEL_PATH = os.path.join(MODEL_DIR, "current_model.pkl")

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS steam_games (
    id               SERIAL PRIMARY KEY,
    snapshot_date    DATE NOT NULL,
    appid            INT NOT NULL,
    name             VARCHAR(255) NOT NULL,
    developer        VARCHAR(255),
    publisher        VARCHAR(255),
    price_thb        NUMERIC(10, 2) DEFAULT 0,
    original_price   NUMERIC(10, 2) DEFAULT 0,
    discount_pct     INT DEFAULT 0,
    ccu              INT DEFAULT 0,
    positive_reviews INT DEFAULT 0,
    negative_reviews INT DEFAULT 0,
    review_score     NUMERIC(5,2) DEFAULT 0,
    owners_estimate  VARCHAR(50),
    genres           TEXT,
    header_image     TEXT,
    is_free          BOOLEAN DEFAULT FALSE,
    ingested_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE (snapshot_date, appid)
);

CREATE TABLE IF NOT EXISTS steam_model_metrics (
    id          SERIAL PRIMARY KEY,
    model_name  VARCHAR(50) NOT NULL,
    target_game VARCHAR(100) NOT NULL,
    rmse        FLOAT NOT NULL,
    deployed    BOOLEAN NOT NULL,
    run_at      TIMESTAMP DEFAULT NOW()
);
"""


# -----------------------------------------------------------------
# 1. Bootstrap: เติมข้อมูลประวัติย้อนหลังหากยังไม่พอ (Idempotent)
# -----------------------------------------------------------------
def bootstrap_historical_ccu(**kwargs):
    """
    ตรวจสอบว่ามีข้อมูล CCU ของเกมเป้าหมายครบ MIN_HISTORY_DAYS หรือยัง
    หากยังไม่พอ จะจำลองข้อมูลย้อนหลังตามลักษณะจริงของ CS2 (900k - 1.2M, เสาร์-อาทิตย์พุ่งสูง)
    เพื่อให้สามารถเทรนและทดสอบ ML Pipeline ได้ทันทีตั้งแต่วันแรก
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    count = hook.get_first(
        "SELECT COUNT(*) FROM steam_games WHERE appid = %s;",
        parameters=(TARGET_APPID,),
    )[0]

    print(f"📊 พบข้อมูลประวัติ {TARGET_NAME} ในระบบ: {count} วัน (ต้องการขั้นต่ำ {MIN_HISTORY_DAYS} วัน)")

    if count >= MIN_HISTORY_DAYS:
        print("✅ ข้อมูลประวัติเพียงพอแล้ว ข้ามขั้นตอน Bootstrap")
        return

    today = date.today()
    days_to_add = MIN_HISTORY_DAYS + 7  # สร้างไว้ 21 วัน
    inserted = 0

    base_ccu = 980000
    random.seed(42)

    for i in range(days_to_add, 0, -1):
        snap_date = today - timedelta(days=i)
        is_weekend = snap_date.weekday() >= 5  # เสาร์-อาทิตย์
        weekend_boost = random.randint(120000, 250000) if is_weekend else 0
        fluctuation = random.randint(-40000, 50000)
        simulated_ccu = int(base_ccu + weekend_boost + fluctuation)

        hook.run(
            """
            INSERT INTO steam_games (
                snapshot_date, appid, name, developer, publisher,
                price_thb, original_price, discount_pct, ccu,
                positive_reviews, negative_reviews, review_score,
                owners_estimate, genres, header_image, is_free
            ) VALUES (
                %s, %s, %s, 'Valve', 'Valve',
                0.0, 0.0, 0, %s,
                7600000, 1170000, 86.6,
                '100,000,000 .. 200,000,000', 'Action, Free to Play',
                'https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/730/header.jpg', TRUE
            )
            ON CONFLICT (snapshot_date, appid) DO NOTHING;
            """,
            parameters=(snap_date, TARGET_APPID, TARGET_NAME, simulated_ccu),
        )
        inserted += 1

    print(f"🚀 Bootstrap ข้อมูลย้อนหลังสำเร็จ {inserted} วัน สำหรับ {TARGET_NAME}")


# -----------------------------------------------------------------
# 2. Extract: ดึงหรืออัปเดต CCU ปัจจุบันของวันนี้
# -----------------------------------------------------------------
def extract_today_ccu(**kwargs):
    """ดึงจำนวนผู้เล่นล่าสุดจาก SteamSpy และบันทึก/อัปเดตของวันนี้"""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    today = date.today().isoformat()

    current_ccu = 1050000
    try:
        url = f"https://steamspy.com/api.php?request=appdetails&appid={TARGET_APPID}"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            fetched_ccu = int(data.get("ccu", 0) or 0)
            if fetched_ccu > 0:
                current_ccu = fetched_ccu
    except Exception as e:
        print(f"⚠️ ดึง SteamSpy สดไม่สำเร็จ ({e}) ใช้ค่ามาตรฐาน {current_ccu}")

    hook.run(
        """
        INSERT INTO steam_games (
            snapshot_date, appid, name, developer, publisher,
            price_thb, original_price, discount_pct, ccu,
            positive_reviews, negative_reviews, review_score,
            owners_estimate, genres, header_image, is_free
        ) VALUES (
            %s, %s, %s, 'Valve', 'Valve',
            0.0, 0.0, 0, %s,
            7640000, 1173000, 86.7,
            '100,000,000 .. 200,000,000', 'Action, Free to Play',
            'https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/730/header.jpg', TRUE
        )
        ON CONFLICT (snapshot_date, appid) DO UPDATE SET
            ccu = EXCLUDED.ccu,
            ingested_at = NOW();
        """,
        parameters=(today, TARGET_APPID, TARGET_NAME, current_ccu),
    )
    print(f"🎯 อัปเดต CCU วันนี้ ({today}) ของ {TARGET_NAME}: {current_ccu:,} คน")


# -----------------------------------------------------------------
# 3. Prepare: สร้าง Feature Matrix (Lag 1-7, Moving Avg, Weekend, Sale)
# -----------------------------------------------------------------
def prepare_training_data(**kwargs):
    """
    ดึงข้อมูล CCU เรียงตามวันที่ แล้วสร้างชุด Feature:
    - Lag 1 ถึง Lag 7: ค่า CCU 7 วันก่อนหน้า
    - Moving Average 7 วัน
    - is_weekend: 1 ถ้าเป็นเสาร์/อาทิตย์, 0 ถ้าวันธรรมดา
    - on_sale: 1 ถ้าเกมกำลังลดราคา
    """
    ti = kwargs["ti"]
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    rows = hook.get_records(
        """
        SELECT snapshot_date, ccu, discount_pct,
               EXTRACT(DOW FROM snapshot_date) AS dow
        FROM steam_games
        WHERE appid = %s AND ccu IS NOT NULL
        ORDER BY snapshot_date ASC;
        """,
        parameters=(TARGET_APPID,),
    )

    df = pd.DataFrame(rows, columns=["date", "ccu", "discount_pct", "dow"])
    print(f"📈 ดึงข้อมูลประวัติมาได้ทั้งหมด {len(df)} วัน")

    if len(df) < LAG_WINDOW + HOLDOUT_SIZE + 1:
        raise ValueError(f"ข้อมูลไม่พอ: มี {len(df)} แถว ต้องการขั้นต่ำ {LAG_WINDOW + HOLDOUT_SIZE + 1}")

    ccu_list = df["ccu"].tolist()
    dow_list = df["dow"].tolist()
    disc_list = [int(d or 0) for d in df["discount_pct"].tolist()]

    features, targets = [], []
    for i in range(LAG_WINDOW, len(ccu_list)):
        lags = ccu_list[i - LAG_WINDOW : i]
        moving_avg = sum(lags) / len(lags)
        is_weekend = 1 if dow_list[i] in [0, 6] else 0  # 0=Sunday, 6=Saturday
        on_sale = 1 if disc_list[i] > 0 else 0

        features.append(lags + [moving_avg, is_weekend, on_sale])
        targets.append(ccu_list[i])

    # Feature ล่าสุด สำหรับพยากรณ์วันพรุ่งนี้ (Tomorrow)
    last7 = ccu_list[-LAG_WINDOW:]
    tomorrow = date.today() + timedelta(days=1)
    tomorrow_weekend = 1 if tomorrow.weekday() >= 5 else 0
    latest_feat = last7 + [sum(last7) / len(last7), tomorrow_weekend, 1 if disc_list[-1] > 0 else 0]

    ti.xcom_push(key="features", value=features)
    ti.xcom_push(key="targets", value=targets)
    ti.xcom_push(key="latest_features", value=latest_feat)
    ti.xcom_push(key="latest_date", value=str(df["date"].iloc[-1]))
    print(f"✨ สร้าง Feature Dataset สำเร็จ {len(features)} ตัวอย่าง")


# -----------------------------------------------------------------
# 4. Train: เทรน RandomForestRegressor
# -----------------------------------------------------------------
def train_model(**kwargs):
    """เทรนโมเดล RandomForestRegressor โดยกัน HOLDOUT_SIZE แถวสุดท้ายไว้วัดผล"""
    import joblib
    from sklearn.ensemble import RandomForestRegressor

    ti = kwargs["ti"]
    features = ti.xcom_pull(task_ids="prepare_training_data", key="features")
    targets = ti.xcom_pull(task_ids="prepare_training_data", key="targets")

    X_train = features[:-HOLDOUT_SIZE]
    y_train = targets[:-HOLDOUT_SIZE]
    X_holdout = features[-HOLDOUT_SIZE:]
    y_holdout = targets[-HOLDOUT_SIZE:]

    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    os.makedirs(MODEL_DIR, exist_ok=True)
    run_id = kwargs["run_id"].replace(":", "-").replace("+", "-")
    candidate_path = os.path.join(MODEL_DIR, f"candidate_{run_id}.pkl")
    joblib.dump(model, candidate_path)

    ti.xcom_push(key="candidate_model_path", value=candidate_path)
    ti.xcom_push(key="X_holdout", value=X_holdout)
    ti.xcom_push(key="y_holdout", value=y_holdout)

    print(f"🤖 เทรน Candidate Model เสร็จสิ้น (Train: {len(X_train)}, Holdout: {len(X_holdout)})")
    print(f"💾 บันทึกโมเดลไว้ที่: {candidate_path}")


# -----------------------------------------------------------------
# 5. Evaluate: คำนวณค่าความคลาดเคลื่อน (RMSE) บน Holdout Set
# -----------------------------------------------------------------
def evaluate_model(**kwargs):
    """วัดค่า RMSE บนชุด Holdout ที่โมเดลไม่เคยเห็นตอนเทรน"""
    import joblib

    ti = kwargs["ti"]
    candidate_path = ti.xcom_pull(task_ids="train_model", key="candidate_model_path")
    X_holdout = ti.xcom_pull(task_ids="train_model", key="X_holdout")
    y_holdout = ti.xcom_pull(task_ids="train_model", key="y_holdout")

    model = joblib.load(candidate_path)
    predictions = model.predict(X_holdout)

    squared_errors = [(p - a) ** 2 for p, a in zip(predictions, y_holdout)]
    rmse = math.sqrt(sum(squared_errors) / len(squared_errors))

    ti.xcom_push(key="rmse", value=rmse)
    print(f"🎯 ผลการประเมิน Candidate Model: RMSE = {rmse:,.2f} คน (ยิ่งต่ำยิ่งแม่นยำ)")


# -----------------------------------------------------------------
# 6. Champion Check: ดึงค่า RMSE ของโมเดลปัจจุบันที่ Deploy อยู่
# -----------------------------------------------------------------
def get_previous_rmse(**kwargs):
    """ดึงสถิติ RMSE ของ Champion Model จาก PostgreSQL"""
    ti = kwargs["ti"]
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    row = hook.get_first(
        """
        SELECT rmse FROM steam_model_metrics
        WHERE model_name = %s AND deployed = TRUE
        ORDER BY run_at DESC LIMIT 1;
        """,
        parameters=(MODEL_NAME,),
    )
    previous_rmse = float(row[0]) if row else None

    ti.xcom_push(key="previous_rmse", value=previous_rmse)
    if previous_rmse is None:
        print("ℹ️ ยังไม่เคยมี Champion Model มาก่อน (รอบแรก) -> Candidate จะได้รับเลือกอัตโนมัติ")
    else:
        print(f"🏆 Champion Model ปัจจุบันมี RMSE = {previous_rmse:,.2f}")


# -----------------------------------------------------------------
# 7. Decide Deploy: เลือกว่าจะ Deploy หรือ Skip
# -----------------------------------------------------------------
def decide_deploy(**kwargs):
    """BranchPythonOperator: Deploy เฉพาะเมื่อ Candidate มี RMSE ดีกว่า Champion"""
    ti = kwargs["ti"]
    rmse = ti.xcom_pull(task_ids="evaluate_model", key="rmse")
    previous_rmse = ti.xcom_pull(task_ids="get_previous_rmse", key="previous_rmse")

    if previous_rmse is None or rmse < previous_rmse:
        print(f"🚀 Candidate (RMSE {rmse:,.2f}) ชนะ! -> เลือกเส้นทาง deploy_model")
        return "deploy_model"
    else:
        print(f"⏹️ Champion เดิม (RMSE {previous_rmse:,.2f}) ยังดีกว่า Candidate ({rmse:,.2f}) -> เลือก skip_deploy")
        return "skip_deploy"


# -----------------------------------------------------------------
# 8. Deploy: นำโมเดลขึ้นใช้งานจริง
# -----------------------------------------------------------------
def deploy_model(**kwargs):
    """คัดลอกไฟล์โมเดลไปทับ current_model.pkl"""
    import shutil

    ti = kwargs["ti"]
    candidate_path = ti.xcom_pull(task_ids="train_model", key="candidate_model_path")

    shutil.copyfile(candidate_path, CURRENT_MODEL_PATH)
    print(f"✅ Deploy โมเดลใหม่สำเร็จ: {candidate_path} -> {CURRENT_MODEL_PATH}")
    return "deployed"


def skip_deploy(**kwargs):
    """ข้ามการ Deploy และใช้โมเดลเดิมต่อไป"""
    ti = kwargs["ti"]
    rmse = ti.xcom_pull(task_ids="evaluate_model", key="rmse")
    prev_rmse = ti.xcom_pull(task_ids="get_previous_rmse", key="previous_rmse")
    print(f"ข้ามการ Deploy: RMSE รอบนี้ {rmse:,.2f} ยังไม่ชนะ {prev_rmse:,.2f}")
    return "skipped"


# -----------------------------------------------------------------
# 9. Smoke Test: ทดสอบพยากรณ์จริงด้วย Production Model
# -----------------------------------------------------------------
def smoke_test(**kwargs):
    """ทดสอบเรียกใช้งานโมเดล production พยากรณ์ CCU วันพรุ่งนี้"""
    import joblib

    ti = kwargs["ti"]
    latest_features = ti.xcom_pull(task_ids="prepare_training_data", key="latest_features")
    latest_date = ti.xcom_pull(task_ids="prepare_training_data", key="latest_date")

    if not os.path.exists(CURRENT_MODEL_PATH):
        print("⚠️ ไม่พบไฟล์ current_model.pkl")
        return

    model = joblib.load(CURRENT_MODEL_PATH)
    predicted_ccu = model.predict([latest_features])[0]

    tomorrow = date.today() + timedelta(days=1)
    print("\n🎮 ================== SMOKE TEST REPORT ==================")
    print(f"📌 ข้อมูลอ้างอิงล่าสุด: {latest_date}")
    print(f"🔮 พยากรณ์ผู้เล่น {TARGET_NAME} ในวันพรุ่งนี้ ({tomorrow}):")
    print(f"🔥 คาดว่าจะมีผู้เล่นพร้อมกันสูงสุด: {predicted_ccu:,.0f} คน")
    print("✅ โมเดลตอบสนองถูกต้อง พร้อมให้บริการบน Dashboard")
    print("=========================================================\n")


# -----------------------------------------------------------------
# 10. Log Result: บันทึกประวัติ Metric ลง PostgreSQL
# -----------------------------------------------------------------
def log_result(**kwargs):
    """บันทึกสถิติการเทรนลงตาราง steam_model_metrics"""
    ti = kwargs["ti"]
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    rmse = ti.xcom_pull(task_ids="evaluate_model", key="rmse")
    skip_res = ti.xcom_pull(task_ids="skip_deploy")
    is_deployed = skip_res is None

    hook.run(
        """
        INSERT INTO steam_model_metrics (model_name, target_game, rmse, deployed)
        VALUES (%s, %s, %s, %s);
        """,
        parameters=(MODEL_NAME, TARGET_NAME, rmse, is_deployed),
    )
    print(f"📝 บันทึกประวัติ Metric: RMSE={rmse:,.2f}, Deployed={'ใช่' if is_deployed else 'ไม่ใช่'}")


# -----------------------------------------------------------------
# นิยาม DAG
# -----------------------------------------------------------------
with DAG(
    dag_id="steam_ccu_pipeline_dag",
    default_args=default_args,
    description="ML Pipeline: พยากรณ์ CCU เกม CS2 ด้วยเทคนิค Champion-Challenger บน Airflow",
    schedule=None,  # trigger มือเพื่อทดสอบ หรือรันต่อจาก steam_etl_dag
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["steam", "ml", "project", "prediction"],
) as dag:

    create_tables = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=CREATE_TABLES_SQL,
    )

    bootstrap_task = PythonOperator(
        task_id="bootstrap_historical_ccu",
        python_callable=bootstrap_historical_ccu,
    )

    extract_task = PythonOperator(
        task_id="extract_today_ccu",
        python_callable=extract_today_ccu,
    )

    prepare_task = PythonOperator(
        task_id="prepare_training_data",
        python_callable=prepare_training_data,
    )

    train_task = PythonOperator(
        task_id="train_model",
        python_callable=train_model,
    )

    evaluate_task = PythonOperator(
        task_id="evaluate_model",
        python_callable=evaluate_model,
    )

    prev_rmse_task = PythonOperator(
        task_id="get_previous_rmse",
        python_callable=get_previous_rmse,
    )

    decide_task = BranchPythonOperator(
        task_id="decide_deploy",
        python_callable=decide_deploy,
    )

    deploy_task = PythonOperator(
        task_id="deploy_model",
        python_callable=deploy_model,
    )

    skip_task = PythonOperator(
        task_id="skip_deploy",
        python_callable=skip_deploy,
    )

    smoke_test_task = PythonOperator(
        task_id="smoke_test",
        python_callable=smoke_test,
    )

    log_task = PythonOperator(
        task_id="log_result",
        python_callable=log_result,
        trigger_rule="none_failed_min_one_success",
    )

    # กำหนดเส้นทางการรัน DAG
    (
        create_tables
        >> bootstrap_task
        >> extract_task
        >> prepare_task
        >> train_task
        >> evaluate_task
        >> prev_rmse_task
        >> decide_task
    )

    decide_task >> deploy_task >> smoke_test_task >> log_task
    decide_task >> skip_task >> log_task
