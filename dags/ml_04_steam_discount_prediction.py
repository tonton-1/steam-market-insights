"""
ml_04_steam_discount_prediction.py
====================================
Project: Steam Discount & Flash Sale Prediction ML Pipeline
ทำนายโอกาสการจัดโปรโมชั่นลดราคา (Discount Probability) และคาดการณ์ % ส่วนลดล่วงหน้า 7 วัน
พร้อมจัดหมวดหมู่คำแนะนำการซื้ออัจฉริยะ (Smart Buying Advisor)

เป้าหมายของไฟล์นี้:
- สร้างตาราง steam_discount_predictions บน PostgreSQL
- สกัด Feature: วันที่ไม่ได้ลดราคามาแล้ว (days_since_sale), ความถี่การลดราคา, ระดับราคา, คะแนนรีวิว, เทศกาล
- เทรนโมเดลคู่ (Dual-Model):
    1. RandomForestClassifier: ทำนายความน่าจะเป็นที่จะลดราคาใน 7 วัน (0.0 - 1.0)
    2. RandomForestRegressor: คาดการณ์อัตราส่วนลด % ที่จะเกิดขึ้น
- รัน Batch Inference ทำนายผลล่วงหน้า 7 วันสำหรับทุกเกมในตาราง steam_games
- บันทึกผลลัพธ์ลง PostgreSQL เพื่อแสดงผลบน Streamlit Dashboard (Tab 2)
"""

import math
import os
import random
from datetime import date, datetime, timedelta

import joblib
import pandas as pd
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.metrics import accuracy_score, mean_squared_error, roc_auc_score

# -----------------------------------------------------------------
# ค่าตั้งต้น
# -----------------------------------------------------------------
default_args = {
    "owner": "steam_project",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

POSTGRES_CONN_ID = "postgres_target"
MODEL_DIR = "/opt/airflow/models/discount_models"
CLF_MODEL_PATH = os.path.join(MODEL_DIR, "discount_classifier.pkl")
REG_MODEL_PATH = os.path.join(MODEL_DIR, "discount_regressor.pkl")

CREATE_PREDICTION_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS steam_discount_predictions (
    id                     SERIAL PRIMARY KEY,
    prediction_date        DATE NOT NULL,
    appid                  INT NOT NULL,
    game_name              VARCHAR(255) NOT NULL,
    current_price_thb      NUMERIC(10, 2),
    original_price         NUMERIC(10, 2),
    is_currently_on_sale   BOOLEAN DEFAULT FALSE,
    discount_probability   FLOAT NOT NULL,
    will_discount_7d       BOOLEAN NOT NULL,
    predicted_discount_pct INT DEFAULT 0,
    predicted_price_thb    NUMERIC(10, 2),
    recommendation         VARCHAR(50),
    confidence_level       VARCHAR(20),
    created_at             TIMESTAMP DEFAULT NOW(),
    UNIQUE (prediction_date, appid)
);
"""


# -----------------------------------------------------------------
# 1. Prepare Training Data: สร้างชุดข้อมูลสำหรับเทรนโมเดล
# -----------------------------------------------------------------
def prepare_discount_data(**kwargs):
    """
    ดึงประวัติราคาและส่วนลดจาก steam_games และสร้าง Features สำหรับทำนายการลดราคา:
    - days_since_last_sale: จำนวนวันที่ไม่ได้ลดราคา
    - original_price: ราคาเต็มของเกม
    - review_score: คะแนนรีวิวแง่บวก
    - ccu: จำนวนผู้เล่น
    - day_of_month, day_of_week, month: ปัจจัยด้านเวลาและเทศกาล
    """
    ti = kwargs["ti"]
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    query = """
    SELECT snapshot_date, appid, name, price_thb, original_price, discount_pct,
           ccu, review_score, is_free
    FROM steam_games
    WHERE is_free = FALSE AND original_price > 0
    ORDER BY appid, snapshot_date ASC;
    """
    rows = hook.get_records(query)
    print(f"📊 ดึงประวัติเกมที่เสียเงินมาได้ทั้งหมด {len(rows)} รายการ")

    if not rows:
        raise ValueError("ไม่พบข้อมูลเกมสำหรับสร้างชุดข้อมูล Train")

    df = pd.DataFrame(rows, columns=[
        "date", "appid", "name", "price_thb", "original_price",
        "discount_pct", "ccu", "review_score", "is_free"
    ])
    df["date"] = pd.to_datetime(df["date"])

    # สร้าง Feature Matrix และ Target สำหรับแต่ละเกม
    X_list, y_clf_list, y_reg_list = [], [], []

    for appid, group in df.groupby("appid"):
        group = group.sort_values("date").reset_index(drop=True)
        days_count = len(group)

        for i in range(days_count):
            row = group.iloc[i]
            cur_date = row["date"]

            # คำนวณว่าไม่ได้ลดราคามาแล้วกี่วันในอดีต (ประวัติย้อนหลัง)
            past = group.iloc[:i]
            sales_past = past[past["discount_pct"] > 0]
            if not sales_past.empty:
                last_sale_date = sales_past["date"].max()
                days_since_sale = (cur_date - last_sale_date).days
            else:
                days_since_sale = (i + 1) * 3  # ค่าสมมติหากยังไม่เคยเจอ

            # ค่าเฉลี่ยส่วนลดที่เคยได้รับ
            avg_past_disc = sales_past["discount_pct"].mean() if not sales_past.empty else 35.0

            # Features
            feat = [
                days_since_sale,
                float(row["original_price"]),
                float(row["review_score"] or 80.0),
                math.log1p(float(row["ccu"] or 1000)),
                cur_date.day,
                cur_date.weekday(),
                cur_date.month,
                avg_past_disc,
            ]

            # กำหนด Target (จะลดราคาใน 7 วันถัดไปหรือไม่)
            future = group.iloc[i + 1 : i + 8]
            if not future.empty:
                will_discount = 1 if any(future["discount_pct"] > 0) else 0
                max_future_disc = future["discount_pct"].max()
            else:
                # ปลายแถว: อิงจากลักษณะของ days_since_sale และรอบสัปดาห์
                prob_factor = min(1.0, (days_since_sale / 28.0) * (1.3 if cur_date.weekday() in [3, 4] else 0.9))
                will_discount = 1 if prob_factor > 0.55 else 0
                max_future_disc = int(avg_past_disc) if will_discount else 0

            X_list.append(feat)
            y_clf_list.append(will_discount)
            y_reg_list.append(max(0, int(max_future_disc)))

    # บันทึกลง XCom
    ti.xcom_push(key="X_data", value=X_list)
    ti.xcom_push(key="y_clf", value=y_clf_list)
    ti.xcom_push(key="y_reg", value=y_reg_list)
    print(f"✨ สกัด Feature สำเร็จทั้งหมด {len(X_list)} ตัวอย่าง")


# -----------------------------------------------------------------
# 2. Train Models: เทรน Classifier และ Regressor
# -----------------------------------------------------------------
def train_discount_models(**kwargs):
    """
    เทรนโมเดล 2 ตัว:
    1. Classifier: RandomForestClassifier -> ทำนายความน่าจะเป็นในการลดราคา (0.0 ถึง 1.0)
    2. Regressor: RandomForestRegressor -> คาดการณ์ % ส่วนลด
    """
    ti = kwargs["ti"]
    X = ti.xcom_pull(task_ids="prepare_discount_data", key="X_data")
    y_clf = ti.xcom_pull(task_ids="prepare_discount_data", key="y_clf")
    y_reg = ti.xcom_pull(task_ids="prepare_discount_data", key="y_reg")

    if not X or len(X) < 10:
        raise ValueError("ข้อมูลตัวอย่างไม่เพียงพอสำหรับการเทรนโมเดล")

    # แบ่ง Train / Holdout สำหรับการประเมิน
    split_idx = int(len(X) * 0.8)
    X_train, X_val = X[:split_idx], X[split_idx:]
    y_clf_train, y_clf_val = y_clf[:split_idx], y_clf[split_idx:]
    y_reg_train, y_reg_val = y_reg[:split_idx], y_reg[split_idx:]

    # 1. เทรน Classifier
    clf = RandomForestClassifier(n_estimators=100, max_depth=6, random_state=42)
    clf.fit(X_train, y_clf_train)
    val_preds_clf = clf.predict(X_val)
    clf_acc = accuracy_score(y_clf_val, val_preds_clf)

    # 2. เทรน Regressor (เทรนเฉพาะตัวอย่างที่มีส่วนลด > 0)
    reg_X_train = [x for x, y in zip(X_train, y_reg_train) if y > 0]
    reg_y_train = [y for y in y_reg_train if y > 0]
    if not reg_X_train:
        reg_X_train, reg_y_train = X_train, y_reg_train

    reg = RandomForestRegressor(n_estimators=100, max_depth=6, random_state=42)
    reg.fit(reg_X_train, reg_y_train)

    # บันทึกโมเดลลงไฟล์
    os.makedirs(MODEL_DIR, exist_ok=True)
    joblib.dump(clf, CLF_MODEL_PATH)
    joblib.dump(reg, REG_MODEL_PATH)

    print(f"🤖 เทรน Discount Classifier สำเร็จ (Validation Accuracy: {clf_acc:.2%})")
    print(f"🤖 เทรน Discount Regressor สำเร็จ")
    print(f"💾 บันทึกไฟล์โมเดลไว้ที่ {MODEL_DIR}")

    ti.xcom_push(key="clf_accuracy", value=clf_acc)


# -----------------------------------------------------------------
# 3. Batch Inference: ทำนายผลล่วงหน้า 7 วันทุกเกมในระบบ
# -----------------------------------------------------------------
def batch_predict_discounts(**kwargs):
    """
    โหลดโมเดลที่เทรนแล้ว มารันทำนายผลสำหรับทุกเกมในรอบล่าสุด
    และบันทึกผลลงตาราง steam_discount_predictions
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    today = date.today()

    if not os.path.exists(CLF_MODEL_PATH) or not os.path.exists(REG_MODEL_PATH):
        raise FileNotFoundError("ไม่พบไฟล์โมเดลที่บันทึกไว้")

    clf = joblib.load(CLF_MODEL_PATH)
    reg = joblib.load(REG_MODEL_PATH)

    # ดึงเกมรอบล่าสุดทั้งหมด
    query = """
    SELECT DISTINCT ON (appid)
           appid, name, price_thb, original_price, discount_pct, ccu, review_score, is_free, snapshot_date
    FROM steam_games
    ORDER BY appid, snapshot_date DESC;
    """
    latest_games = hook.get_records(query)
    print(f"🎯 กำลังรัน Batch Prediction สำหรับ {len(latest_games)} เกม...")

    inserted_count = 0

    for g in latest_games:
        appid, name, price_thb, orig_price, disc_pct, ccu, rev_score, is_free, snap_date = g
        price_thb = float(price_thb or 0)
        orig_price = float(orig_price or price_thb or 0)
        disc_pct = int(disc_pct or 0)
        is_on_sale = disc_pct > 0

        # ถ้าเป็นเกม Free to play
        if is_free or (orig_price == 0 and price_thb == 0):
            prob = 0.0
            will_discount = False
            pred_disc_pct = 0
            pred_price = 0.0
            recommendation = "FREE_TO_PLAY"
            confidence = "HIGH"
        elif is_on_sale:
            # ถ้าปัจจุบันกำลังลดราคาอยู่แล้ว
            prob = 0.95
            will_discount = True
            pred_disc_pct = disc_pct
            pred_price = price_thb
            recommendation = "BUY_NOW_ON_SALE"
            confidence = "HIGH"
        else:
            # คำนวณจำนวนวันที่ไม่ได้ลดราคา
            sale_history = hook.get_records(
                "SELECT snapshot_date FROM steam_games WHERE appid = %s AND discount_pct > 0 ORDER BY snapshot_date DESC LIMIT 1;",
                parameters=(appid,),
            )
            if sale_history:
                days_since_sale = (today - sale_history[0][0]).days
            else:
                days_since_sale = 25  # ค่าประมาณ

            # สร้าง Feature Input
            feat = [
                days_since_sale,
                orig_price,
                float(rev_score or 80.0),
                math.log1p(float(ccu or 1000)),
                today.day,
                today.weekday(),
                today.month,
                35.0,  # baseline avg discount
            ]

            # พยากรณ์ความน่าจะเป็นและส่วนลด
            prob_raw = clf.predict_proba([feat])[0]
            prob = float(prob_raw[1]) if len(prob_raw) > 1 else float(prob_raw[0])
            
            # ปรับสเกลให้อยู่ในช่วงที่สมเหตุสมผลตาม days_since_sale
            prob = min(0.92, max(0.08, prob + (days_since_sale / 60.0) * 0.15))
            
            will_discount = prob >= 0.50
            predicted_disc = int(round(reg.predict([feat])[0]))
            pred_disc_pct = max(10, min(85, predicted_disc if predicted_disc > 0 else 35))
            
            pred_price = round(orig_price * (1 - (pred_disc_pct / 100.0)), 2)

            # จัดระดับคำแนะนำ (Smart Recommendation)
            if prob >= 0.70:
                recommendation = "WAIT"         # แนะนำให้รอก่อน โอกาสลดสูง
                confidence = "HIGH" if prob >= 0.80 else "MEDIUM"
            elif prob >= 0.40:
                recommendation = "WATCH"        # จับตาดูราคา โอกาสลดปานกลาง
                confidence = "MEDIUM"
            else:
                recommendation = "BUY_NOW"      # ซื้อได้เลย โอกาสลดต่ำ
                confidence = "HIGH" if prob <= 0.25 else "MEDIUM"

        # บันทึกลง PostgreSQL (Upsert)
        upsert_sql = """
        INSERT INTO steam_discount_predictions (
            prediction_date, appid, game_name, current_price_thb, original_price,
            is_currently_on_sale, discount_probability, will_discount_7d,
            predicted_discount_pct, predicted_price_thb, recommendation, confidence_level, created_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
        ) ON CONFLICT (prediction_date, appid) DO UPDATE SET
            current_price_thb = EXCLUDED.current_price_thb,
            original_price = EXCLUDED.original_price,
            is_currently_on_sale = EXCLUDED.is_currently_on_sale,
            discount_probability = EXCLUDED.discount_probability,
            will_discount_7d = EXCLUDED.will_discount_7d,
            predicted_discount_pct = EXCLUDED.predicted_discount_pct,
            predicted_price_thb = EXCLUDED.predicted_price_thb,
            recommendation = EXCLUDED.recommendation,
            confidence_level = EXCLUDED.confidence_level,
            created_at = NOW();
        """
        hook.run(
            upsert_sql,
            parameters=(
                today, appid, name, price_thb, orig_price,
                is_on_sale, prob, will_discount, pred_disc_pct,
                pred_price, recommendation, confidence
            ),
        )
        inserted_count += 1

    print(f"💾 บันทึกผลพยากรณ์สำเร็จทั้งหมด {inserted_count} เกม สำหรับวันที่ {today}")


# -----------------------------------------------------------------
# 4. นิยาม DAG
# -----------------------------------------------------------------
with DAG(
    dag_id="steam_discount_prediction_dag",
    default_args=default_args,
    description="MLOps Pipeline: พยากรณ์โอกาสลดราคาและอัตราส่วนลดล่วงหน้า 7 วันบน Steam",
    schedule="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["steam", "ml", "discount", "prediction", "deals"],
) as dag:

    start = EmptyOperator(task_id="start")

    create_table = PostgresOperator(
        task_id="create_prediction_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=CREATE_PREDICTION_TABLE_SQL,
    )

    prepare_data = PythonOperator(
        task_id="prepare_discount_data",
        python_callable=prepare_discount_data,
    )

    train_models = PythonOperator(
        task_id="train_discount_models",
        python_callable=train_discount_models,
    )

    batch_predict = PythonOperator(
        task_id="batch_predict_discounts",
        python_callable=batch_predict_discounts,
    )

    end = EmptyOperator(task_id="end")

    # ลำดับการทำงาน (DAG Workflow)
    start >> create_table >> prepare_data >> train_models >> batch_predict >> end
