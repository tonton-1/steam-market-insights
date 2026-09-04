"""
07_etl_steam.py
================
Project: Steam Games Analytics & Price Intelligence Pipeline
Apache Airflow Pipeline สำหรับดึงข้อมูลเกมยอดนิยมบน Steam, ราคา (บาทไทย), ส่วนลด, และจำนวนผู้เล่น (CCU)

เป้าหมายของไฟล์นี้:
- ดึงข้อมูล Top Games จาก SteamSpy API และ Steam Store API
- ETL ครบวงจร: Extract -> Enrich -> Transform -> Load ลง PostgreSQL
- ตรวจจับ Price Changes & Flash Sales บันทึกลงตาราง steam_price_events
- ป้อนข้อมูลเข้าตาราง steam_games เพื่อใช้สำหรับ Dashboard และ ML Pipeline
"""

import time
from datetime import date, datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

# -----------------------------------------------------------------
# ค่าตั้งต้น
# -----------------------------------------------------------------
default_args = {
    "owner": "steam_project",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

POSTGRES_CONN_ID = "postgres_target"

# รายการเกมเด่นที่ต้องการติดตามเจาะลึก
TRACKED_GAMES = [
    {"appid": 730, "name": "Counter-Strike 2"},
    {"appid": 578080, "name": "PUBG: BATTLEGROUNDS"},
    {"appid": 1172470, "name": "Apex Legends"},
    {"appid": 1245620, "name": "ELDEN RING"},
    {"appid": 1086940, "name": "Baldur's Gate 3"},
    {"appid": 2358720, "name": "Black Myth: Wukong"},
    {"appid": 2246340, "name": "Monster Hunter Wilds"},
    {"appid": 553850, "name": "HELLDIVERS 2"},
    {"appid": 1623730, "name": "Palworld"},
    {"appid": 105600, "name": "Terraria"},
    {"appid": 440, "name": "Team Fortress 2"},
    {"appid": 271590, "name": "Grand Theft Auto V"},
    {"appid": 550, "name": "Left 4 Dead 2"},
    {"appid": 236390, "name": "War Thunder"},
    {"appid": 230410, "name": "Warframe"},
    {"appid": 2694490, "name": "Path of Exile 2"},
    {"appid": 431960, "name": "Wallpaper Engine"},
    {"appid": 291550, "name": "Brawlhalla"},
    {"appid": 252490, "name": "Rust"},
    {"appid": 359550, "name": "Tom Clancy's Rainbow Six Siege"},
    {"appid": 1091500, "name": "Cyberpunk 2077"},
    {"appid": 1145360, "name": "Hades"},
    {"appid": 814380, "name": "Sekiro: Shadows Die Twice"},
    {"appid": 1174180, "name": "Red Dead Redemption 2"},
    {"appid": 289070, "name": "Sid Meier's Civilization VI"},
]

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

CREATE TABLE IF NOT EXISTS steam_price_events (
    id           SERIAL PRIMARY KEY,
    appid        INT NOT NULL,
    game_name    VARCHAR(255),
    event_date   DATE NOT NULL,
    old_price    NUMERIC(10, 2),
    new_price    NUMERIC(10, 2),
    discount_pct INT,
    event_type   VARCHAR(20),
    detected_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS steam_model_metrics (
    id          SERIAL PRIMARY KEY,
    model_name  VARCHAR(50) NOT NULL,
    target_game VARCHAR(100) NOT NULL,
    rmse        FLOAT NOT NULL,
    deployed    BOOLEAN NOT NULL,
    run_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS steam_game_regions (
    id                SERIAL PRIMARY KEY,
    snapshot_date     DATE NOT NULL,
    appid             INT NOT NULL,
    game_name         VARCHAR(255) NOT NULL,
    region_code       VARCHAR(50) NOT NULL,
    region_name       VARCHAR(100) NOT NULL,
    country_iso3      VARCHAR(10) NOT NULL,
    country_name      VARCHAR(100) NOT NULL,
    player_count      INT NOT NULL DEFAULT 0,
    share_percentage  NUMERIC(5, 2) NOT NULL,
    peak_time_bkk     VARCHAR(50),
    created_at        TIMESTAMP DEFAULT NOW(),
    UNIQUE (snapshot_date, appid, region_code, country_iso3)
);
"""


# -----------------------------------------------------------------
# 1. Extract: ดึงข้อมูล Top Games จาก SteamSpy
# -----------------------------------------------------------------
def extract_steam_data(**kwargs):
    """
    ดึงข้อมูลเกมยอดนิยมจาก SteamSpy API (top100in2weeks)
    และดึงข้อมูลเฉพาะของเกมใน TRACKED_GAMES
    """
    ti = kwargs["ti"]
    print("📡 กำลังดึงข้อมูลจาก SteamSpy API...")

    top100_map = {}
    try:
        url = "https://steamspy.com/api.php?request=top100in2weeks"
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        top100_map = resp.json()
        print(f"✅ ดึง Top 100 จาก SteamSpy สำเร็จ: พบ {len(top100_map)} เกม")
    except Exception as e:
        print(f"⚠️ ดึง top100in2weeks ไม่สำเร็จ ({e}) จะใช้ individual requests แทน")

    extracted_games = []
    tracked_appids = {g["appid"] for g in TRACKED_GAMES}

    # รวมเกมจาก TRACKED_GAMES
    for item in TRACKED_GAMES:
        appid = item["appid"]
        fallback_name = item["name"]

        if str(appid) in top100_map:
            raw = top100_map[str(appid)]
        else:
            try:
                single_url = f"https://steamspy.com/api.php?request=appdetails&appid={appid}"
                resp = requests.get(single_url, timeout=10)
                raw = resp.json() if resp.status_code == 200 else {}
                time.sleep(0.5)
            except Exception:
                raw = {}

        name = raw.get("name") or fallback_name
        dev = raw.get("developer", "Unknown")
        pub = raw.get("publisher", "Unknown")
        ccu = int(raw.get("ccu", 0) or 0)
        pos = int(raw.get("positive", 0) or 0)
        neg = int(raw.get("negative", 0) or 0)
        total_rev = pos + neg
        score = round((pos / total_rev) * 100, 2) if total_rev > 0 else 0.0

        raw_price = int(raw.get("price", 0) or 0)
        raw_initial = int(raw.get("initialprice", 0) or 0)
        discount = int(raw.get("discount", 0) or 0)

        # SteamSpy ราคาหน่วยเป็น cent USD แปลงเป็น THB โดยประมาณ (~33 THB/USD)
        # จะถูก refine ด้วย Steam Store API ใน task ถัดไป
        price_thb = round((raw_price / 100.0) * 33.0, 2)
        orig_thb = round((raw_initial / 100.0) * 33.0, 2)

        is_free = (raw_price == 0 and raw_initial == 0)

        extracted_games.append({
            "appid": appid,
            "name": name,
            "developer": dev,
            "publisher": pub,
            "price_thb": price_thb,
            "original_price": orig_thb,
            "discount_pct": discount,
            "ccu": ccu,
            "positive_reviews": pos,
            "negative_reviews": neg,
            "review_score": score,
            "owners_estimate": raw.get("owners", "Unknown"),
            "is_free": is_free,
            "genres": "",
            "header_image": f"https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/{appid}/header.jpg",
        })

    print(f"📦 รวมข้อมูลดึงเบื้องต้นได้ {len(extracted_games)} เกม")
    ti.xcom_push(key="extracted_games", value=extracted_games)


# -----------------------------------------------------------------
# 2. Enrich: เสริมข้อมูลราคาบาทแท้ + หมวดหมู่ + ภาพปก จาก Steam Store API
# -----------------------------------------------------------------
def enrich_steam_store(**kwargs):
    """
    เรียก Steam Store API (cc=th, l=thai) เพื่อเอา:
    1. ราคาบาทไทยแท้จริง (THB)
    2. % ส่วนลดทางการ
    3. หมวดหมู่เกม (genres)
    4. ลิงก์ภาพปกเกมคมชัด (header_image)
    """
    ti = kwargs["ti"]
    games = ti.xcom_pull(task_ids="extract_steam_data", key="extracted_games")
    if not games:
        print("⚠️ ไม่มีข้อมูลเกมจาก task ก่อนหน้า")
        return

    enriched_list = []
    print("🔍 กำลังดึงข้อมูลเสริมจาก Steam Store API (cc=th)...")

    for i, g in enumerate(games):
        appid = g["appid"]
        store_url = f"https://store.steampowered.com/api/appdetails?appids={appid}&cc=th&l=thai"

        try:
            resp = requests.get(store_url, timeout=10)
            if resp.status_code == 200:
                body = resp.json()
                app_info = body.get(str(appid), {})
                if app_info.get("success"):
                    data = app_info.get("data", {})
                    g["name"] = data.get("name", g["name"])
                    g["is_free"] = data.get("is_free", g["is_free"])

                    # Genres
                    genres_list = [item["description"] for item in data.get("genres", [])]
                    g["genres"] = ", ".join(genres_list)

                    # Header image
                    if data.get("header_image"):
                        g["header_image"] = data["header_image"]

                    # Price Overview (ราคาบาทจริงในไทย)
                    if data.get("is_free"):
                        g["price_thb"] = 0.0
                        g["original_price"] = 0.0
                        g["discount_pct"] = 0
                    elif "price_overview" in data:
                        p_info = data["price_overview"]
                        g["price_thb"] = round(p_info.get("final", 0) / 100.0, 2)
                        g["original_price"] = round(p_info.get("initial", 0) / 100.0, 2)
                        g["discount_pct"] = int(p_info.get("discount_percent", 0))
                    elif not data.get("is_free") and g["price_thb"] == 0.0:
                        # Fallback สำหรับเกมที่ขายเฉพาะใน Bundle เช่น GTA V Premium Edition
                        if appid == 271590:
                            g["price_thb"] = 704.00
                            g["original_price"] = 704.00
                            g["discount_pct"] = 0

                    print(f"  [{i+1}/{len(games)}] ✅ {g['name']}: ฿{g['price_thb']} (ลด {g['discount_pct']}%) | CCU: {g['ccu']:,}")
                else:
                    print(f"  [{i+1}/{len(games)}] ℹ️ Steam Store success=false สำหรับ appid {appid}")
            else:
                print(f"  [{i+1}/{len(games)}] ⚠️ Steam Store API status {resp.status_code}")
        except Exception as err:
            print(f"  [{i+1}/{len(games)}] ⚠️ ดึง Steam Store appid {appid} ผิดพลาด: {err}")

        enriched_list.append(g)
        # หน่วงเวลาสั้นๆ ป้องกัน rate limit
        time.sleep(0.8)

    ti.xcom_push(key="enriched_games", value=enriched_list)
    print(f"✨ Enrich ข้อมูลสำเร็จทั้งหมด {len(enriched_list)} เกม")


# -----------------------------------------------------------------
# 3. Load: บันทึกข้อมูลลง PostgreSQL แบบ Idempotent
# -----------------------------------------------------------------
def load_to_postgres(**kwargs):
    """บันทึกข้อมูล snapshot ประจำวันลงตาราง steam_games"""
    ti = kwargs["ti"]
    games = ti.xcom_pull(task_ids="enrich_steam_store", key="enriched_games")
    if not games:
        print("⚠️ ไม่มีข้อมูลสำหรับบันทึกลง Postgres")
        return

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    today = date.today().isoformat()
    inserted_count = 0

    insert_sql = """
    INSERT INTO steam_games (
        snapshot_date, appid, name, developer, publisher,
        price_thb, original_price, discount_pct, ccu,
        positive_reviews, negative_reviews, review_score,
        owners_estimate, genres, header_image, is_free
    ) VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s, %s
    )
    ON CONFLICT (snapshot_date, appid) DO UPDATE SET
        name = EXCLUDED.name,
        developer = EXCLUDED.developer,
        publisher = EXCLUDED.publisher,
        price_thb = EXCLUDED.price_thb,
        original_price = EXCLUDED.original_price,
        discount_pct = EXCLUDED.discount_pct,
        ccu = EXCLUDED.ccu,
        positive_reviews = EXCLUDED.positive_reviews,
        negative_reviews = EXCLUDED.negative_reviews,
        review_score = EXCLUDED.review_score,
        owners_estimate = EXCLUDED.owners_estimate,
        genres = EXCLUDED.genres,
        header_image = EXCLUDED.header_image,
        is_free = EXCLUDED.is_free,
        ingested_at = NOW();
    """

    for g in games:
        hook.run(
            insert_sql,
            parameters=(
                today,
                g["appid"],
                g["name"],
                g["developer"],
                g["publisher"],
                g["price_thb"],
                g["original_price"],
                g["discount_pct"],
                g["ccu"],
                g["positive_reviews"],
                g["negative_reviews"],
                g["review_score"],
                g["owners_estimate"],
                g["genres"],
                g["header_image"],
                g["is_free"],
            ),
        )
        inserted_count += 1

    print(f"💾 บันทึก/อัปเดตข้อมูลเกม {inserted_count} แถว ประจำวันที่ {today} สำเร็จ!")


# -----------------------------------------------------------------
# 4. Detect Events: ตรวจจับการเปลี่ยนแปลงของราคาและส่วนลด
# -----------------------------------------------------------------
def detect_price_events(**kwargs):
    """
    เปรียบเทียบ snapshot ของวันนี้ กับวันล่าสุดที่มีก่อนหน้า
    เพื่อตรวจหาเกมที่เริ่มลดราคา (sale_started), หมดเวลาลด (sale_ended), หรือเปลี่ยนราคา
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    today = date.today().isoformat()

    query = """
    WITH current_day AS (
        SELECT appid, name, price_thb, discount_pct
        FROM steam_games
        WHERE snapshot_date = %s
    ),
    previous_day AS (
        SELECT DISTINCT ON (appid) appid, price_thb, discount_pct
        FROM steam_games
        WHERE snapshot_date < %s
        ORDER BY appid, snapshot_date DESC
    )
    SELECT
        c.appid,
        c.name,
        p.price_thb AS old_price,
        c.price_thb AS new_price,
        c.discount_pct
    FROM current_day c
    JOIN previous_day p ON c.appid = p.appid
    WHERE c.price_thb != p.price_thb OR c.discount_pct != p.discount_pct;
    """

    changed_rows = hook.get_records(query, parameters=(today, today))
    if not changed_rows:
        print("ℹ️ ไม่พบการเปลี่ยนแปลงของราคาเมื่อเทียบกับวันก่อนหน้า")
        return

    event_count = 0
    for row in changed_rows:
        appid, name, old_p, new_p, disc = row
        old_p = float(old_p or 0)
        new_p = float(new_p or 0)
        disc = int(disc or 0)

        if new_p < old_p:
            ev_type = "sale_started"
        elif new_p > old_p:
            ev_type = "sale_ended"
        else:
            ev_type = "price_change"

        hook.run(
            """
            INSERT INTO steam_price_events (
                appid, game_name, event_date, old_price, new_price, discount_pct, event_type
            ) VALUES (%s, %s, %s, %s, %s, %s, %s);
            """,
            parameters=(appid, name, today, old_p, new_p, disc, ev_type),
        )
        event_count += 1
        print(f"🔥 Event [{ev_type}]: {name} | ฿{old_p} -> ฿{new_p} (ลด {disc}%)")

    print(f"🔔 บันทึก Price Events ทั้งหมด {event_count} รายการ")


# -----------------------------------------------------------------
# 5. Regional Distribution: คำนวณสัดส่วนผู้เล่นแยกตามโซนและประเทศ
# -----------------------------------------------------------------
def calculate_regional_distribution(**kwargs):
    """
    วิเคราะห์และกระจายสัดส่วนผู้เล่น CCU ไปยังภูมิภาคและประเทศหลักทั่วโลก
    ตามลักษณะของเกม (Game Archetypes) และ Timezone Traffic ของ Steam
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    today = date.today().isoformat()

    games = hook.get_records(
        "SELECT appid, name, ccu FROM steam_games WHERE snapshot_date = %s;",
        parameters=(today,),
    )
    if not games:
        print("ℹ️ ไม่พบข้อมูลเกมของวันนี้สำหรับคำนวณ Regional Distribution")
        return

    # รายชื่อประเทศและสัดส่วนในแต่ละภูมิภาค
    COUNTRY_TEMPLATES = [
        # East Asia
        {"code": "ASIA_EAST", "reg_name": "เอเชียตะวันออก", "iso": "CHN", "name": "จีน", "base_pct": 0.28, "peak": "19:00 - 23:00 น."},
        {"code": "ASIA_EAST", "reg_name": "เอเชียตะวันออก", "iso": "JPN", "name": "ญี่ปุ่น", "base_pct": 0.05, "peak": "19:00 - 23:00 น."},
        {"code": "ASIA_EAST", "reg_name": "เอเชียตะวันออก", "iso": "KOR", "name": "เกาหลีใต้", "base_pct": 0.04, "peak": "19:00 - 23:00 น."},
        {"code": "ASIA_EAST", "reg_name": "เอเชียตะวันออก", "iso": "TWN", "name": "ไต้หวัน", "base_pct": 0.02, "peak": "19:00 - 23:00 น."},
        # Southeast Asia
        {"code": "ASIA_SEA", "reg_name": "เอเชียตะวันออกเฉียงใต้", "iso": "THA", "name": "ประเทศไทย", "base_pct": 0.045, "peak": "20:00 - 00:00 น."},
        {"code": "ASIA_SEA", "reg_name": "เอเชียตะวันออกเฉียงใต้", "iso": "VNM", "name": "เวียดนาม", "base_pct": 0.035, "peak": "20:00 - 00:00 น."},
        {"code": "ASIA_SEA", "reg_name": "เอเชียตะวันออกเฉียงใต้", "iso": "IDN", "name": "อินโดนีเซีย", "base_pct": 0.030, "peak": "20:00 - 00:00 น."},
        {"code": "ASIA_SEA", "reg_name": "เอเชียตะวันออกเฉียงใต้", "iso": "PHL", "name": "ฟิลิปปินส์", "base_pct": 0.025, "peak": "20:00 - 00:00 น."},
        {"code": "ASIA_SEA", "reg_name": "เอเชียตะวันออกเฉียงใต้", "iso": "MYS", "name": "มาเลเซีย", "base_pct": 0.015, "peak": "20:00 - 00:00 น."},
        {"code": "ASIA_SEA", "reg_name": "เอเชียตะวันออกเฉียงใต้", "iso": "SGP", "name": "สิงคโปร์", "base_pct": 0.010, "peak": "20:00 - 00:00 น."},
        # Europe
        {"code": "EUROPE", "reg_name": "ยุโรป", "iso": "RUS", "name": "รัสเซีย", "base_pct": 0.12, "peak": "22:00 - 02:00 น."},
        {"code": "EUROPE", "reg_name": "ยุโรป", "iso": "DEU", "name": "เยอรมนี", "base_pct": 0.06, "peak": "00:00 - 04:00 น."},
        {"code": "EUROPE", "reg_name": "ยุโรป", "iso": "GBR", "name": "สหราชอาณาจักร", "base_pct": 0.04, "peak": "01:00 - 05:00 น."},
        {"code": "EUROPE", "reg_name": "ยุโรป", "iso": "FRA", "name": "ฝรั่งเศส", "base_pct": 0.03, "peak": "00:00 - 04:00 น."},
        {"code": "EUROPE", "reg_name": "ยุโรป", "iso": "POL", "name": "โปแลนด์", "base_pct": 0.03, "peak": "00:00 - 04:00 น."},
        # North America
        {"code": "NORTH_AMERICA", "reg_name": "อเมริกาเหนือ", "iso": "USA", "name": "สหรัฐอเมริกา", "base_pct": 0.11, "peak": "07:00 - 11:00 น."},
        {"code": "NORTH_AMERICA", "reg_name": "อเมริกาเหนือ", "iso": "CAN", "name": "แคนาดา", "base_pct": 0.02, "peak": "07:00 - 11:00 น."},
        # Latin America
        {"code": "LATIN_AMERICA", "reg_name": "ลาตินอเมริกา", "iso": "BRA", "name": "บราซิล", "base_pct": 0.04, "peak": "05:00 - 09:00 น."},
        {"code": "LATIN_AMERICA", "reg_name": "ลาตินอเมริกา", "iso": "ARG", "name": "อาร์เจนตินา", "base_pct": 0.015, "peak": "05:00 - 09:00 น."},
        # Oceania
        {"code": "OCEANIA", "reg_name": "โอเชียเนีย", "iso": "AUS", "name": "ออสเตรเลีย", "base_pct": 0.015, "peak": "16:00 - 20:00 น."},
    ]

    total_inserted = 0

    for appid, name, total_ccu in games:
        total_ccu = int(total_ccu or 0)
        if total_ccu <= 0:
            continue

        # ปรับสัดส่วนตามลักษณะเด่นของเกม
        multiplier = {}
        if appid == 2358720:  # Black Myth: Wukong (Asia Heavy)
            multiplier = {"CHN": 2.6, "JPN": 1.2, "THA": 1.5, "USA": 0.4, "DEU": 0.3, "RUS": 0.3}
        elif appid == 730:    # CS2 (Europe / CIS Heavy)
            multiplier = {"RUS": 1.8, "POL": 1.7, "DEU": 1.4, "CHN": 1.1, "THA": 0.9, "USA": 0.8}
        elif appid == 578080: # PUBG (Asia Heavy)
            multiplier = {"CHN": 2.0, "KOR": 2.2, "THA": 1.8, "VNM": 2.0, "USA": 0.5, "DEU": 0.5}
        elif appid == 271590: # GTA V (Global Balanced)
            multiplier = {"USA": 1.6, "BRA": 1.8, "GBR": 1.4, "DEU": 1.3, "THA": 1.2, "CHN": 0.7}

        # Normalize weights
        adjusted_weights = []
        for c in COUNTRY_TEMPLATES:
            iso = c["iso"]
            mult = multiplier.get(iso, 1.0)
            adjusted_weights.append(c["base_pct"] * mult)

        sum_w = sum(adjusted_weights)
        norm_weights = [w / sum_w for w in adjusted_weights]

        insert_sql = """
        INSERT INTO steam_game_regions (
            snapshot_date, appid, game_name, region_code, region_name,
            country_iso3, country_name, player_count, share_percentage, peak_time_bkk, created_at
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, NOW()
        ) ON CONFLICT (snapshot_date, appid, region_code, country_iso3) DO UPDATE SET
            player_count = EXCLUDED.player_count,
            share_percentage = EXCLUDED.share_percentage,
            peak_time_bkk = EXCLUDED.peak_time_bkk;
        """

        for c, w in zip(COUNTRY_TEMPLATES, norm_weights):
            player_cnt = int(round(total_ccu * w))
            share_pct = round(w * 100.0, 2)

            hook.run(
                insert_sql,
                parameters=(
                    today, appid, name, c["code"], c["reg_name"],
                    c["iso"], c["name"], player_cnt, share_pct, c["peak"]
                ),
            )
            total_inserted += 1

    print(f"🌍 คำนวณและบันทึกสถิติผู้เล่นแยกตามโซน/ประเทศ {total_inserted} รายการ สำเร็จ!")


# -----------------------------------------------------------------
# 6. Verify: สรุปผลการทำงานของ Pipeline
# -----------------------------------------------------------------
def verify_pipeline(**kwargs):
    """ตรวจสอบความถูกต้องของข้อมูลในตาราง steam_games"""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    today = date.today().isoformat()

    total_today = hook.get_first(
        "SELECT COUNT(*) FROM steam_games WHERE snapshot_date = %s;",
        parameters=(today,),
    )[0]

    top_ccu = hook.get_records(
        """
        SELECT name, ccu, price_thb, discount_pct
        FROM steam_games
        WHERE snapshot_date = %s
        ORDER BY ccu DESC
        LIMIT 5;
        """,
        parameters=(today,),
    )

    print("\n==================================================")
    print(f"🎮 สรุปรายงาน Steam ETL ประจำวันที่ {today}")
    print(f"📊 จำนวนเกมที่บันทึกสำเร็จวันนี้: {total_today} เกม")
    print("🏆 Top 5 เกมที่มีผู้เล่นพร้อมกันสูงสุด (CCU):")
    for rank, (name, ccu, price, disc) in enumerate(top_ccu, 1):
        price_str = "ฟรี" if price == 0 else f"฿{price:,.2f}"
        disc_str = f" (ลด {disc}%)" if disc > 0 else ""
        print(f"   #{rank} {name} — {ccu:,} คน | {price_str}{disc_str}")
    print("==================================================\n")


# -----------------------------------------------------------------
# นิยาม DAG
# -----------------------------------------------------------------
with DAG(
    dag_id="steam_etl_dag",
    default_args=default_args,
    description="ETL Pipeline: ดึงราคา, ส่วนลด, และจำนวนผู้เล่นเกม Steam เข้า PostgreSQL ทุกวัน",
    schedule="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["steam", "etl", "project", "gaming"],
) as dag:

    start = EmptyOperator(task_id="start")

    create_tables = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=CREATE_TABLES_SQL,
    )

    extract_task = PythonOperator(
        task_id="extract_steam_data",
        python_callable=extract_steam_data,
    )

    enrich_task = PythonOperator(
        task_id="enrich_steam_store",
        python_callable=enrich_steam_store,
    )

    load_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    events_task = PythonOperator(
        task_id="detect_price_events",
        python_callable=detect_price_events,
    )

    regions_task = PythonOperator(
        task_id="calculate_regional_distribution",
        python_callable=calculate_regional_distribution,
    )

    verify_task = PythonOperator(
        task_id="verify_pipeline",
        python_callable=verify_pipeline,
    )

    # กำหนดลำดับ Task:
    # start -> [create_tables, extract_task] -> enrich -> load -> [events, regions] -> verify
    start >> [create_tables, extract_task]
    [create_tables, extract_task] >> enrich_task >> load_task
    load_task >> [events_task, regions_task] >> verify_task

