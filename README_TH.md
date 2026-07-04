# 🎬 Netflix Data Engineering Pipeline (ไปป์ไลน์วิศวกรรมข้อมูล Netflix)

> **ไปป์ไลน์ข้อมูลพร้อมใช้งานจริงที่ใช้สถาปัตยกรรม Medallion (Bronze → Silver → Gold) สำหรับการวิเคราะห์เนื้อหา Netflix**

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)](https://databricks.com)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=flat&logo=apachespark&logoColor=white)](https://spark.apache.org)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?style=flat&logo=delta&logoColor=white)](https://delta.io)
[![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)](https://python.org)

---

## 📋 สารบัญ

- [ภาพรวม](#ภาพรวม)
- [สถาปัตยกรรม](#สถาปัตยกรรม)
- [โครงสร้างโปรเจค](#โครงสร้างโปรเจค)
- [ส่วนประกอบของไปป์ไลน์](#ส่วนประกอบของไปป์ไลน์)
- [โครงสร้างตาราง](#โครงสร้างตาราง)
- [เริ่มต้นใช้งาน](#เริ่มต้นใช้งาน)
- [ระบบทดสอบ](#ระบบทดสอบ)
- [ตัวอย่างการใช้งาน](#ตัวอย่างการใช้งาน)
- [ประสิทธิภาพ](#ประสิทธิภาพ)
- [แนวทางปฏิบัติที่ดี](#แนวทางปฏิบัติที่ดี)
- [การแก้ไขปัญหา](#การแก้ไขปัญหา)
- [การมีส่วนร่วม](#การมีส่วนร่วม)

---

## 🎯 ภาพรวม

โปรเจคนี้พัฒนา**ไปป์ไลน์ข้อมูลที่พร้อมใช้งานจริงและขยายขนาดได้**สำหรับการประมวลผลข้อมูลเนื้อหา Netflix โดยใช้ Databricks และ**สถาปัตยกรรม Medallion** ไปป์ไลน์นี้รับข้อมูล CSV แบบดิบ นำไปตรวจสอบคุณภาพข้อมูลอย่างครอบคลุม และแปลงเป็น**โครงสร้าง Star Schema** ที่เหมาะสมสำหรับการวิเคราะห์และ Business Intelligence

### ฟีเจอร์หลัก

✅ **สถาปัตยกรรม Medallion**: ชั้น Bronze (ดิบ) → Silver (สะอาด) → Gold (รวมกลุ่ม)  
✅ **ตรวจสอบคุณภาพข้อมูล**: ไปป์ไลน์ตรวจสอบคุณภาพ 8 ขั้นตอนพร้อมติดตามข้อมูลไม่ถูกต้อง  
✅ **SCD Type 2**: ติดตามประวัติการเปลี่ยนแปลงพร้อมความถูกต้องเชิงเวลา  
✅ **Star Schema**: 1 มิติหลัก + 4 มิติย่อย + 4 ตาราง Bridge  
✅ **ตรวจจับการเปลี่ยนแปลงด้วย Hash**: การระบุความแตกต่างที่มีประสิทธิภาพ  
✅ **ประมวลผลแบบเพิ่มหน่วย**: Change Data Feed (CDF) เพื่อประสิทธิภาพ  
✅ **การทดสอบครอบคลุม**: ชุดทดสอบอัตโนมัติ 5 ชุดผ่านทั้งหมด 100%  
✅ **ประสิทธิภาพระดับ Production**: ประมวลผล 317+ รายการต่อวินาที  

### กรณีการใช้งานทางธุรกิจ

- 📊 **การวิเคราะห์เนื้อหา**: วิเคราะห์แนวโน้มแคตตาล็อก Netflix, ประเภท และรูปแบบการเผยแพร่
- 🎭 **การวิเคราะห์บุคลากร**: ติดตามนักแสดง ผู้กำกับ และเครือข่ายความร่วมมือ
- 🌍 **การกระจายตามภูมิศาสตร์**: ศึกษาความพร้อมของเนื้อหาในแต่ละประเทศ
- 📈 **การวิเคราะห์อนุกรมเวลา**: ติดตามการเพิ่มและการเปลี่ยนแปลงเนื้อหาตามเวลา
- 🔍 **ติดตามคุณภาพข้อมูล**: ติดตามความสมบูรณ์และเมตริกการตรวจสอบข้อมูล

---

## 🏗️ สถาปัตยกรรม

### รูปแบบสถาปัตยกรรม Medallion

```
┌─────────────────────────────────────────────────────────────────────┐
│                         ไปป์ไลน์การไหลของข้อมูล                        │
└─────────────────────────────────────────────────────────────────────┘

  📁 ไฟล์ต้นทาง              🥉 ชั้น Bronze                   🥈 ชั้น Silver                   🥇 ชั้น Gold
  ─────────────             ───────────────                ───────────────                  ─────────────
       CSV                            │                            │                              │
       JSON          ──────►    คลังข้อมูลดิบ      ──────►      Star Schema        ──────►      การรวมกลุ่ม
      Parquet                   + Metadata                   + ตรวจสอบคุณภาพ                + การวิเคราะห์
                                + เปิด CDF                    + SCD Type 2                   + Metrics
                                                             + Normalization

  ┌────────────┐              ┌────────────┐              ┌────────────────┐              ┌────────────┐
  │ netflix.csv│              │  netflix   │              │   dim_titles   │              │  Dashboard │
  │            │  ─────────►  │   _bronze  │  ─────────►  │ + 4 sub-dims   │  ─────────►  │    KPIs    │
  │ (17K แถว)  │              │            │              │ + 4 bridges    │              │  รายงาน    │
  └────────────┘              └────────────┘              │ + bad_records  │              └────────────┘
                                     ▲                    └────────────────┘
                                     │
                               config_table
                              (การตั้งค่าไปป์ไลน์)
```

### ความรับผิดชอบของแต่ละชั้น

#### 🥉 **ชั้น Bronze** - การรับข้อมูลดิบ
- **วัตถุประสงค์**: จุดลงจอดสำหรับข้อมูลภายนอก
- **ลักษณะเฉพาะ**: ไม่เปลี่ยนแปลง, เพิ่มเติมเท่านั้น, schema-on-read
- **ส่วนประกอบ**: คลาส `BronzeLayer`
- **ฟีเจอร์**:
  - ติดตาม metadata ของไฟล์ (`_load_dt`, `_file_name`, `_file_path`, ฯลฯ)
  - เปิดใช้งาน Change Data Feed (CDF)
  - รองรับรูปแบบ CSV, JSON, Parquet
  - ปรับแต่งได้ผ่าน `config_table`

#### 🥈 **ชั้น Silver** - คุณภาพข้อมูลและ Normalization
- **วัตถุประสงค์**: ข้อมูลที่สะอาด ตรวจสอบแล้ว พร้อมใช้งานทางธุรกิจ
- **ลักษณะเฉพาะ**: Normalized, ลบข้อมูลซ้ำ, ตรวจสอบแล้ว
- **ส่วนประกอบ**: คลาส `SilverLayer`
- **ฟีเจอร์**:
  - ไปป์ไลน์ตรวจสอบคุณภาพข้อมูล 8 ขั้นตอน
  - Star schema ทั้งหมด 9 ตาราง
  - ติดตามประวัติ SCD Type 2
  - ตรวจจับการเปลี่ยนแปลงด้วย Hash
  - บันทึกตรวจสอบข้อมูลไม่ถูกต้อง

#### 🥇 **ชั้น Gold** - การวิเคราะห์และการรวมกลุ่ม
- **วัตถุประสงค์**: การรวมกลุ่มและเมตริกเฉพาะทางธุรกิจ
- **ลักษณะเฉพาะ**: Denormalized, รวมกลุ่มไว้ล่วงหน้า, เหมาะสำหรับ query
- **ตัวอย่าง**: การเพิ่มเนื้อหารายเดือน, ประเภทยอดนิยม, สถิติผู้กำกับ

---

## 📂 โครงสร้างโปรเจค

```
Netflix_project/
│
├── framework.ipynb                      # การใช้งานไปป์ไลน์หลัก
│   ├── คลาส BronzeLayer                # โลจิกการรับข้อมูล Bronze
│   ├── คลาส SilverLayer                # โลจิกการแปลง Silver
│   ├── คลาส GoldLayer                  # โลจิกการรวมกลุ่ม Gold
│   ├── เอกสาร Bronze (markdown)         # คู่มือ Bronze แบบทีละขั้นตอน
│   ├── เอกสาร Silver (markdown)         # คู่มือ Silver แบบทีละขั้นตอน
│   └── เอกสาร Gold (markdown)           # คู่มือ Gold แบบทีละขั้นตอน
│
├── silver_layer_tests.py                # ชุดทดสอบครอบคลุม
│   ├── คลาส SilverLayerTests           # วิธีทดสอบอัตโนมัติ 5 วิธี
│   └── คลาส StarSchemaQueries          # ตัวช่วย SQL analytics
│
├── README.md                            # เอกสารภาษาอังกฤษ
├── README_TH.md                         # ไฟล์นี้
│
└── ตารางข้อมูล:
    ├── workspace.netflix.config_table              # การตั้งค่าไปป์ไลน์
    ├── workspace.netflix.netflix_bronze            # ข้อมูลดิบ (Bronze)
    ├── workspace.netflix.dim_titles_silver         # มิติหลัก (Silver)
    ├── workspace.netflix.dim_cast_silver           # มิติย่อยนักแสดง
    ├── workspace.netflix.dim_directors_silver      # มิติย่อยผู้กำกับ
    ├── workspace.netflix.dim_countries_silver      # มิติย่อยประเทศ
    ├── workspace.netflix.dim_categories_silver     # มิติย่อยประเภท
    ├── workspace.netflix.bridge_title_cast_silver  # ความสัมพันธ์ชื่อเรื่อง-นักแสดง
    ├── workspace.netflix.bridge_title_director_silver
    ├── workspace.netflix.bridge_title_country_silver
    ├── workspace.netflix.bridge_title_category_silver
    ├── workspace.netflix.netflix_bronze_bad_record # บันทึกตรวจสอบข้อมูลไม่ถูกต้อง
    ├── workspace.netflix.netflix_content_by_cast_gold # Cast แบบ Denormalized (Gold)
    └── workspace.netflix.netflix_yearly_content_trends_gold # แนวโน้มรายปี (Gold)
```

---

## ⚙️ ส่วนประกอบของไปป์ไลน์

### 1. การจัดการการตั้งค่า

**ตาราง**: `workspace.netflix.config_table`

```python
# การตั้งค่าไปป์ไลน์แบบรวมศูนย์
คอลัมน์ config_table:
- pipeline_name: str          # ตัวระบุเฉพาะ (เช่น "netflix")
- file_path: str              # ตำแหน่งข้อมูลต้นทาง
- header: bool                # การมีแถวหัวตาราง CSV
- delimiter: str              # ตัวแบ่งฟิลด์
- table_name: str             # ตาราง Bronze เป้าหมาย
- schema_detail: map          # การแมปคอลัมน์ → ชนิดข้อมูล
- keys: array                 # คอลัมน์ Primary key
- write_mode: str             # append/overwrite
```

### 2. ชั้น Bronze (คลาส `BronzeLayer`)

**ความรับผิดชอบ**:
- อ่านข้อมูลจากไฟล์ (CSV, JSON, Parquet)
- เพิ่มคอลัมน์ metadata สำหรับการติดตามที่มา
- เริ่มต้นตาราง Delta พร้อม CDF
- รูปแบบการโหลดแบบเพิ่มเติมเท่านั้น

**เมธอดหลัก**:
- `from_config_table(pipeline_name)` - Factory method
- `read_from_file()` - โหลดและเพิ่ม metadata
- `load_to_bronze_table(df)` - เพิ่มเติมไปยังตาราง Bronze
- `_init_bronze_table()` - การสร้างตารางครั้งแรก

### 3. ชั้น Silver (คลาส `SilverLayer`)

**ความรับผิดชอบ**:
- ประมวลผลการเปลี่ยนแปลงแบบเพิ่มหน่วยผ่าน CDF
- ตรวจสอบคุณภาพข้อมูล 8 ขั้นตอน
- แปลงเป็น star schema (9 ตาราง)
- ใช้ SCD Type 2 สำหรับติดตามการเปลี่ยนแปลง
- บันทึกข้อมูลไม่ถูกต้องเพื่อการตรวจสอบ

**เมธอดหลัก**:

#### ไปป์ไลน์คุณภาพข้อมูล:
1. `trim_data()` - ลบช่องว่าง
2. `change_data_type()` - แปลงชนิดข้อมูล
3. `get_invalid_record()` - ตรวจจับค่าไม่ถูกต้อง
4. `get_key_null_record()` - ค้นหา key ที่เป็น null
5. `get_dup_record()` - ระบุข้อมูลซ้ำ (แถวและ key)
6. `get_all_bad_record()` - รวบรวมข้อมูลไม่ถูกต้อง
7. `load_bad_record()` - บันทึกตรวจสอบ
8. `get_final_result()` - ดึงข้อมูลที่สะอาด

#### การสร้าง Star Schema:
- `get_hash_key_value()` - สร้าง hash สำหรับตรวจจับการเปลี่ยนแปลง
- `load_sub_dimensions()` - โหลดข้อมูลหลัก (นักแสดง, ผู้กำกับ, ฯลฯ)
- `load_bridge_tables()` - โหลดความสัมพันธ์แบบ many-to-many
- `load_main_dimension()` - SCD Type 2 upserts
- `process_cdf_stream_to_silver()` - จัดการไปป์ไลน์ทั้งหมด

### 4. ชั้น Gold (`GoldLayer` class)

**ความรับผิดชอบ**:
- สร้างตารางการวิเคราะห์แบบ Denormalized
- คำนวณเมตริกทางธุรกิจและ KPI ล่วงหน้า
- กรองเฉพาะข้อมูลปัจจุบัน (`active_flag = True`)
- เหมาะสำหรับ Dashboard และเครื่องมือ BI
- รูปแบบการรีเฟรชแบบเต็ม (โหมด overwrite)

**เมธอดหลัก**:

#### ตาราง Denormalized:
- `create_gold_content_by_cast()` - รวมความสัมพันธ์ Title-Cast แบบ Many-to-Many
  - Joins: `dim_titles_silver` ⋈ `bridge_title_cast_silver` ⋈ `dim_cast_silver`
  - Output: หนึ่งแถวต่อ Title-Cast หนึ่งคู่
  - คำถามทางธุรกิจ: "นักแสดงคนไหนแสดงในเรื่องอะไรบ้าง?"

- `create_gold_yearly_content_trends()` - รวมกลุ่มปริมาณเนื้อหาตามปีและประเภท
  - การรวมกลุ่ม: `GROUP BY release_year, type`
  - เมตริก: นับจำนวนเรื่อง
  - คำถามทางธุรกิจ: "ปริมาณเนื้อหาเปลี่ยนแปลงตามปีและประเภทอย่างไร?"

#### การจัดการไปป์ไลน์:
- `from_config_table(pipeline_name)` - Factory method จากการตั้งค่า
- `run_gold_pipeline()` - รันเมธอดสร้างตาราง Gold ทั้งหมด

**ลักษณะตาราง Gold**:
- **โหมดการเขียน**: เสมอ `overwrite` (รีเฟรชแบบเต็ม)
- **ความทันสมัยของข้อมูล**: สะท้อนสถานะล่าสุดของชั้น Silver
- **ประสิทธิภาพ Query**: รวม Join และรวมกลุ่มไว้ล่วงหน้าเพื่อความเร็ว
- **การใช้งาน**: Dashboard, รายงาน, การวิเคราะห์ Ad-hoc, Self-service BI

---

## 📊 โครงสร้างตาราง

### มิติหลัก: `dim_titles_silver`

| คอลัมน์ | ชนิด | คำอธิบาย |
|--------|------|---------|
| `title_sk` | BIGINT | Surrogate key (primary key) |
| `show_id` | STRING | Business key จากแหล่งข้อมูล |
| `type` | STRING | ภาพยนตร์หรือซีรีส์ |
| `title` | STRING | ชื่อเรื่อง |
| `date_added` | DATE | วันที่เพิ่มใน Netflix |
| `release_year` | INT | ปีที่เผยแพร่ |
| `rating` | STRING | เรตติ้ง (PG, R, ฯลฯ) |
| `duration` | STRING | ระยะเวลาหรือจำนวนซีซัน |
| `description` | STRING | เนื้อเรื่องย่อ |
| `hash_key` | STRING | SHA-256 ของ business keys |
| `hash_value` | STRING | SHA-256 ของคอลัมน์ข้อมูล |
| `active_flag` | BOOLEAN | ตัวบ่งชี้เวอร์ชันปัจจุบัน |
| `start_date` | TIMESTAMP | เวอร์ชันมีผลตั้งแต่ |
| `end_date` | TIMESTAMP | เวอร์ชันมีผลจนถึง (NULL = ปัจจุบัน) |
| `load_dt` | DATE | วันที่โหลด |
| `load_dttm` | TIMESTAMP | เวลาที่โหลด |

### มิติย่อย

**dim_cast_silver** (นักแสดง 36,399 คน):
- `cast_sk`, `cast_name`

**dim_directors_silver** (ผู้กำกับ 4,996 คน):
- `director_sk`, `director_name`

**dim_countries_silver** (145 ประเทศ):
- `country_sk`, `country_name`

**dim_categories_silver** (73 ประเภท):
- `category_sk`, `category_name`

### ตาราง Bridge (ความสัมพันธ์แบบ Many-to-Many)

**bridge_title_cast_silver** (ความสัมพันธ์ 128,818 รายการ):
- `title_sk`, `cast_sk`

**bridge_title_director_silver** (ความสัมพันธ์ 14,039 รายการ):
- `title_sk`, `director_sk`

**bridge_title_country_silver** (ความสัมพันธ์ 20,110 รายการ):
- `title_sk`, `country_sk`

**bridge_title_category_silver** (ความสัมพันธ์ 38,848 รายการ):
- `title_sk`, `category_sk`

### ตารางตรวจสอบ: `netflix_bronze_bad_record`

| คอลัมน์ | ชนิด | คำอธิบาย |
|--------|------|---------|
| คอลัมน์ต้นทางทั้งหมด | หลายชนิด | บันทึกต้นฉบับ |
| `_reason` | ARRAY<STRING> | รายการความล้มเหลวในการตรวจสอบ |
| `batch_id` | INT | ตัวระบุ batch |
| `load_dt` | DATE | วันที่ปฏิเสธ |
| `load_dttm` | TIMESTAMP | เวลาที่ปฏิเสธ |

### แผนภาพ Star Schema

```text

       [ ตารางมิติย่อย: ผู้กำกับ ]                       [ ตารางมิติย่อย: นักแสดง ]
          dim_directors_silver                             dim_cast_silver
         ┌──────────────────────┐                         ┌──────────────────┐
         │ PK  │ director_sk    │                         │ PK  │ cast_sk    │
         │     │ director_name  │                         │     │ cast_name  │
         └──────────┬───────────┘                         └────────┬─────────┘
                    │ (1)                                          │ (1)
                    ▼                                              ▼
                    ∞ (Many)                                       ∞ (Many)
       [ ตารางสะพานเชื่อมสัมพันธ์ ]                       [ ตารางสะพานเชื่อมสัมพันธ์ ]
         bridge_title_director                            bridge_title_cast
         ┌──────────────────────┐                         ┌──────────────────┐
         │ FK  │ title_sk       │                         │ FK  │ title_sk   │
         │ FK  │ director_sk    │                         │ FK  │ cast_sk    │
         └──────────┬───────────┘                         └────────┬─────────┘
                    │                                              │
                    │                                              │
                    └───────────────┐              ┌───────────────┘
                            ∞ (Many)│              │ ∞ (Many)
                                    ▼              ▼
                        ┌────────────────────────────────────────┐
                        │            dim_titles_silver           │
                        │        (ตารางมิติแกนกลางของหนัง)        │
                        ├────────────────────────────────────────┤
                        │ PK        │ title_sk                   │
                        │           │ show_id                    │
                        │           │ title                      │
                        │           │ type                       │
                        │           │ release_year               │
                        │           │ rating                     │
                        │           │ duration                   │
                        │           │ description                │
                        │           │ date_added                 │
                        ├────────────────────────────────────────┤
                        │ Hashing   │ hash_key                   │
                        │           │ hash_value                 │
                        ├────────────────────────────────────────┤
                        │ SCD T2    │ active_flag                │
                        │           │ start_date                 │
                        │           │ end_date                   │
                        ├────────────────────────────────────────┤
                        │ Metadata  │ load_dt                    │
                        │           │ load_dttm                  │
                        └────────────────────────────────────────┘
                                    ▲              ▲
                            ∞ (Many)│              │ ∞ (Many)
                    ┌───────────────┘              └───────────────┐
                    │                                              │
                    │                                              │
         ┌──────────┴───────────┐                         ┌────────┴─────────┐
         │ FK  │ title_sk       │                         │ FK  │ title_sk   │
         │ FK  │ country_sk     │                         │ FK  │ category_sk│
         └──────────────────────┘                         └──────────────────┘
          bridge_title_country                             bridge_title_category
                    ▲                                              ▲
                    │ ∞ (Many)                                     │ ∞ (Many)
                    │ (1)                                          │ (1)
         ┌──────────┴───────────┐                         ┌────────┴─────────┐
         │ PK  │ country_sk     │                         │ PK  │ category_sk│
         │     │ country_name   │                         │     │category_name│
         └──────────────────────┘                         └──────────────────┘
        [ ตารางมิติย่อย: ประเทศผลิต ]                    [ ตารางมิติย่อย: หมวดหมู่หนัง ]
          dim_countries_silver                            dim_categories_silver

```

**คำอธิบายสัญลักษณ์**:
- **PK** = Primary Key (คีย์หลัก)
- **FK** = Foreign Key (คีย์นอก)
- **(1)** = ด้านที่มีความสัมพันธ์แบบหนึ่ง
- **∞ (Many)** = ด้านที่มีความสัมพันธ์แบบหลาย
- **ตารางมิติหลัก** = ตารางข้อเท็จจริงกลางพร้อม SCD Type 2
- **ตารางมิติย่อย** = ตารางข้อมูลหลัก/ตารางค้นหา
- **ตารางสะพาน** = ตารางความสัมพันธ์แบบ many-to-many

**อธิบายความสัมพันธ์** (Cardinality):
- **ผู้กำกับหนึ่งคน** → **หลายเรื่อง** (ผ่าน bridge_title_director)
- **นักแสดงหนึ่งคน** → **หลายเรื่อง** (ผ่าน bridge_title_cast)
- **ประเทศหนึ่งประเทศ** → **หลายเรื่อง** (ผ่าน bridge_title_country)
- **หมวดหมู่หนึ่งหมวด** → **หลายเรื่อง** (ผ่าน bridge_title_category)
- แต่ละเรื่องสามารถมีหลายผู้กำกับ, หลายนักแสดง, หลายประเทศ, และหลายหมวดหมู่

---

## 🚀 เริ่มต้นใช้งาน

### ข้อกำหนดเบื้องต้น

- Databricks workspace (AWS/Azure/GCP)
- เปิดใช้งาน Unity Catalog
- Databricks Runtime 13.0+ หรือ MLR 13.0+
- Python 3.10+
- สิทธิ์เข้าถึง workspace catalog และ schema

### ขั้นตอนที่ 1: ตั้งค่าการกำหนดค่า

```python
# สร้างตารางการตั้งค่า
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.netflix.config_table (
    pipeline_name STRING,
    file_path STRING,
    header BOOLEAN,
    delimiter STRING,
    table_name STRING,
    schema_detail MAP<STRING, STRING>,
    keys ARRAY<STRING>,
    write_mode STRING
)
""")

# เพิ่มการตั้งค่าไปป์ไลน์ Netflix
config_data = [(
    "netflix",
    "/Volumes/main/default/netflix_data/*.csv",
    True,
    ",",
    "workspace.netflix.netflix_bronze",
    {"show_id": "string", "type": "string", ...},
    ["show_id"],
    "append"
)]

spark.createDataFrame(config_data, schema).write.mode("overwrite").saveAsTable("workspace.netflix.config_table")
```

### ขั้นตอนที่ 2: เรียกใช้ชั้น Bronze

```python
from framework import BronzeLayer

# เริ่มต้นจากการตั้งค่า
b = BronzeLayer.from_config_table("netflix")

# อ่านและโหลดข้อมูล
bronze_df = b.read_from_file()
b.load_to_bronze_table(bronze_df)

# ตรวจสอบ
spark.table("workspace.netflix.netflix_bronze").display()
```

### ขั้นตอนที่ 3: เรียกใช้ชั้น Silver

```python
from framework import SilverLayer

# เริ่มต้นจากการตั้งค่า
s = SilverLayer.from_config_table("netflix")

# ประมวลผลข้อมูลผ่านไปป์ไลน์คุณภาพ
s.process_cdf_stream_to_silver(
    checkpoint_location="/checkpoints/netflix_silver"
)
```

### ขั้นตอนที่ 4: เรียกใช้ชั้น Gold

```python
from framework import GoldLayer

# เริ่มต้นจากการตั้งค่า
g = GoldLayer.from_config_table("netflix")

# สร้างตาราง Gold ทั้งหมด
g.run_gold_pipeline()

# ตรวจสอบตาราง Gold
spark.table("workspace.netflix.netflix_content_by_cast_gold").display()
spark.table("workspace.netflix.netflix_yearly_content_trends_gold").display()
```

### ขั้นตอนที่ 5: ตรวจสอบผลลัพธ์

```python
# ตรวจสอบมิติหลัก
spark.sql("""
SELECT 
    COUNT(*) as total_titles,
    COUNT(CASE WHEN active_flag THEN 1 END) as active_titles,
    COUNT(DISTINCT show_id) as unique_shows
FROM workspace.netflix.dim_titles_silver
""").display()

# ตรวจสอบคุณภาพข้อมูล
spark.sql("""
SELECT 
    _reason,
    COUNT(*) as count
FROM workspace.netflix.netflix_bronze_bad_record
GROUP BY _reason
ORDER BY count DESC
""").display()
```

---

## 🧪 ระบบทดสอบ

### ภาพรวมชุดทดสอบ

โปรเจคมีชุดทดสอบครอบคลุมใน `silver_layer_tests.py` พร้อม**การทดสอบอัตโนมัติ 5 ชุด**ครอบคลุมทุกด้านของไปป์ไลน์

### การเรียกใช้การทดสอบ

```python
from silver_layer_tests import SilverLayerTests

# เริ่มต้นชุดทดสอบ
tests = SilverLayerTests(
    bronze_table="workspace.netflix.netflix_bronze",
    silver_table="workspace.netflix.dim_titles_silver",
    bad_record_table="workspace.netflix.netflix_bronze_bad_record"
)

# เรียกใช้การทดสอบทั้งหมด
results = tests.run_all_tests(skip_full_dataset=False)

# เรียกใช้การทดสอบแยกรายการ
tests.test_star_schema_integration()
tests.test_complete_pipeline_real_data(batch_size=100)
tests.test_scd_type2_change_detection()
tests.test_full_dataset_performance()
tests.test_idempotency()
```

### คำอธิบายการทดสอบ

#### 1. **การทดสอบการรวม Star Schema**
- **วัตถุประสงค์**: ตรวจสอบว่าทั้ง 9 ตารางมีอยู่และมีข้อมูล
- **ตรวจสอบ**:
  - การสร้าง hash key
  - ตารางมิติหลัก
  - ตารางมิติย่อย 4 ตาราง
  - ตาราง bridge 4 ตาราง
- **เกณฑ์ผ่าน**: ตารางทั้งหมดมีอยู่พร้อมจำนวนระเบียนที่คาดหวัง

#### 2. **การทดสอบไปป์ไลน์ครบวงจร** (100 รายการ)
- **วัตถุประสงค์**: การตรวจสอบไปป์ไลน์แบบครบวงจร
- **ตรวจสอบ**:
  - การโหลดข้อมูลจาก Bronze
  - ไปป์ไลน์ตรวจสอบคุณภาพ
  - แยกข้อมูลดี/ไม่ดี
  - การโหลดตาราง Silver
- **เกณฑ์ผ่าน**: ข้อมูลที่ถูกต้อง 100% ถูกโหลด, บันทึกข้อมูลไม่ถูกต้อง

#### 3. **การทดสอบการตรวจจับการเปลี่ยนแปลง SCD Type 2**
- **วัตถุประสงค์**: ตรวจสอบการติดตามประวัติการเปลี่ยนแปลง
- **ตรวจสอบ**:
  - การแทรกบันทึกเริ่มต้น
  - การตรวจจับการเปลี่ยนแปลงผ่าน hash_value
  - การปิดบันทึกประวัติ (end_date)
  - การสร้างเวอร์ชันใหม่
  - การจัดการ active_flag
- **เกณฑ์ผ่าน**: เวอร์ชันเก่าปิด, เวอร์ชันใหม่ active, บันทึกที่ไม่เปลี่ยนแปลงไม่ถูกแก้ไข

#### 4. **การทดสอบประสิทธิภาพชุดข้อมูลเต็ม** (17,618 รายการ)
- **วัตถุประสงค์**: การตรวจสอบขนาด Production
- **ตรวจสอบ**:
  - การประมวลผลชุดข้อมูลเต็ม
  - เมตริกประสิทธิภาพ
  - การคำนวณ throughput
- **เกณฑ์ผ่าน**: ประมวลผลข้อมูลทั้งหมดภายในเวลาที่ยอมรับได้ (<2 นาที)
- **ผลลัพธ์**: **317.7 รายการต่อวินาที**, รวม 55.46 วินาที

#### 5. **การทดสอบ Idempotency**
- **วัตถุประสงค์**: ตรวจสอบความสามารถในการเรียกใช้ซ้ำอย่างปลอดภัย
- **ตรวจสอบ**:
  - จำนวนระเบียนก่อน/หลังการเรียกใช้ซ้ำ
  - ไม่มีการสร้างข้อมูลซ้ำ
  - จำนวน active_flag ที่สอดคล้อง
- **เกณฑ์ผ่าน**: การประมวลผลข้อมูลเดิมซ้ำไม่สร้างระเบียนใหม่

### สรุปผลการทดสอบ

```
================================================================================
สรุปชุดทดสอบ
================================================================================
   การรวม Star Schema               : ✅ ผ่าน
   ไปป์ไลน์ครบวงจร                  : ✅ ผ่าน
   SCD Type 2                        : ✅ ผ่าน
   ชุดข้อมูลเต็ม                    : ✅ ผ่าน
   Idempotency                       : ✅ ผ่าน

📊 ผลลัพธ์: ผ่าน 5/5, ล้มเหลว 0, ข้าม 0
⏱️  เวลารวม: 184.88 วินาที (3.1 นาที)

🎉 การทดสอบทั้งหมดผ่าน - ไปป์ไลน์พร้อมใช้งาน PRODUCTION!
```

---

## 💡 ตัวอย่างการใช้งาน

### ตัวอย่างที่ 1: Query ชื่อเรื่องที่ active

```sql
SELECT 
    title,
    type,
    release_year,
    rating
FROM workspace.netflix.dim_titles_silver
WHERE active_flag = true
ORDER BY date_added DESC
LIMIT 10
```

### ตัวอย่างที่ 2: วิเคราะห์เนื้อหาตามประเทศ

```sql
SELECT 
    c.country_name,
    COUNT(DISTINCT t.title_sk) as title_count,
    SUM(CASE WHEN t.type = 'Movie' THEN 1 ELSE 0 END) as movies,
    SUM(CASE WHEN t.type = 'TV Show' THEN 1 ELSE 0 END) as tv_shows
FROM workspace.netflix.dim_titles_silver t
JOIN workspace.netflix.bridge_title_country_silver b ON t.title_sk = b.title_sk
JOIN workspace.netflix.dim_countries_silver c ON b.country_sk = c.country_sk
WHERE t.active_flag = true
GROUP BY c.country_name
ORDER BY title_count DESC
LIMIT 15
```

### ตัวอย่างที่ 3: นักแสดงยอดนิยมตามจำนวนเนื้อหา

```sql
SELECT 
    a.cast_name,
    COUNT(DISTINCT t.title_sk) as appearances,
    COUNT(DISTINCT CASE WHEN t.type = 'Movie' THEN t.title_sk END) as movies,
    COUNT(DISTINCT CASE WHEN t.type = 'TV Show' THEN t.title_sk END) as tv_shows
FROM workspace.netflix.dim_cast_silver a
JOIN workspace.netflix.bridge_title_cast_silver b ON a.cast_sk = b.cast_sk
JOIN workspace.netflix.dim_titles_silver t ON b.title_sk = t.title_sk
WHERE t.active_flag = true
GROUP BY a.cast_name
ORDER BY appearances DESC
LIMIT 10
```

### ตัวอย่างที่ 4: ติดตามประวัติการเปลี่ยนแปลง (SCD Type 2)

```sql
SELECT 
    show_id,
    title,
    rating,
    active_flag,
    start_date,
    end_date,
    CASE 
        WHEN active_flag THEN 'ปัจจุบัน'
        ELSE 'ประวัติ'
    END as version_status
FROM workspace.netflix.dim_titles_silver
WHERE show_id = 's1'  -- แทนที่ด้วย show_id จริง
ORDER BY start_date DESC
```

### ตัวอย่างที่ 5: แดชบอร์ดคุณภาพข้อมูล

```sql
SELECT 
    DATE(load_dttm) as load_date,
    explode(_reason) as failure_reason,
    COUNT(*) as failure_count
FROM workspace.netflix.netflix_bronze_bad_record
GROUP BY DATE(load_dttm), explode(_reason)
ORDER BY load_date DESC, failure_count DESC
```

### ตัวอย่างที่ 6: การใช้ SQL Query Helpers

```python
from silver_layer_tests import StarSchemaQueries

# Query การวิเคราะห์ด่วน
StarSchemaQueries.query_overview(spark)
StarSchemaQueries.query_top_actors(spark, limit=10)
StarSchemaQueries.query_content_by_country(spark, limit=15)
StarSchemaQueries.query_genre_analysis(spark, limit=15)
StarSchemaQueries.query_multidimensional_analysis(spark, limit=10)
StarSchemaQueries.query_scd_history(spark)
```

### ตัวอย่างที่ 7: ชั้น Gold - การวิเคราะห์เนื้อหาตามนักแสดง (เร็ว!)

```sql
-- รวม Join และ Denormalized ไว้แล้ว - ไม่ต้อง Join ซับซ้อน!
SELECT 
    cast_name,
    COUNT(DISTINCT show_id) as total_titles,
    COUNT(DISTINCT CASE WHEN type = 'Movie' THEN show_id END) as movies,
    COUNT(DISTINCT CASE WHEN type = 'TV Show' THEN show_id END) as tv_shows
FROM workspace.netflix.netflix_content_by_cast_gold
GROUP BY cast_name
ORDER BY total_titles DESC
LIMIT 10
```

### ตัวอย่างที่ 8: ชั้น Gold - แนวโน้มเนื้อหารายปี

```sql
-- เมตริกที่รวมกลุ่มไว้ล่วงหน้า - ผลลัพธ์ทันที!
SELECT 
    release_year,
    SUM(CASE WHEN type = 'Movie' THEN total_title ELSE 0 END) as movies,
    SUM(CASE WHEN type = 'TV Show' THEN total_title ELSE 0 END) as tv_shows,
    SUM(total_title) as total_content
FROM workspace.netflix.netflix_yearly_content_trends_gold
WHERE release_year >= 2015
GROUP BY release_year
ORDER BY release_year DESC
```

### ตัวอย่างที่ 9: ชั้น Gold - เครือข่ายความร่วมมือของนักแสดง

```sql
-- ค้นหานักแสดงที่มักร่วมงานด้วยกัน
SELECT 
    a.cast_name as actor1,
    b.cast_name as actor2,
    COUNT(DISTINCT a.show_id) as collaborations
FROM workspace.netflix.netflix_content_by_cast_gold a
JOIN workspace.netflix.netflix_content_by_cast_gold b
    ON a.show_id = b.show_id 
    AND a.cast_id < b.cast_id  -- หลีกเลี่ยงข้อมูลซ้ำ
GROUP BY actor1, actor2
HAVING COUNT(DISTINCT a.show_id) >= 3
ORDER BY collaborations DESC
LIMIT 20
```

### ตัวอย่างที่ 10: ชั้น Gold - แนวโน้มอัตราส่วนภาพยนตร์ต่อซีรีส์

```sql
-- วิเคราะห์การเปลี่ยนแปลงกลยุทธ์เนื้อหาตามเวลา
SELECT 
    release_year,
    MAX(CASE WHEN type = 'Movie' THEN total_title END) as movie_count,
    MAX(CASE WHEN type = 'TV Show' THEN total_title END) as tv_count,
    ROUND(
        MAX(CASE WHEN type = 'Movie' THEN total_title END) * 100.0 / 
        NULLIF(MAX(CASE WHEN type = 'TV Show' THEN total_title END), 0),
        2
    ) as movie_to_tv_ratio_pct
FROM workspace.netflix.netflix_yearly_content_trends_gold
WHERE release_year >= 2010
GROUP BY release_year
ORDER BY release_year DESC
```

---

## 📈 ประสิทธิภาพ

### มาตรฐาน Production

**สภาพแวดล้อมการทดสอบ**:
- แพลตฟอร์ม: Databricks Serverless (AWS)
- ชุดข้อมูล: 17,618 รายการ
- ไปป์ไลน์: Bronze → Silver (การแปลงเต็มรูปแบบ)

**ผลลัพธ์**:

| เมตริก | ค่า |
|--------|-----|
| **เวลาประมวลผลรวม** | 55.46 วินาที |
| **Throughput** | 317.7 รายการต่อวินาที |
| **เฉลี่ยต่อรายการ** | 3.15 มิลลิวินาที |
| **อัตราผ่านคุณภาพข้อมูล** | 50.0% (8,809 ถูกต้อง) |
| **ตรวจพบข้อมูลไม่ถูกต้อง** | 50.0% (8,809 ไม่ถูกต้อง) |
| **ตารางที่สร้าง** | 9 ตาราง |
| **ความสัมพันธ์ที่สร้าง** | 201,815 รวม |

**ความสามารถในการขยายขนาด**:
- ✅ จัดการข้อมูล 17K+ รายการใน <1 นาที
- ✅ Idempotent (เรียกใช้ซ้ำได้อย่างปลอดภัย)
- ✅ การประมวลผลแบบเพิ่มหน่วยผ่าน CDF
- ✅ เหมาะสำหรับงาน batch รายชั่วโมง/รายวัน

### การกระจายข้อมูล

**การเติมข้อมูล Star Schema**:

| ตาราง | จำนวนรายการ | คำอธิบาย |
|-------|------------|---------|
| dim_titles_silver | 8,817 | มิติหลัก (8,816 active) |
| dim_cast_silver | 36,399 | นักแสดงที่ไม่ซ้ำ |
| dim_directors_silver | 4,996 | ผู้กำกับที่ไม่ซ้ำ |
| dim_countries_silver | 145 | ประเทศ |
| dim_categories_silver | 73 | ประเภท |
| bridge_title_cast_silver | 128,818 | ความสัมพันธ์ชื่อเรื่อง-นักแสดง |
| bridge_title_director_silver | 14,039 | ความสัมพันธ์ชื่อเรื่อง-ผู้กำกับ |
| bridge_title_country_silver | 20,110 | ความสัมพันธ์ชื่อเรื่อง-ประเทศ |
| bridge_title_category_silver | 38,848 | ความสัมพันธ์ชื่อเรื่อง-ประเภท |

**ความสัมพันธ์รวม**: 201,815 รายการในตาราง bridge

---

## ✅ แนวทางปฏิบัติที่ดี

### 1. คุณภาพข้อมูล

✅ **ตรวจสอบข้อมูลไม่ถูกต้องเสมอ**:
```python
spark.sql("""
SELECT _reason, COUNT(*) 
FROM workspace.netflix.netflix_bronze_bad_record 
GROUP BY _reason
""").display()
```

✅ **ติดตามแนวโน้มคุณภาพ**:
```python
spark.sql("""
SELECT 
    load_dt,
    COUNT(*) as total_processed,
    SUM(CASE WHEN _reason IS NULL THEN 1 ELSE 0 END) as good_records,
    SUM(CASE WHEN _reason IS NOT NULL THEN 1 ELSE 0 END) as bad_records
FROM (
    SELECT load_dt, NULL as _reason FROM workspace.netflix.dim_titles_silver
    UNION ALL
    SELECT load_dt, _reason FROM workspace.netflix.netflix_bronze_bad_record
)
GROUP BY load_dt
ORDER BY load_dt DESC
""").display()
```

### 2. การเพิ่มประสิทธิภาพ

✅ **ใช้การประมวลผลแบบเพิ่มหน่วย**:
- เปิดใช้งาน CDF บนตาราง Bronze
- ใช้ checkpoint สำหรับ streaming
- ประมวลผลเฉพาะข้อมูลที่เปลี่ยนแปลง

✅ **แบ่งพาร์ติชันตารางขนาดใหญ่**:
```sql
ALTER TABLE workspace.netflix.netflix_bronze 
PARTITION BY (DATE(_load_dt))
```

✅ **เพิ่มประสิทธิภาพการ join**:
- ใช้ broadcast join สำหรับตารางมิติขนาดเล็ก
- กรองก่อนการ join
- ใช้ตารางที่ cache สำหรับ query ซ้ำ

### 3. การกำกับดูแลข้อมูล

✅ **บันทึกการเปลี่ยนแปลง schema**:
- อัปเดต config_table เมื่อ schema ต้นทางเปลี่ยน
- ควบคุมเวอร์ชันของโค้ดไปป์ไลน์
- รักษาบันทึกตรวจสอบใน Bronze

✅ **ตั้งนโยบายการเก็บรักษา**:
```sql
ALTER TABLE workspace.netflix.netflix_bronze
SET TBLPROPERTIES (
    'delta.logRetentionDuration' = '90 days',
    'delta.deletedFileRetentionDuration' = '90 days'
)
```

✅ **ใช้การควบคุมการเข้าถึง**:
```sql
GRANT SELECT ON TABLE workspace.netflix.dim_titles_silver TO `analysts`
GRANT SELECT ON TABLE workspace.netflix.netflix_bronze TO `data_engineers`
```

### 4. การติดตามและแจ้งเตือน

✅ **ติดตามสุขภาพไปป์ไลน์**:
- ติดตามเวลาประมวลผล batch
- แจ้งเตือนเมื่อการตรวจสอบคุณภาพล้มเหลว
- ติดตามจำนวนข้อมูลไม่ถูกต้อง
- ติดตามอัตราการเติบโตของตาราง

✅ **ตั้งค่าแดชบอร์ด**:
- ความสดของข้อมูล (timestamp การโหลดล่าสุด)
- เมตริกคุณภาพ (อัตราดี/ไม่ดี)
- การเปลี่ยนแปลง schema
- แนวโน้มประสิทธิภาพ

### 5. การทดสอบ

✅ **เรียกใช้การทดสอบก่อนนำไปใช้งาน production**:
```python
# เรียกใช้ชุดทดสอบเต็มเสมอ
results = tests.run_all_tests(skip_full_dataset=False)
assert results['failed'] == 0, "การทดสอบต้องผ่านก่อนนำไปใช้งาน"
```

✅ **ทดสอบด้วยปริมาณข้อมูลเหมือน production**  
✅ **ตรวจสอบพฤติกรรม SCD Type 2**  
✅ **ยืนยัน idempotency**  

---

## 🔧 การแก้ไขปัญหา

### ปัญหาที่พบบ่อย

#### ปัญหาที่ 1: ข้อผิดพลาด Schema ไม่ตรงกัน

**อาการ**: `AnalysisException: cannot resolve column`

**สาเหตุ**:
- คอลัมน์ CSV ต้นทางเปลี่ยน
- schema_detail ของ config table ล้าสมัย
- คอลัมน์หายในข้อมูลต้นทาง

**แก้ไข**:
```python
# 1. ตรวจสอบ schema ต้นทาง
df = spark.read.option("header", True).csv("/path/to/file.csv")
df.printSchema()

# 2. อัปเดต config_table
spark.sql("""
UPDATE workspace.netflix.config_table
SET schema_detail = map(
    'show_id', 'string',
    'new_column', 'string',
    ...
)
WHERE pipeline_name = 'netflix'
""")

# 3. สร้างตาราง Bronze ใหม่ถ้าจำเป็น
spark.sql("DROP TABLE IF EXISTS workspace.netflix.netflix_bronze")
```

#### ปัญหาที่ 2: ข้อมูลซ้ำ

**อาการ**: ข้อมูลไม่ถูกต้องที่มีเหตุผล `_key_duplicate`

**สาเหตุ**:
- show_id เดียวกันแต่ข้อมูลต่างกัน
- ปัญหาคุณภาพข้อมูลต้นทาง

**แก้ไข**:
```python
# ตรวจสอบข้อมูลซ้ำ
spark.sql("""
SELECT show_id, COUNT(*) as dup_count
FROM workspace.netflix.netflix_bronze
GROUP BY show_id
HAVING dup_count > 1
""").display()

# ตรวจสอบข้อมูลไม่ถูกต้อง
spark.sql("""
SELECT *
FROM workspace.netflix.netflix_bronze_bad_record
WHERE array_contains(_reason, '_key_duplicate')
""").display()
```

#### ปัญหาที่ 3: SCD Type 2 ไม่ปิดข้อมูลเก่า

**อาการ**: มีหลายรายการ active สำหรับ show_id เดียวกัน

**สาเหตุ**:
- การคำนวณ Hash ไม่สอดคล้อง
- คอลัมน์ hash_key หรือ hash_value หายไป

**แก้ไข**:
```python
# ตรวจสอบเวอร์ชัน active หลายรายการ
spark.sql("""
SELECT show_id, COUNT(*) as active_count
FROM workspace.netflix.dim_titles_silver
WHERE active_flag = true
GROUP BY show_id
HAVING active_count > 1
""").display()

# ตรวจสอบการสร้าง hash
from framework import SilverLayer
s = SilverLayer.from_config_table("netflix")
test_df = spark.table("workspace.netflix.netflix_bronze").limit(5)
hash_df = s.get_hash_key_value(test_df)
hash_df.select("show_id", "hash_key", "hash_value").display()
```

#### ปัญหาที่ 4: ประสิทธิภาพลดลง

**อาการ**: การประมวลผลใช้เวลานานกว่าที่คาดหวัง

**สาเหตุ**:
- ตารางไม่ได้เพิ่มประสิทธิภาพ
- สแกนตารางเต็มแทนการประมวลผลแบบเพิ่มหน่วย
- ขาด CDF checkpoint

**แก้ไข**:
```sql
-- เพิ่มประสิทธิภาพตาราง Delta
OPTIMIZE workspace.netflix.netflix_bronze;
OPTIMIZE workspace.netflix.dim_titles_silver;

-- ตรวจสอบสถิติตาราง
DESCRIBE DETAIL workspace.netflix.netflix_bronze;

-- วิเคราะห์แผน query
EXPLAIN EXTENDED
SELECT * FROM workspace.netflix.dim_titles_silver WHERE active_flag = true;
```

#### ปัญหาที่ 5: การทดสอบล้มเหลว

**อาการ**: การทดสอบ SCD Type 2 ล้มเหลว

**สาเหตุ**:
- ข้อมูลทดสอบเก่ายังคงอยู่
- Schema ไม่ตรงกันในข้อมูลทดสอบ

**แก้ไข**:
```python
# ล้างข้อมูลทดสอบเก่า
spark.sql("""
DELETE FROM workspace.netflix.dim_titles_silver 
WHERE show_id LIKE 'TEST_SCD_%'
""")

# เรียกใช้การทดสอบอีกครั้ง
tests = SilverLayerTests(...)
tests.test_scd_type2_change_detection()
```

---

## 🤝 การมีส่วนร่วม

### ขั้นตอนการพัฒนา

1. **Fork notebook** หรือสร้างเวอร์ชันใหม่
2. **ทำการเปลี่ยนแปลง**ในสภาพแวดล้อมการพัฒนาของคุณ
3. **เรียกใช้ชุดทดสอบเต็ม**เพื่อตรวจสอบการเปลี่ยนแปลง
4. **อัปเดตเอกสาร**หากเพิ่มฟีเจอร์ใหม่
5. **ส่งเพื่อตรวจสอบ** (หากเป็นโปรเจคร่วมกัน)

### มาตรฐานโค้ด

✅ ใช้แนวทาง PEP 8  
✅ เพิ่ม docstring ในทุก method  
✅ รวมการทดสอบสำหรับฟีเจอร์ใหม่  
✅ อัปเดต README สำหรับการเปลี่ยนแปลงที่สำคัญ  
✅ ใช้ type hints ที่เหมาะสม  

### ข้อกำหนดการทดสอบ

- การทดสอบทั้ง 5 ชุดต้องผ่าน
- ไม่มีการทำลาย schema ในชั้น Silver
- รักษามาตรฐานประสิทธิภาพ
- รักษาความเข้ากันได้แบบย้อนหลัง

---

## 📚 แหล่งข้อมูลเพิ่มเติม

### เอกสาร

- [Databricks Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Change Data Feed Guide](https://docs.databricks.com/delta/delta-change-data-feed.html)
- [SCD Type 2 Best Practices](https://www.databricks.com/blog/2022/08/22/dimensional-modeling-delta-lake.html)

### Notebook ที่เกี่ยวข้อง

- `framework.ipynb` - การใช้งานไปป์ไลน์หลัก
- `silver_layer_tests.py` - ชุดทดสอบครอบคลุม

### การสนับสนุน

สำหรับคำถามหรือปัญหา:
1. ตรวจสอบส่วนการแก้ไขปัญหาด้านบน
2. ตรวจสอบผลการทดสอบสำหรับรายละเอียดข้อผิดพลาด
3. ตรวจสอบตารางข้อมูลไม่ถูกต้องสำหรับปัญหาคุณภาพข้อมูล
4. ศึกษาเอกสาร Databricks

---

## 📝 สิทธิ์การใช้งาน

โปรเจคนี้เป็นส่วนหนึ่งของโปรแกรมฝึกอบรม**Databricks for Data Engineers Bootcamp**

---

## 🎉 กิตติกรรมประกาศ

**สร้างด้วย**:
- Databricks Unified Analytics Platform
- Apache Spark 3.x
- Delta Lake
- Python 3.10+

**สถาปัตยกรรม**:
- Medallion Architecture (Bronze/Silver/Gold)
- Star Schema Design
- SCD Type 2 Implementation

**การทดสอบ**:
- ชุดทดสอบอัตโนมัติครอบคลุม
- การตรวจสอบระดับ Production
- การวัดประสิทธิภาพ

---

**อัปเดตล่าสุด**: มิถุนายน 2026  
**เวอร์ชัน**: 1.0  
**สถานะ**: พร้อมใช้งาน Production ✅

---

*สนุกกับการทำ Data Engineering! 🚀*