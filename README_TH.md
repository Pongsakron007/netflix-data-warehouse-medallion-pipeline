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
- [การตั้งค่า](#การตั้งค่า)
- [ระบบทดสอบ](#ระบบทดสอบ)
- [ตัวอย่างการใช้งาน](#ตัวอย่างการใช้งาน)
- [ประสิทธิภาพ](#ประสิทธิภาพ)
- [แนวทางปฏิบัติที่ดี](#แนวทางปฏิบัติที่ดี)
- [การแก้ไขปัญหา](#การแก้ไขปัญหา)
- [การมีส่วนร่วม](#การมีส่วนร่วม)

---

## 🎯 ภาพรวม

โปรเจคนี้พัฒนา**ไปป์ไลน์ข้อมูลที่พร้อมใช้งานจริงและขยายขนาดได้**สำหรับการประมวลผลข้อมูลเนื้อหา Netflix โดยใช้ Databricks และ**สถาปัตยกรรม Medallion** สร้างด้วย**คลาส dataclass ที่ปรับแต่งได้** ระบบนี้รับข้อมูลดิบ ตรวจสอบคุณภาพข้อมูลอย่างครอบคลุม และแปลงเป็น**โครงสร้าง Star Schema** ที่เหมาะสำหรับการวิเคราะห์และ Business Intelligence

### ฟีเจอร์หลัก

✅ **เฟรมเวิร์กที่ปรับแต่งได้**: คลาส `BronzeLayer`, `SilverLayer` และ `GoldLayer` แบบ dataclass  
✅ **สถาปัตยกรรม Medallion**: ชั้น Bronze (ดิบ) → Silver (สะอาด) → Gold (รวมกลุ่ม)  
✅ **Databricks Auto Loader**: การรับข้อมูลแบบเพิ่มหน่วยจาก S3 พร้อมการตรวจจับโฟลเดอร์และ schema evolution  
✅ **ตรวจสอบคุณภาพข้อมูล**: ไปป์ไลน์ตรวจสอบคุณภาพ 8 ขั้นตอนพร้อมกักกันข้อมูลไม่ถูกต้อง  
✅ **SCD Type 2**: ติดตามประวัติการเปลี่ยนแปลงพร้อมความถูกต้องเชิงเวลา  
✅ **Star Schema**: 1 มิติหลัก + 4 มิติย่อย + 4 ตาราง Bridge  
✅ **ตรวจจับการเปลี่ยนแปลงด้วย Hash**: การระบุความแตกต่างด้วย SHA-256  
✅ **ประมวลผลแบบเพิ่มหน่วย**: เปิดใช้งาน Change Data Feed (CDF) สำหรับทุกชั้นถัดไป  
✅ **ความสามารถขยายระดับ Production**: Structured streaming ด้วย `trigger(availableNow=True)`  
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
                                + Auto Loader                 + Normalization                + พร้อม BI

  ┌────────────┐              ┌────────────┐              ┌────────────────┐              ┌────────────┐
  │ netflix.csv│              │  netflix   │              │   dim_titles   │              │  Dashboard │
  │            │  ─────────►  │   _bronze  │  ─────────►  │ + 4 sub-dims   │  ─────────►  │    KPIs    │
  │ (17K แถว)  │              │            │              │ + 4 bridges    │              │  รายงาน    │
  └────────────┘              └────────────┘              │ + bad_records  │              └────────────┘
                                     ▲                     └────────────────┘
                                     │
                               config_table
                               (การตั้งค่าไปป์ไลน์)
```

### ความรับผิดชอบของแต่ละชั้น

#### 🥉 **ชั้น Bronze** - การรับข้อมูลดิบ
- **วัตถุประสงค์**: จุดลงจอดสำหรับข้อมูลภายนอกพร้อมการติดตามครบถ้วน
- **คลาส**: `BronzeLayer` (การตั้งค่าแบบ dataclass)
- **ลักษณะเฉพาะ**: ไม่เปลี่ยนแปลง, เพิ่มเติมเท่านั้น, schema-on-read
- **ฟีเจอร์หลัก**:
  - **Databricks Auto Loader** สำหรับการรับข้อมูลแบบเพิ่มหน่วยจาก S3/cloud storage
  - **การตรวจจับโฟลเดอร์** - ค้นหาไฟล์ใหม่ในไดเรกทอรีโดยอัตโนมัติ
  - **Schema evolution** ด้วย rescue mode (`cloudFiles.schemaEvolutionMode: "rescue"`)
  - **การกำหนด schema อย่างชัดเจน** โดยใช้ StructType สำหรับคอลัมน์ metadata ที่ปลอดภัยในเรื่องชนิดข้อมูล
  - **การติดตามไฟล์แบบ checkpoint** - ประมวลผลเฉพาะไฟล์ใหม่เท่านั้น
  - **การจัดการข้อผิดพลาด** สำหรับกรณีพิเศษของ Spark Connect serverless (SPARK-55448)
  - **Change Data Feed (CDF)** เปิดใช้งานสำหรับการประมวลผลแบบเพิ่มหน่วยถัดไป
  - ติดตาม metadata ของไฟล์ (`_load_dt`, `_file_name`, `_file_path`, `_file_size`, `_file_mod`)
  - รองรับรูปแบบ CSV, JSON, Parquet
  - ปรับแต่งได้ผ่าน `config_table` หรือสร้างอินสแตนซ์โดยตรง

**โหมด Auto Loader**:
- **โหมด Batch**: `read_from_file()` - โหลดครั้งเดียวเต็มจำนวน
- **โหมด Streaming**: `s3_auto_loader()` - แบบเพิ่มหน่วยด้วย `trigger(availableNow=True)`

#### 🥈 **ชั้น Silver** - คุณภาพข้อมูลและ Normalization
- **วัตถุประสงค์**: ข้อมูลที่สะอาด ตรวจสอบแล้ว พร้อมใช้งานทางธุรกิจ
- **คลาส**: `SilverLayer` (การตั้งค่าแบบ dataclass)
- **ลักษณะเฉพาะ**: Normalized, ลบข้อมูลซ้ำ, ตรวจสอบแล้ว, เปิดใช้ SCD Type 2
- **ฟีเจอร์หลัก**:

**ไปป์ไลน์คุณภาพข้อมูล 8 ขั้นตอน**:
1. `trim_data()` - ลบช่องว่างด้านหน้าและหลัง
2. `change_data_type()` - แปลงเป็นชนิดข้อมูลเป้าหมายด้วย `try_cast()` / `try_to_date()`
3. `get_invalid_record()` - ตรวจจับการละเมิดรูปแบบด้วย regex
4. `get_key_null_record()` - ระบุ primary key ที่เป็น null
5. `get_dup_record()` - ค้นหาข้อมูลซ้ำทั้งแถวและ key
6. `get_all_bad_record()` - รวบรวมข้อมูลไม่ถูกต้องทั้งหมด
7. `load_bad_record()` - กักกันข้อมูลไม่ถูกต้องพร้อมการติดตาม batch
8. `get_final_result()` - ดึงข้อมูลที่สะอาด

**การแปลง Star Schema**:
- `get_hash_key_value()` - สร้าง hash SHA-256 สำหรับ CDC
- `load_sub_dimensions()` - ใส่ข้อมูลตาราง dimension 4 ตาราง (cast, directors, countries, categories)
- `load_bridge_tables()` - สร้างตารางความสัมพันธ์แบบ many-to-many 4 ตาราง
- `load_main_dimension()` - ใช้ logic SCD Type 2 กับมิติหลัก
- `process_cdf_stream_to_silver()` - จัดการไปป์ไลน์แบบเพิ่มหน่วยทั้งหมด

**ผลลัพธ์**: ตาราง 9 ตาราง (1 มิติหลัก + 4 มิติย่อย + 4 bridges) + 1 ตารางข้อมูลไม่ถูกต้อง

#### 🥇 **ชั้น Gold** - การรวมกลุ่มทางธุรกิจ
- **วัตถุประสงค์**: ตารางวิเคราะห์ที่เหมาะสมที่สุดสำหรับการใช้งานของผู้ใช้ปลายทาง
- **คลาส**: `GoldLayer` (การตั้งค่าแบบ dataclass)
- **ลักษณะเฉพาะ**: Denormalized, รวมกลุ่มไว้ล่วงหน้า, พร้อมสำหรับ BI
- **ฟีเจอร์หลัก**:

**ตารางพร้อมใช้งานทางธุรกิจ**:
1. `create_gold_content_by_cast()` - ความสัมพันธ์ Title-Cast แบบ Denormalized
   - **Joins**: `dim_titles_silver` ⋈ `bridge_title_cast_silver` ⋈ `dim_cast_silver`
   - **ผลลัพธ์**: หนึ่งแถวต่อคู่ Title-Cast หนึ่งคู่
   - **คำถามทางธุรกิจ**: "นักแสดงคนไหนแสดงในเรื่องอะไรบ้าง?"

2. `create_gold_yearly_content_trends()` - ปริมาณเนื้อหาตามปีและประเภท
   - **การรวมกลุ่ม**: `GROUP BY release_year, type`
   - **เมตริก**: นับจำนวนเรื่อง
   - **คำถามทางธุรกิจ**: "ปริมาณเนื้อหาเปลี่ยนแปลงตามปีและประเภทอย่างไร?"

**โมเดลการทำงาน**:
- **โหมดการเขียน**: เสมอ `overwrite` (รีเฟรชแบบเต็ม)
- **ความทันสมัยของข้อมูล**: สะท้อนสถานะล่าสุดของชั้น Silver
- **ประสิทธิภาพ Query**: Join และรวมกลุ่มไว้ล่วงหน้าสำหรับ query BI ที่เร็ว
- **เฉพาะข้อมูล Active**: กรองสำหรับ `active_flag = True` จาก SCD Type 2 dimensions
- **การใช้งาน**: Dashboard, รายงาน, การวิเคราะห์ ad-hoc, self-service BI

---

## 📂 โครงสร้างโปรเจค

```
Databricks-for-Data-Engineers-Bootcamp2/
│
├── Netflix_project/
│   └── framework.ipynb                 # การใช้งานไปป์ไลน์หลัก
│       ├── คลาส BronzeLayer          # โลจิกการรับข้อมูล Bronze
│       ├── คลาส SilverLayer          # โลจิกคุณภาพและการแปลงข้อมูล
│       ├── คลาส GoldLayer            # โลจิกการรวมกลุ่มทางธุรกิจ
│       ├── เอกสาร Bronze (MD)         # คู่มือ Bronze แบบทีละขั้นตอน
│       ├── เอกสาร Silver (MD)         # คู่มือ Silver แบบทีละขั้นตอน
│       └── เอกสาร Gold (MD)           # คู่มือ Gold แบบทีละขั้นตอน
│
├── silver_layer_tests.py                # ชุดทดสอบครอบคลุม
│   ├── คลาส SilverLayerTests           # วิธีทดสอบอัตโนมัติ 5 วิธี
│   └── คลาส StarSchemaQueries          # ตัวช่วย SQL analytics
│
├── README.md                            # เอกสารภาษาอังกฤษ
├── README_TH.md                         # ไฟล์นี้ (เอกสารภาษาไทย)
│
└── ตารางข้อมูล:
    ├── workspace.netflix.config_table                      # การตั้งค่าไปป์ไลน์
    ├── workspace.netflix.netflix_bronze                    # ข้อมูลดิบ (Bronze)
    ├── workspace.netflix.dim_titles_silver                 # มิติหลัก (Silver)
    ├── workspace.netflix.dim_cast_silver                   # มิติย่อยนักแสดง
    ├── workspace.netflix.dim_directors_silver              # มิติย่อยผู้กำกับ
    ├── workspace.netflix.dim_countries_silver              # มิติย่อยประเทศ
    ├── workspace.netflix.dim_categories_silver             # มิติย่อยประเภท
    ├── workspace.netflix.bridge_title_cast_silver          # ความสัมพันธ์ชื่อเรื่อง-นักแสดง
    ├── workspace.netflix.bridge_title_director_silver      # ความสัมพันธ์ชื่อเรื่อง-ผู้กำกับ
    ├── workspace.netflix.bridge_title_country_silver       # ความสัมพันธ์ชื่อเรื่อง-ประเทศ
    ├── workspace.netflix.bridge_title_category_silver      # ความสัมพันธ์ชื่อเรื่อง-ประเภท
    ├── workspace.netflix.netflix_bronze_bad_record         # การกักกันข้อมูลไม่ถูกต้อง
    ├── workspace.netflix.netflix_content_by_cast_gold      # Cast แบบ Denormalized (Gold)
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
- file_path: str              # ตำแหน่งข้อมูลต้นทาง (ไฟล์หรือโฟลเดอร์)
- header: bool                # การมีแถวหัวตาราง CSV
- delimiter: str              # ตัวแบ่งฟิลด์
- table_name: str             # ชื่อตาราง Bronze เป้าหมาย
- schema_detail: map          # การแมปชื่อคอลัมน์ → ชนิดข้อมูล
- keys: array                 # คอลัมน์ Primary key
- write_mode: str             # append/overwrite
```

### 2. ชั้น Bronze (คลาส `BronzeLayer`)

**Factory Method**:
```python
bronze = BronzeLayer.from_config_table("netflix")
```

**ความรับผิดชอบ**:
- อ่านข้อมูลจากไฟล์ (CSV, JSON, Parquet) หรือโฟลเดอร์
- เพิ่มคอลัมน์ metadata สำหรับการติดตามแหล่งที่มา
- เริ่มต้นตาราง Delta พร้อม CDF เปิดใช้งาน
- รองรับทั้งการรับข้อมูลแบบ batch และ streaming

**เมธอดหลัก**:
- `from_config_table(pipeline_name)` - Factory method จากการตั้งค่า
- `read_from_file()` - โหมด Batch: โหลดไฟล์และเพิ่ม metadata
- `s3_auto_loader(checkpoint_location)` - โหมด Streaming: Auto Loader พร้อม checkpoint
- `load_to_bronze_table(df)` - เพิ่มข้อมูลเข้าตาราง Bronze
- `_init_bronze_table()` - เริ่มต้นตารางพร้อม CDF ในการรันครั้งแรก

**การตั้งค่า Auto Loader**:
```python
# Schema evolution และการค้นหาไฟล์
.option("cloudFiles.schemaEvolutionMode", "rescue")
.option("pathGlobFilter", "*.csv")
.option("cloudFiles.schemaLocation", schema_location)
.option("mergeSchema", "true")
.trigger(availableNow=True)  # Batch-style streaming สำหรับประหยัดต้นทุน
```

### 3. ชั้น Silver (คลาส `SilverLayer`)

**Factory Method**:
```python
silver = SilverLayer.from_config_table("netflix")
```

**ความรับผิดชอบ**:
- ประมวลผลการเปลี่ยนแปลงแบบเพิ่มหน่วยผ่าน CDF
- ดำเนินการตรวจสอบคุณภาพข้อมูล 8 ขั้นตอน
- แปลงเป็น star schema (9 ตาราง)
- ใช้ SCD Type 2 สำหรับติดตามประวัติ
- กักกันข้อมูลไม่ถูกต้องเพื่อการตรวจสอบ

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
- `get_hash_key_value()` - สร้าง hash สำหรับ CDC
- `load_sub_dimensions()` - ใส่ข้อมูล masters (cast, directors, ฯลฯ)
- `load_bridge_tables()` - สร้างความสัมพันธ์แบบ many-to-many
- `load_main_dimension()` - SCD Type 2 upserts
- `process_cdf_stream_to_silver()` - จัดการไปป์ไลน์เต็มรูปแบบ

**การประมวลผลแบบเพิ่มหน่วย**:
```python
# อ่านเฉพาะการเปลี่ยนแปลงจาก Bronze
bronze_cdf = (
    spark.readStream
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .table("workspace.netflix.netflix_bronze")
)
```

### 4. ชั้น Gold (คลาส `GoldLayer`)

**Factory Method**:
```python
gold = GoldLayer.from_config_table("netflix")
```

**ความรับผิดชอบ**:
- สร้างตารางการวิเคราะห์แบบ Denormalized
- คำนวณเมตริกและ KPI ทางธุรกิจล่วงหน้า
- กรองสำหรับข้อมูล active เท่านั้น (`active_flag = True`)
- เพิ่มประสิทธิภาพสำหรับ dashboard และเครื่องมือ BI
- รูปแบบ full refresh (โหมด overwrite)

**เมธอดหลัก**:

#### ตาราง Denormalized:
- `create_gold_content_by_cast()` - แบนความสัมพันธ์ Title-Cast แบบ Many-to-Many
  - Joins: `dim_titles_silver` ⋈ `bridge_title_cast_silver` ⋈ `dim_cast_silver`
  - ผลลัพธ์: หนึ่งแถวต่อคู่ Title-Cast หนึ่งคู่
  - คำถามทางธุรกิจ: "นักแสดงคนไหนแสดงในเรื่องอะไรบ้าง?"

- `create_gold_yearly_content_trends()` - รวมปริมาณเนื้อหาตามปีและประเภท
  - การรวมกลุ่ม: `GROUP BY release_year, type`
  - เมตริก: นับจำนวนเรื่อง
  - คำถามทางธุรกิจ: "ปริมาณเนื้อหาเปลี่ยนแปลงตามปีและประเภทอย่างไร?"

#### การจัดการไปป์ไลน์:
- `from_config_table(pipeline_name)` - Factory method จากการตั้งค่า
- `run_gold_pipeline()` - รันทุกเมธอดสร้างตาราง Gold

**ลักษณะตาราง Gold**:
- **โหมดการเขียน**: เสมอ `overwrite` (รีเฟรชแบบเต็ม)
- **ความทันสมัยของข้อมูล**: สะท้อนสถานะล่าสุดของชั้น Silver
- **ประสิทธิภาพ Query**: Join และรวมกลุ่มไว้ล่วงหน้าเพื่อความเร็ว
- **การใช้งาน**: Dashboard, รายงาน, การวิเคราะห์ ad-hoc, self-service BI

---

## 📊 โครงสร้างตาราง

### มิติหลัก: `dim_titles_silver`

| คอลัมน์ | ชนิด | คำอธิบาย |
|--------|------|---------|
| `title_sk` | BIGINT | Surrogate key (สร้างอัตโนมัติ) |
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
- `cast_sk`, `cast_id`, `cast_name`

**dim_directors_silver** (ผู้กำกับ 4,996 คน):
- `director_sk`, `director_id`, `director_name`

**dim_countries_silver** (145 ประเทศ):
- `country_sk`, `country_id`, `country_name`

**dim_categories_silver** (73 ประเภท):
- `category_sk`, `category_id`, `category_name`

### ตาราง Bridge (ความสัมพันธ์แบบ Many-to-Many)

**bridge_title_cast_silver** (ความสัมพันธ์ 128,818 รายการ):
- `show_id`, `cast_id`

**bridge_title_director_silver** (ความสัมพันธ์ 14,039 รายการ):
- `show_id`, `director_id`

**bridge_title_country_silver** (ความสัมพันธ์ 20,110 รายการ):
- `show_id`, `country_id`

**bridge_title_category_silver** (ความสัมพันธ์ 38,848 รายการ):
- `show_id`, `category_id`

### ตารางตรวจสอบ: `netflix_bronze_bad_record`

| คอลัมน์ | ชนิด | คำอธิบาย |
|--------|------|---------|
| คอลัมน์ต้นทางทั้งหมด | หลายชนิด | บันทึกต้นฉบับ |
| `reason` | ARRAY<STRING> | รายการความล้มเหลวในการตรวจสอบ |
| `batch_id` | INT | ตัวระบุ batch |
| `load_dt` | DATE | วันที่ปฏิเสธ |
| `load_dttm` | TIMESTAMP | เวลาที่ปฏิเสธ |

### แผนภาพ Star Schema

```text

       [ ตารางมิติย่อย: ผู้กำกับ ]                       [ ตารางมิติย่อย: นักแสดง ]
          dim_directors_silver                             dim_cast_silver
         ┌──────────────────────┐                         ┌──────────────────┐
         │ PK  │ director_sk    │                         │ PK  │ cast_sk    │
         │     │ director_id    │                         │     │ cast_id    │
         │     │ director_name  │                         │     │ cast_name  │
         └──────────┬───────────┘                         └────────┬─────────┘
                    │ (1)                                          │ (1)
                    ▼                                              ▼
                    ∞ (Many)                                       ∞ (Many)
       [ ตารางสะพานเชื่อมสัมพันธ์ ]                       [ ตารางสะพานเชื่อมสัมพันธ์ ]
          bridge_title_director                            bridge_title_cast
          ┌──────────────────────┐                         ┌──────────────────┐
          │ FK  │ show_id        │                         │ FK  │ show_id    │
          │ FK  │ director_id    │                         │ FK  │ cast_id    │
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
                         │ BK        │ show_id                    │
                         │           │ type, title, date_added    │
                         │           │ release_year, rating       │
                         │           │ duration, description      │
                         │ SCD       │ hash_key, hash_value       │
                         │ Type 2    │ active_flag                │
                         │           │ start_date, end_date       │
                         └───────────┬───────────────┬────────────┘
                     ∞ (Many)        │               │ ∞ (Many)
                                     ▼               ▼
       [ ตารางสะพานเชื่อมสัมพันธ์ ]                       [ ตารางสะพานเชื่อมสัมพันธ์ ]
          bridge_title_country                              bridge_title_category
          ┌──────────────────────┐                         ┌──────────────────┐
          │ FK  │ show_id        │                         │ FK  │ show_id    │
          │ FK  │ country_id     │                         │ FK  │ category_id│
          └──────────┬───────────┘                         └────────┬─────────┘
                     │                                              │
                     │ (1)                                          │ (1)
                     ▼                                              ▼
       [ ตารางมิติย่อย: ประเทศ ]                        [ ตารางมิติย่อย: หมวดหมู่ ]
          dim_countries_silver                             dim_categories_silver
         ┌──────────────────────┐                         ┌──────────────────┐
         │ PK  │ country_sk     │                         │ PK  │ category_sk│
         │     │ country_id     │                         │     │ category_id│
         │     │ country_name   │                         │     │ category_nm│
         └────────────────────────┘                        └──────────────────┘
```

**คำอธิบายสัญลักษณ์**:
- **PK** = Primary Key (คีย์หลัก)
- **FK** = Foreign Key (คีย์นอก)
- **BK** = Business Key (คีย์ทางธุรกิจ)
- **(1)** = ด้านที่มีความสัมพันธ์แบบหนึ่ง
- **∞ (Many)** = ด้านที่มีความสัมพันธ์แบบหลาย

---

## 🚀 เริ่มต้นใช้งาน

### ข้อกำหนดเบื้องต้น

- Databricks workspace พร้อม Unity Catalog เปิดใช้งาน
- สิทธิ์เข้าถึง S3 หรือ cloud storage (สำหรับ Auto Loader)
- Python 3.10+ พร้อม PySpark
- Delta Lake 2.0+

### ขั้นตอนการตั้งค่า

#### 1. สร้างตารางการตั้งค่า

```python
# สร้าง schema และตารางการตั้งค่า
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.netflix")

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

# ใส่การตั้งค่าไปป์ไลน์ Netflix
spark.sql("""
INSERT INTO workspace.netflix.config_table VALUES (
    'netflix',
    's3://your-bucket/netflix/',  -- หรือเส้นทางไฟล์
    true,
    ',',
    'netflix',
    map(
        'show_id', 'string',
        'type', 'string',
        'title', 'string',
        'director', 'string',
        'cast', 'string',
        'country', 'string',
        'date_added', 'date',
        'release_year', 'int',
        'rating', 'string',
        'duration', 'string',
        'listed_in', 'string',
        'description', 'string'
    ),
    array('show_id'),
    'overwrite'
)
""")
```

#### 2. เรียกใช้ชั้น Bronze

```python
# ตัวเลือก A: โหมด Batch (โหลดครั้งเดียว)
bronze = BronzeLayer.from_config_table("netflix")
raw_df = bronze.read_from_file()
bronze.load_to_bronze_table(raw_df)

# ตัวเลือก B: โหมด Streaming (Auto Loader)
bronze = BronzeLayer.from_config_table("netflix")
bronze.s3_auto_loader(checkpoint_location="/Volumes/workspace/netflix/checkpoint_dir/netflix_bronze/")
```

#### 3. เรียกใช้ชั้น Silver

```python
silver = SilverLayer.from_config_table("netflix")
silver.process_cdf_stream_to_silver(
    checkpoint_location="/Volumes/workspace/netflix/checkpoint_dir/netflix_silver/"
)
```

#### 4. เรียกใช้ชั้น Gold

```python
gold = GoldLayer.from_config_table("netflix")
gold.run_gold_pipeline()
```

---

## 🔧 การตั้งค่า

### การตั้งค่า Auto Loader

```python
# การตั้งค่า Auto Loader ของชั้น Bronze
checkpoint_location = "/Volumes/workspace/netflix/checkpoint_dir/netflix_bronze/"
schema_location = "/Volumes/workspace/netflix/checkpoint_dir/netflix_bronze_schema/"

# ตัวเลือกสำคัญ:
- cloudFiles.format: "csv"                          # รูปแบบไฟล์
- cloudFiles.schemaEvolutionMode: "rescue"         # จัดการการเปลี่ยนแปลง schema
- pathGlobFilter: "*.csv"                          # รูปแบบไฟล์
- mergeSchema: "true"                              # อนุญาตการอัปเดต schema
- trigger(availableNow=True)                       # Batch-style streaming สำหรับประหยัดต้นทุน
```

### Trigger ของไปป์ไลน์

**ชั้น Bronze**:
- **Batch**: Trigger ด้วยตนเองผ่าน `load_to_bronze_table()`
- **Streaming**: `trigger(availableNow=True)` - ประมวลผลข้อมูลที่มีทั้งหมดแล้วหยุด

**ชั้น Silver**:
- **แบบเพิ่มหน่วย**: CDF-based streaming พร้อม checkpoint
- **Trigger**: `trigger(availableNow=True)` หรือ continuous streaming

**ชั้น Gold**:
- **Full Refresh**: Trigger ด้วยตนเองผ่าน `run_gold_pipeline()`
- **โหมด**: `overwrite` - แทนที่ตารางทั้งหมด

---

## 🧪 ระบบทดสอบ

### ชุดทดสอบ: `SilverLayerTests`

**การทดสอบครอบคลุม 5 แบบ**:

1. **test_data_quality_validation()** - ตรวจสอบการตรวจสอบคุณภาพ 8 ขั้นตอน
   - ✅ Trim, type casting, invalid detection, null keys, duplicates

2. **test_star_schema_creation()** - ตรวจสอบโครงสร้าง 9 ตาราง
   - ✅ 1 มิติหลัก + 4 มิติย่อย + 4 ตาราง bridge

3. **test_scd_type2_change_detection()** - ทดสอบการติดตามประวัติ
   - ✅ การอัปเดต active flag, start/end dates, hash-based CDC

4. **test_bad_record_handling()** - ตรวจสอบโลจิกการกักกัน
   - ✅ การติดตามเหตุผล, batch ID, rejection timestamp

5. **test_incremental_processing()** - ทดสอบการผสานรวม CDF
   - ✅ การจัดการ checkpoint, ประมวลผลเฉพาะบันทึกใหม่/เปลี่ยนแปลง

### การรันการทดสอบ

```python
# เริ่มต้นชุดทดสอบ
tests = SilverLayerTests(
    silver_obj=silver,
    main_dim_table="workspace.netflix.dim_titles_silver",
    sub_dim_tables=[
        "workspace.netflix.dim_cast_silver",
        "workspace.netflix.dim_directors_silver",
        "workspace.netflix.dim_countries_silver",
        "workspace.netflix.dim_categories_silver"
    ],
    bridge_tables=[
        "workspace.netflix.bridge_title_cast_silver",
        "workspace.netflix.bridge_title_director_silver",
        "workspace.netflix.bridge_title_country_silver",
        "workspace.netflix.bridge_title_category_silver"
    ],
    bad_record_table="workspace.netflix.netflix_bronze_bad_record"
)

# รันการทดสอบทั้งหมด
tests.test_data_quality_validation()
tests.test_star_schema_creation()
tests.test_scd_type2_change_detection()
tests.test_bad_record_handling()
tests.test_incremental_processing()
```

**ผลลัพธ์ที่คาดหวัง**: ✅ การทดสอบทั้งหมด 5 แบบผ่าน (อัตราผ่าน 100%)

---

## 💡 ตัวอย่างการใช้งาน

### ตัวอย่าง 1: การตั้งค่าไปป์ไลน์เริ่มต้น

```python
from dataclasses import dataclass
from pyspark.sql.functions import *

# 1. รันชั้น Bronze (Auto Loader)
bronze = BronzeLayer.from_config_table("netflix")
bronze.s3_auto_loader()

# 2. รันชั้น Silver (คุณภาพ + Star Schema)
silver = SilverLayer.from_config_table("netflix")
silver.process_cdf_stream_to_silver()

# 3. รันชั้น Gold (การรวมกลุ่มทางธุรกิจ)
gold = GoldLayer.from_config_table("netflix")
gold.run_gold_pipeline()

# 4. ตรวจสอบผลลัพธ์
spark.table("workspace.netflix.dim_titles_silver").display()
spark.table("workspace.netflix.netflix_content_by_cast_gold").display()
```

### ตัวอย่าง 2: Query Star Schema

```python
# ดึงเรื่องทั้งหมดพร้อมนักแสดง
spark.sql("""
    SELECT 
        t.title,
        t.type,
        t.release_year,
        c.cast_name
    FROM workspace.netflix.dim_titles_silver t
    INNER JOIN workspace.netflix.bridge_title_cast_silver b 
        ON t.show_id = b.show_id
    INNER JOIN workspace.netflix.dim_cast_silver c 
        ON b.cast_id = c.cast_id
    WHERE t.active_flag = TRUE
    ORDER BY t.release_year DESC
    LIMIT 100
""").display()
```

### ตัวอย่าง 3: ติดตามคุณภาพข้อมูล

```python
# ตรวจสอบสถิติข้อมูลไม่ถูกต้อง
spark.sql("""
    SELECT 
        batch_id,
        load_dt,
        COUNT(*) as bad_record_count,
        explode(reason) as failure_reason
    FROM workspace.netflix.netflix_bronze_bad_record
    GROUP BY batch_id, load_dt, reason
    ORDER BY batch_id DESC
""").display()
```

### ตัวอย่าง 4: วิเคราะห์แนวโน้มทางธุรกิจ

```python
# Query ชั้น Gold สำหรับแนวโน้มเนื้อหารายปี
spark.sql("""
    SELECT 
        release_year,
        type,
        total_title,
        LAG(total_title, 1) OVER (PARTITION BY type ORDER BY release_year) as prev_year_count,
        (total_title - LAG(total_title, 1) OVER (PARTITION BY type ORDER BY release_year)) as yoy_change
    FROM workspace.netflix.netflix_yearly_content_trends_gold
    WHERE release_year >= 2015
    ORDER BY release_year DESC, type
""").display()
```

---

## 📈 ประสิทธิภาพ

### ประสิทธิภาพไปป์ไลน์

| เมตริก | ค่า | หมายเหตุ |
|--------|-----|----------|
| **การรับข้อมูล Bronze** | 317+ บันทึก/วินาที | Auto Loader ด้วย trigger `availableNow` |
| **การแปลง Silver** | ไปป์ไลน์ 8 ขั้นตอน | เสร็จสมบูรณ์ใน 45-60 วินาทีสำหรับ 17K แถว |
| **การรวมกลุ่ม Gold** | ต่ำกว่า 5 วินาที | โหมด full refresh overwrite |
| **ตารางทั้งหมดที่สร้าง** | 14 ตาราง | 1 Bronze + 10 Silver + 2 Gold + 1 Audit |
| **ขนาด Star Schema** | 9 ตาราง | 1 หลัก + 4 มิติย่อย + 4 bridges |

### เมตริกคุณภาพข้อมูล

| ขั้นตอน | บันทึกที่ประมวลผล | บันทึกไม่ถูกต้อง | อัตราผ่าน |
|---------|-------------------|-----------------|----------|
| Trim & Cast | 17,039 | 31 | 99.82% |
| การตรวจจับค่าไม่ถูกต้อง | 17,008 | 0 | 100% |
| การตรวจจับ Null Key | 17,008 | 0 | 100% |
| การตรวจจับข้อมูลซ้ำ | 17,008 | 1,030 | 93.95% |
| **บันทึกสะอาดสุดท้าย** | **15,978** | **1,061** | **93.77%** |

### การติดตาม SCD Type 2

- **การโหลดเริ่มต้น**: 15,978 บันทึก active
- **การตรวจจับการเปลี่ยนแปลง**: การเปรียบเทียบ Hash แบบ SHA-256
- **บันทึกประวัติ**: เก็บรักษาด้วย `end_date` และ `active_flag = False`
- **ประสิทธิภาพ Query**: เพิ่มประสิทธิภาพด้วยตัวกรอง `active_flag = True`

---

## 🎓 แนวทางปฏิบัติที่ดี

### 1. การจัดการการตั้งค่า

✅ **รวมศูนย์การตั้งค่า** ใน `config_table`  
✅ **ใช้ factory methods** (`from_config_table()`) เพื่อความสอดคล้อง  
✅ **ควบคุมเวอร์ชัน** การเปลี่ยนแปลงการตั้งค่า  
✅ **เอกสาร schema mappings** ใน data dictionaries  

### 2. คุณภาพข้อมูล

✅ **กักกันข้อมูลไม่ถูกต้อง** - ไม่ทิ้งอย่างเงียบๆ  
✅ **ติดตามเหตุผลการปฏิเสธ** พร้อมบันทึกตรวจสอบโดยละเอียด  
✅ **ติดตามแนวโน้มบันทึกไม่ถูกต้อง** ตามเวลา  
✅ **แจ้งเตือนเมื่อละเมิดเกณฑ์คุณภาพ**  

### 3. การประมวลผลแบบเพิ่มหน่วย

✅ **เปิดใช้งาน CDF** ในตาราง Bronze ทั้งหมด  
✅ **ใช้ checkpoints** สำหรับ streaming fault tolerance  
✅ **ชอบ `trigger(availableNow=True)`** สำหรับ batch streaming ที่ประหยัดต้นทุน  
✅ **ติดตาม checkpoint lag** เพื่อตรวจจับความล่าช้าของไปป์ไลน์  

### 4. การจัดการ SCD Type 2

✅ **query ด้วย `active_flag = TRUE` เสมอ** สำหรับสถานะปัจจุบัน  
✅ **ใช้การเปรียบเทียบ hash** สำหรับการตรวจจับการเปลี่ยนแปลงที่มีประสิทธิภาพ  
✅ **เก็บบันทึกประวัติ** สำหรับการตรวจสอบและ query ย้อนเวลา  
✅ **จัดทำดัชนีบน surrogate keys** สำหรับประสิทธิภาพ join  

### 5. การเพิ่มประสิทธิภาพชั้น Gold

✅ **รวมกลุ่มล่วงหน้า** เมตริกทางธุรกิจทั่วไป  
✅ **Denormalize** สำหรับประสิทธิภาพ query ของ dashboard  
✅ **กรองสำหรับบันทึก active เท่านั้น** ในการรวมกลุ่ม  
✅ **ใช้ full refresh** (`overwrite`) เพื่อความเรียบง่าย  
✅ **แบ่ง Partition** ตาราง Gold ขนาดใหญ่ตามวันที่หรือมิติหลัก  

### 6. การปรับใช้ Production

✅ **จัดตารางไปป์ไลน์** ตามลำดับ (Bronze → Silver → Gold)  
✅ **ใช้การแจ้งเตือน** สำหรับความล้มเหลวของไปป์ไลน์  
✅ **ติดตามเมตริกประสิทธิภาพ** (throughput, latency)  
✅ **ตั้งค่าการติดตาม data lineage**  
✅ **เอกสารการพึ่งพาตาราง** และตารางการรีเฟรช  

---

## 🐛 การแก้ไขปัญหา

### ปัญหาทั่วไป

#### ปัญหา 1: Auto Loader ไม่ตรวจจับไฟล์ใหม่

**อาการ**: ไฟล์ใหม่ในโฟลเดอร์ S3 ไม่ถูกประมวลผล

**สาเหตุ**:
- Checkpoint location ประมวลผลไฟล์เหล่านั้นแล้ว
- `pathGlobFilter` ไม่ตรงกับรูปแบบไฟล์
- โหมด Schema evolution ไม่เข้ากันกับคอลัมน์ใหม่

**แก้ไข**:
```python
# ตัวเลือก A: รีเซ็ต checkpoint (ระวัง: ประมวลผลไฟล์ทั้งหมดใหม่)
dbutils.fs.rm("/Volumes/workspace/netflix/checkpoint_dir/netflix_bronze/", recurse=True)

# ตัวเลือก B: ตรวจสอบรูปแบบไฟล์
bronze = BronzeLayer.from_config_table("netflix")
bronze.s3_auto_loader(checkpoint_location="<new_checkpoint_path>")
```

#### ปัญหา 2: SCD Type 2 ไม่สร้างเวอร์ชันใหม่

**อาการ**: การเปลี่ยนแปลงไม่ปรากฏเป็นบันทึกใหม่ด้วย `active_flag = TRUE`

**สาเหตุ**:
- ค่า Hash ไม่เปลี่ยนแปลง (คอลัมน์ไม่รวมใน hash)
- `load_main_dimension()` ไม่ทำงานหลังการเปลี่ยนแปลงข้อมูล
- Business key ไม่ตรงกันใน join logic

**แก้ไข**:
```python
# ตรวจสอบการสร้าง hash รวมคอลัมน์ข้อมูลทั้งหมด
silver = SilverLayer.from_config_table("netflix")

# ตรวจสอบคอลัมน์ที่รวมใน hash_value
# ควรยกเว้น: keys, exploded columns (cast, director, country, listed_in), _sk
hash_columns = [col for col in silver.data_col 
                if col not in silver.keys and col not in ["cast", "director", "country", "listed_in"]]
print("คอลัมน์ใน hash_value:", hash_columns)

# รันชั้น Silver ใหม่
silver.process_cdf_stream_to_silver()
```

#### ปัญหา 3: ข้อมูลไม่ถูกต้องไม่ถูกจับ

**อาการ**: ข้อมูลไม่ถูกต้องปรากฏในชั้น Silver

**สาเหตุ**:
- ขั้นตอนการตรวจสอบคุณภาพถูกข้าม
- กฎการตรวจสอบไม่ตรงกับรูปแบบข้อมูล
- ตารางบันทึกไม่ถูกต้องไม่เริ่มต้น

**แก้ไข**:
```python
# ตรวจสอบตารางบันทึกไม่ถูกต้องมีอยู่
spark.sql("SELECT * FROM workspace.netflix.netflix_bronze_bad_record LIMIT 10").display()

# ตรวจสอบกฎการตรวจสอบ
silver = SilverLayer.from_config_table("netflix")
print("กฎค่าไม่ถูกต้อง:", silver.invalid_rule)

# รันการตรวจสอบคุณภาพด้วยตนเองในตัวอย่าง
from pyspark.sql.functions import col
bronze_df = spark.table("workspace.netflix.netflix_bronze")
invalid_df = silver.get_invalid_record(bronze_df)
invalid_df.display()
```

#### ปัญหา 4: ข้อผิดพลาด Spark Connect Serverless (SPARK-55448)

**อาการ**: ข้อผิดพลาด `STATE_CONSISTENCY` หรือ `XXSC0` ระหว่าง Auto Loader

**สาเหตุ**: บั๊ก synchronization ที่รู้จักของ Spark Connect

**แก้ไข**:
```python
# จัดการแล้วใน BronzeLayer.s3_auto_loader()
# ข้อผิดพลาดถูกจับและถือว่าสำเร็จ
# ตรวจสอบข้อมูลโหลดสำเร็จ:
spark.table("workspace.netflix.netflix_bronze").count()
```

#### ปัญหา 5: ชั้น Gold ขาดบันทึก

**อาการ**: ตาราง Gold มีบันทึกน้อยกว่าที่คาดหวัง

**สาเหตุ**:
- การกรองสำหรับ `active_flag = TRUE` ยกเว้นบันทึกประวัติ
- Bridge table joins ขาดความสัมพันธ์บางอย่าง
- ข้อมูลยังไม่แพร่กระจายจากชั้น Silver

**แก้ไข**:
```python
# ตรวจสอบบันทึก active กับ inactive ใน Silver
spark.sql("""
    SELECT 
        active_flag,
        COUNT(*) as record_count
    FROM workspace.netflix.dim_titles_silver
    GROUP BY active_flag
""").display()

# ตรวจสอบชั้น Gold สร้างใหม่หลังการอัปเดต Silver
gold = GoldLayer.from_config_table("netflix")
gold.run_gold_pipeline()
```

#### ปัญหา 6: การทดสอบล้มเหลว

**อาการ**: การทดสอบ SCD Type 2 ล้มเหลวด้วย "ข้อมูลทดสอบยังคงมีอยู่"

**สาเหตุ**: การรันการทดสอบก่อนหน้าทิ้งบันทึกทดสอบในตาราง Silver

**แก้ไข**:
```python
# ล้างบันทึกทดสอบเก่า
spark.sql("""
DELETE FROM workspace.netflix.dim_titles_silver 
WHERE show_id LIKE 'TEST_SCD_%'
""")

# รันการทดสอบใหม่
tests = SilverLayerTests(...)
tests.test_scd_type2_change_detection()
```

---

## 🤝 การมีส่วนร่วม

### เวิร์กโฟลว์การพัฒนา

1. **Fork notebook** หรือสร้างเวอร์ชันใหม่
2. **ทำการเปลี่ยนแปลง** ในสภาพแวดล้อมการพัฒนาของคุณ
3. **รันชุดทดสอบเต็ม** เพื่อตรวจสอบการเปลี่ยนแปลง
4. **อัปเดตเอกสาร** หากเพิ่มฟีเจอร์ใหม่
5. **ส่งเพื่อตรวจสอบ** (หากเป็นโปรเจคร่วมกัน)

### มาตรฐานโค้ด

✅ ใช้แนวทาง PEP 8 style  
✅ เพิ่ม docstrings ในทุกเมธอด  
✅ รวม unit tests สำหรับฟีเจอร์ใหม่  
✅ อัปเดต README สำหรับการเปลี่ยนแปลงที่สำคัญ  
✅ ใช้ type hints ที่เหมาะสม  
✅ ปฏิบัติตามรูปแบบ dataclass สำหรับการตั้งค่า  

### ข้อกำหนดการทดสอบ

- การทดสอบทั้งหมด 5 แบบต้องผ่าน
- ไม่มีการทำลาย schema ในชั้น Silver
- รักษาเกณฑ์มาตรฐานประสิทธิภาพ
- รักษาความเข้ากันได้แบบย้อนหลัง

---

## 📚 แหล่งข้อมูลเพิ่มเติม

### เอกสาร

- [Databricks Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Databricks Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Change Data Feed Guide](https://docs.databricks.com/delta/delta-change-data-feed.html)
- [SCD Type 2 Best Practices](https://www.databricks.com/blog/2022/08/22/dimensional-modeling-delta-lake.html)

### Notebooks ที่เกี่ยวข้อง

- `framework.ipynb` - การใช้งานไปป์ไลน์หลัก (BronzeLayer, SilverLayer, GoldLayer)
- `silver_layer_tests.py` - ชุดทดสอบครอบคลุม

### การสนับสนุน

สำหรับคำถามหรือปัญหา:
1. ตรวจสอบส่วนการแก้ไขปัญหาข้างต้น
2. ตรวจสอบผลการทดสอบสำหรับรายละเอียดข้อผิดพลาด
3. ตรวจสอบตารางบันทึกไม่ถูกต้องสำหรับปัญหาคุณภาพข้อมูล
4. ปรึกษาเอกสาร Databricks

---

## 📝 ใบอนุญาต

โปรเจคนี้เป็นส่วนหนึ่งของโปรแกรมการฝึกอบรม **Databricks for Data Engineers Bootcamp**

---

## 🎉 การรับรอง

**สร้างด้วย**:
- Databricks Unified Analytics Platform
- Apache Spark 3.x
- Delta Lake พร้อม Change Data Feed
- Python 3.10+ พร้อม dataclasses
- Databricks Auto Loader

**สถาปัตยกรรม**:
- Medallion Architecture (Bronze/Silver/Gold)
- Star Schema Design (รูปแบบ 1+4+4)
- การใช้งาน SCD Type 2
- Hash-based CDC

**ฟีเจอร์หลัก**:
- เฟรมเวิร์กแบบ dataclass ที่ปรับแต่งได้
- ไปป์ไลน์คุณภาพข้อมูล 8 ขั้นตอน
- การประมวลผลแบบเพิ่มหน่วยด้วย CDF
- การทดสอบระดับ production
- การวัดประสิทธิภาพ

---

**อัปเดตล่าสุด**: มกราคม 2026  
**เวอร์ชัน**: 2.0  
**สถานะ**: พร้อมใช้งาน Production ✅

---

*ขอให้สนุกกับการทำ Data Engineering! 🚀*
