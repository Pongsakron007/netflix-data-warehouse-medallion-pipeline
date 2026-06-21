# Databricks-for-Data-Engineers-Bootcamp2
# 🎬 Netflix Enterprise Data Engineering Project (Silver Layer)

โปรเจคนี้จัดทำขึ้นเพื่อพัฒนาระบบคลังข้อมูล (Data Warehouse) สำหรับข้อมูลภาพยนตร์และซีรีส์ของ Netflix โดยสร้างขึ้นบนสถาปัตยกรรม **Medallion Architecture** (เปลี่ยนผ่านจาก Bronze สู่ Silver Layer) โดยใช้เทคโนโลยีหลักอย่าง **Databricks, PySpark, และ Delta Lake Structured Streaming** ควบคู่กับฟีเจอร์ **Change Data Feed (CDF)**

---

## 🚀 สถาปัตยกรรมและหลักการออกแบบระบบ (System Design & Principles)

### 1. Structured Streaming & CDF (แทนการใช้ Control Table)
ระบบนี้เปลี่ยนจากการเขียนลอจิกควบคุมเวอร์ชันข้อมูลด้วยมือ (Control Table) มาใช้ขีดความสามารถของ **Delta Change Data Feed (CDF)** ร่วมกับ **Structured Streaming (`readStream`)** ซึ่งข้อดีคือ:
* **Automation:** ปล่อยให้ Spark และระบบ Checkpoint ทำหน้าที่บันทึกสถานะ (Offset) การอ่านข้อมูลให้อยู่ในรูปแบบ Incremental Load โดยอัตโนมัติ
* **Efficiency:** ระบบจะไปดึงมาเฉพาะแถวที่มีการเพิ่ม (Insert) หรือแก้ไข (Update) ในตาราง Bronze เท่านั้น ไม่ต้องกวาดข้อมูลเก่ามาประมวลผลซ้ำ

### 2. ลำดับการทำความสะอาดข้อมูลที่ถูกต้อง (Fail-Early Data Standardization)
เพื่อป้องกันไม่ให้ข้อมูลที่จัดรูปแบบผิดพลาดหลุดเข้าไปสร้างความเสียหายในคลังข้อมูล สายพานนี้จึงจัดลำดับลอจิกใหม่:
* ทำการ `Trim` พื้นที่ว่างและล้างช่องว่างของ String ทั้งหมดตั้งแต่ต้นสายพาน
* ใช้คำสั่ง `try_cast` หรือ `try_to_date` เพื่อทดลองแปลงประเภทข้อมูล (Data Type) ทันที หากแถวใดพัง ค่าจะกลายเป็น `NULL` โดยกลไกธรรมชาติของ Spark ก่อนที่จะส่งเข้าไปยังชุดเครื่องตรวจ Data Quality (DQ) เพื่อคัดแยก

### 3. การเพิ่มประสิทธิภาพระบบ ลด RPC และ Network Overhead
เนื่องจากระบบต้องนำข้อมูลดีก้อนเดียวกันไปกระจายโหลดลงตารางหลักและตารางย่อยรวม 5-6 ตาราง หากปล่อยให้ประมวลผลตามปกติ (Lazy Evaluation) Spark จะต้องวิ่งไปคำนวณลอจิก DQ ใหม่ทุกครั้ง ซึ่งทำให้เกิด **Remote Procedure Call (RPC)** และสร้างภาระให้เครือข่ายสูงมาก
* **แนวทางแก้ไข:** ระบบใช้คำสั่ง **`.cache()`** บนข้อมูลดี (`final_df`) ทันทีหลังจากตรวจ DQ เสร็จ เพื่อล็อกข้อมูลไว้ใน Memory ของ Worker Nodes ทำให้ตารางย่อยดึงข้อมูลไปใช้ได้ทันทีโดยไม่เกิดการประมวลผลซ้ำ และสั่ง **`.unpersist()`** คืนหน่วยความจำเมื่อจบกระบวนการ

### 4. ทำไมตารางหลักถึงชื่อ `dim_titles` ไม่ใช่ตาราง Fact?
* **Fact Table:** ต้องเก็บข้อมูลที่เป็นตัวเลขที่นำไปคำนวณหรือวัดผลเชิงปริมาณได้ (Measures/Metrics) เช่น ยอดขาย ยอดวิว หรือทรานแซกชัน
* **Dimension Table:** เก็บข้อมูลที่เป็นบริบท รายละเอียด หรือคุณลักษณะ (Context/Attributes) เพื่อใช้อธิบายเหตุการณ์ นำไปใช้ Filter หรือ Group By 
* **สรุป:** ข้อมูล Netflix ชุดนี้ประกอบด้วย ชื่อหนัง, ประเภท, ปีที่ฉาย, เรตติ้ง ซึ่งล้วนแต่เป็นข้อมูลคุณลักษณะทั้งสิ้น ในทาง Data Modeling ตารางหลักนี้จึงเป็น **ตารางมิติ (Dimension Table)** และใช้ระบบ **SCD Type 2** เพื่อเก็บประวัติการเปลี่ยนแปลง

---

## 📊 แผนผังการไหลของข้อมูล (Data Pipeline Architecture)

นี่คือเส้นทางการเดินทางของข้อมูลสตรีมมิ่งจากตาราง Bronze ผ่านขั้นตอนการกรองแปลงประเภทและตรวจสอบคุณภาพข้อมูล ก่อนคัดแยกออกเป็นข้อมูลดีและเสียตามลอจิก DQ:

```text
 📥 [ ตารางต้นทาง ] ──>  workspace.netflix.netflix_bronze (ด้วย Change Data Feed)
                                   │
                                   ▼
 🛠️ [ เตรียมผิวข้อมูล ] ──>  trim_data()  (ตัดช่องว่าง String หัวท้าย)
                                   │
                                   ▼
 🔄 [ ลองแปลงประเภท ] ──>  change_data_type() (ใช้ try_to_date และ try_cast)
                                   │  *หากแปลงไม่ผ่าน ข้อมูลจะกลายเป็น NULL ทันที
                                   │
                                   ▼
 🔍 [ ด่านตรวจ DQ ]  ───>  get_invalid_record()  ──> เช็ค Format พังด้วย Regex
                                   ├──>  get_key_null_record() ──> เช็ค Business Key เป็น NULL
                                   └──>  get_dup_record()      ──> เช็คแถวซ้ำ / คีย์ซ้ำข้อมูลไม่ตรง
                                   │
                                   ▼
                       [ get_all_bad_record() ] (รวมศูนย์สรุปเหตุผลความพัง)
                                   │
                                   ├─── ❌ [ ข้อมูลเสีย ] ──> load_bad_record() ──> 📂 netflix_bad_record
                                   │
                                   └─── ✅ [ ข้อมูลดี ]   ──> get_final_result() (ทำ Left-Anti Join)
                                                               │
                                                               ▼
                                                    [ ปั๊มลายนิ้วมือข้อมูล ]
                                                    - hash_key   (จาก show_id)
                                                    - hash_value (มัดรวมทุกคอลัมน์)
                                                               │
                                                               ▼
                                                    🚀 ส่งต่อไปที่คลังข้อมูล (Star Schema)


       [ ตารางมิติย่อย: ผู้กำกับ ]                       [ ตารางมิติย่อย: นักแสดง ]
          dim_directors_silver                             dim_cast_silver
         ┌──────────────────────┐                         ┌──────────────────┐
         │ PK  │ director_id    │                         │ PK  │ cast_id    │
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
                        │ PK        │ show_id                    │
                        │           │ title                      │
                        │           │ type                       │
                        │           │ release_year               │
                        │           │ rating                     │
                        │           │ duration                   │
                        │           │ description                │
                        ├────────────────────────────────────────┤
                        │ Hashing   │ hash_key                   │
                        │           │ hash_value                 │
                        ├────────────────────────────────────────┤
                        │ SCD T2    │ start_date                 │
                        │           │ end_date                   │
                        │           │ is_current                 │
                        └────────────────────────────────────────┘
                                      ▲              ▲
                              ∞ (Many)│              │ ∞ (Many)
                    ┌───────────────┘              └───────────────┐
                    │                                              │
                    │                                              │
         ┌──────────┴───────────┐                         ┌────────┴─────────┐
         │ FK  │ show_id        │                         │ FK  │ show_id    │
         │ FK  │ country_id     │                         │ FK  │ category_id│
         └──────────────────────┘                         └──────────────────┘
          bridge_title_country                             bridge_title_category
                    ▲                                              ▲
                    │ ∞ (Many)                                     │ ∞ (Many)
                    │ (1)                                          │ (1)
         ┌──────────┴───────────┐                         ┌────────┴─────────┐
         │ PK  │ country_id     │                         │ PK  │ category_id│
         │     │ country_name   │                         │     │category_name│
         └──────────────────────┘                         └──────────────────┘
        [ ตารางมิติย่อย: ประเทศผลิต ]                    [ ตารางมิติย่อย: หมวดหมู่หนัง ]
          dim_countries_silver                            dim_categories_silver

```