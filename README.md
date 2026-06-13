# Databricks-for-Data-Engineers-Bootcamp2
This repository for project in Databricks for Data Engineers Bootcamp2
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
                                                             (จากคอลัมน์ listed_in)