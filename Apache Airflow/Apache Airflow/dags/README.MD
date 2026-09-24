# 1. DAG: `click_house_work`

Простейший DAG для проверки подключения к ClickHouse.

## Что делает
- Запускается вручную (`schedule_interval=None`).
- Выполняет одну задачу `select_version` через `ClickHouseOperator`.
- Отправляет запрос `SELECT version()` и возвращает версию ClickHouse-сервера.

## Параметры
| Параметр | Значение |
|----------|----------|
| `dag_id` | `click_house_work` |
| `start_date` | вчерашний день (`days_ago(1)`) |
| `schedule_interval` | `None` (только ручной запуск) |
| `clickhouse_conn_id` | `click_house_work` — Airflow-соединение с ClickHouse |

## Назначение
Smoke-тест: убедиться, что соединение `click_house_work` настроено корректно и Airflow может выполнять запросы в ClickHouse.

# 2. DAG: `pp_employee_plan_fact`

Ежедневный ETL-пайплайн: выгрузка факт-плановых показателей сотрудников из MariaDB в ClickHouse с интеллектуальной догрузкой данных.

## Что делает

1. **`check_table_status`** — проверяет состояние таблицы в ClickHouse и определяет режим загрузки:
   - `all` — таблица пустая/не существует → грузим всю историю с 2025-10-01;
   - `update_current` — данные за текущий месяц есть → перезаливаем месяц;
   - `fill_gap` — есть пропуск месяцев → догружаем недостающие (до 12 назад).

2. **`extract_analytics_from_mariadb`** — выгружает данные из MariaDB по SQL-шаблону:
   - факт: продажи, выручка, литры, снеки, фасовка, сертификаты;
   - план: литры, % снеков, % фасовки по сотрудникам и торговым точкам.
   - читает чанками по 50k строк, конвертирует типы, сохраняет в Parquet.

3. **`load_analytics_to_clickhouse`** — загружает данные в ClickHouse:
   - создаёт таблицу `pp_employee_plan_fact` (`MergeTree`, партиционирование по `toYYYYMM(date)`);
   - в режиме `update_current` удаляет данные за месяц перед вставкой;
   - вставляет чанками, логирует итоговую статистику по месяцам.

4. **`cleanup_analytics_files`** — удаляет временные файлы.

## Особенности
- **Retry-механизм** с экспоненциальной задержкой для БД-ошибок.
- **ConnectionManager** — ленивое создание и переиспользование подключений MySQL/ClickHouse.
- **Context manager** для безопасного закрытия соединений.
- **Memory-friendly**: чанковая обработка, `gc.collect()`, fallback на CSV при ошибке Parquet.
- **Уведомления**: Telegram + Email при успехе и ошибке (через `on_success_callback` / `on_failure_callback`).
- **Расписание**: ежедневно (`22 0 * * *`), без catchup, `max_active_runs=1`.

## Источники
- **MariaDB** (`pp`): `sale_orders`, `sale_order_tabular_parts`, `nomenclatures`, `employees`, `usertable_planovye_pokazateli_sotrudniki`
- **ClickHouse** (`click_house_work`): таблица `pp_employee_plan_fact`

## Стек
`Airflow` · `PythonOperator` · `MySqlHook` · `ClickHouseHook` · `pandas` · `pyarrow` · `SQLAlchemy`

# 3. DAG: `pp`

Ежедневный ETL-пайплайн: инкрементальная загрузка данных о продажах из MariaDB в ClickHouse по «скользящему окну» 30 дней, с контролем качества данных.

## Что делает

1. **`check_data_volume`** — оценивает объём данных в источнике и определяет уровень нагрузки (`NORMAL` / `WARNING` / `CRITICAL`), чтобы дальше выбрать размер чанка.

2. **`extract_from_mariadb`** — выгружает продажи из MariaDB за последние 30 дней:
   - обычные позиции (`pay_certificate = 0`);
   - позиции с оплатой сертификатом (`pay_certificate > 0`), где `total_price` распределяется пропорционально количеству позиций в заказе.
   - читает чанками (`25k`/`50k` в зависимости от объёма), конвертирует типы, сохраняет в Parquet (fallback — CSV).

3. **`cleanup_temp_files`** — очистка ClickHouse перед удалением:
   - останавливает зависшие `DELETE`-мутации,
   - сбрасывает системные кеши (`MARK`, `UNCOMPRESSED`, `COMPILED EXPRESSION`),
   - проверяет свободное место на дисках,
   - убивает мутации, висящие более часа.

4. **`delete_window_in_clickhouse`** — удаляет из целевой таблицы данные за последние 30 дней (`ALTER TABLE ... DELETE`), чтобы залить их заново.

5. **`load_window_to_clickhouse`** — загружает данные из Parquet/CSV в ClickHouse:
   - создаёт таблицу `pp_Order_Sales` (`MergeTree`, партиционирование по `toYYYYMM(sbis_created_at)`);
   - итеративно читает батчами по 50k строк, вставляет через `insert_sql`.

6. **`quality_check`** — сверка агрегатов «источник vs цель» по дням:
   - считает выручку в MariaDB и ClickHouse за 30 дней,
   - если относительное расхождение > 0.5% — шлёт алерт в Telegram.

7. **`cleanup_file`** — удаляет временные файлы и `*.part_*` артефакты.

## Особенности
- **Retry-механизм** с экспоненциальной задержкой для ошибок БД.
- **ConnectionManager** — переиспользование соединений MySQL/ClickHouse.
- **Memory-friendly**: чанковая обработка, `gc.collect()`, итеративное чтение Parquet через `ParquetFile.iter_batches`.
- **Уведомления**: Telegram + Email при успехе/ошибке DAG.
- **Расписание**: ежедневно (`15 0 * * *`), без catchup, `max_active_runs=1`.
- **Стратегия загрузки**: delete-window + insert (простое обновление без `ReplacingMergeTree`).

## Источники
- **MariaDB** (`pp`): `sale_orders`, `sale_order_tabular_parts`, `nomenclatures`, `employees`
- **ClickHouse** (`click_house_work`): таблица `pp_Order_Sales`

## Стек
`Airflow` · `PythonOperator` · `MySqlHook` · `ClickHouseHook` · `pandas` · `pyarrow` · `SQLAlchemy`

# 4. DAG: `pp_trend_plan`

Ежедневный ETL-пайплайн: расчёт тренд-плановых показателей по торговым точкам из MariaDB в ClickHouse с интеллектуальной догрузкой и режимом «тренда» для текущего месяца.

## Что делает

1. **`check_table_status`** — проверяет таблицу в ClickHouse и выбирает режим загрузки:
   - `all` — таблица пустая/не существует → грузим всю историю;
   - `update_current` — данные за текущий месяц есть → перезаливаем месяц;
   - `fill_gap` — есть пропуск месяцев → догружаем недостающие (до 12 назад).

2. **`extract_analytics_from_mariadb`** — выгружает данные из MariaDB по SQL-шаблону:
   - **план** из `usertable_planovye_pokazateli_magaziny` (выручка, чеки, литры, снеки, фасовка);
   - **факт** из продаж (`sale_orders` + `sale_order_tabular_parts`), включая распределение сертификатов;
   - **тренд** для текущего месяца: `факт / (день_сегодня − 1) × дней_в_месяце` — экстраполяция к концу месяца;
   - разбивка по категориям: общие показатели, литры (папки 62, 565, 1519–1526, 4857, 5569, 5930), снеки (`root_folder_id = 50`), фасовка (`root_folder_id = 1070`).
   - читает чанками по 50k, сохраняет в Parquet.

3. **`load_analytics_to_clickhouse`** — загрузка в ClickHouse:
   - создаёт таблицу `pp_Trend_Plan` (`MergeTree`, партиционирование по `toYYYYMM(last_day_of_month)`);
   - в режиме `update_current` удаляет данные за текущий месяц перед вставкой;
   - вставляет чанками, логирует статистику (кол-во записей, диапазон дат, кол-во месяцев).

4. **`cleanup_analytics_files`** — удаляет временные файлы.

## Особенности
- **Retry-механизм** с экспоненциальной задержкой для БД-ошибок.
- **ConnectionManager** — переиспользование соединений MySQL/ClickHouse.
- **Memory-friendly**: чанковая обработка, `gc.collect()`, fallback на CSV.
- **Уведомления**: Telegram + Email при успехе/ошибке DAG.
- **Расписание**: ежедневно (`20 0 * * *`), без catchup, `max_active_runs=1`.
- **Тренд-логика**: для текущего месяца факт экстраполируется на весь месяц (`факт / прошедшие_дни × дни_в_месяце`), для прошлых — тренд = факт.

## Результат
| Группа | Поля |
|--------|------|
| Основное | `plan_count_orders`, `trend_count_orders`, `fact_count_orders`, `plan_revenue`, `trend_revenue`, `fact_revenue` |
| Литры | `plan_liters`, `trend_liters`, `fact_liters`, `trend_revenue_liters`, `trend_count_orders_liters` |
| Снеки | `plan_revenue_snacks`, `trend_revenue_snacks`, `fact_revenue_snacks`, `trend_count_orders_snacks` |
| Фасовка | `plan_revenue_packaging`, `trend_revenue_packaging`, `fact_revenue_packaging`, `trend_count_orders_packaging` |
| Мета | `last_day_of_month`, `company_name`, `calculated_at`, `load_batch_id` |

## Источники
- **MariaDB** (`pp`): `usertable_planovye_pokazateli_magaziny`, `sale_orders`, `sale_order_tabular_parts`, `nomenclatures`
- **ClickHouse** (`click_house_work`): таблица `pp_Trend_Plan`

## Стек
`Airflow` · `PythonOperator` · `MySqlHook` · `ClickHouseHook` · `pandas` · `pyarrow` · `SQLAlchemy`
