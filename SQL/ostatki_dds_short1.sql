-- ============================================================
-- Запрос формирует витрину по движению и остаткам денежных средств:
--   * по календарю дат,
--   * по организациям,
--   * по корневым статьям ДДС,
--   * по типам операций: Поступление, Выплата, Оборот,
--     а также остаткам на начало и конец.
-- ============================================================

WITH 
-- Календарь: все даты начиная с 2024-12-31 и до сегодняшнего дня включительно
calendar AS (
    SELECT 
        toDate('2024-12-31') + number AS date_col
    FROM 
        numbers(dateDiff('day', toDate('2024-12-31'), today() + 1))
),

-- Справочник организаций: объединяем уникальные организации из остатков и движений
organizations AS (
    SELECT  
        Organizatsiya 
    FROM 
        OstatkiDeneg2
    UNION DISTINCT
    SELECT  
        Organizatsiya 
    FROM 
        DvizhenieDS2
), 

-- Корневые папки статей ДДС:
--   * для внутреннего перемещения отдельное значение,
--   * если иерархия не найдена — 'не известна',
--   * иначе берём root_folder из справочника иерархии.
movement_root_folders AS (
    SELECT DISTINCT
        CASE 
            WHEN dd.StatyaDDS = 'Внутреннее перемещение денежных средств'
            THEN dd.StatyaDDS
            WHEN g.root_folder IS NULL
            THEN 'не известна'
            ELSE g.root_folder
        END AS root_folder
    FROM 
        DvizhenieDS2 dd
    LEFT JOIN 
        StatiDDS_Hierarchy g 
        ON dd.StatyaDDSID = g.StatyaID
), 

-- Типы движений, которые должны присутствовать в итоговой витрине
movement_types AS (
    SELECT 'Поступление' AS type_value
    UNION ALL
    SELECT 'Выплата' AS type_value
    UNION ALL
    SELECT 'Оборот' AS type_value
),

-- Типы остатков, которые должны присутствовать в итоговой витрине
remainder_types AS (
    SELECT 'Остаток на начало' AS type_value
    UNION ALL
    SELECT 'Остаток на конец' AS type_value
), 

-- Полный набор комбинаций для движений:
-- каждая дата × организация × корневая папка × тип движения.
-- Нужен, чтобы в отчёте были и нулевые значения.
movement_combinations AS (
    SELECT 
        c.date_col AS date_col,
        o.Organizatsiya AS Organizatsiya,
        r.root_folder AS root_folder,
        t.type_value AS type_value
    FROM calendar c
    CROSS JOIN organizations o
    CROSS JOIN movement_root_folders r
    CROSS JOIN movement_types t
), 

-- Полный набор комбинаций для остатков:
-- каждая дата × организация × тип остатка.
-- Здесь type_value остатка кладём в root_folder, а type_value делаем пробелом,
-- чтобы структура совпадала с движенческими строками.
remainder_combinations AS (
    SELECT 
        c.date_col AS date_col,
        o.Organizatsiya AS Organizatsiya,
        t.type_value AS root_folder,       -- переносим значение type_value в root_folder
        ' ' AS type_value                   -- type_value становится пустым
    FROM calendar c
    CROSS JOIN organizations o
    CROSS JOIN remainder_types t
), 

-- Объединяем все возможные комбинации:
-- движения + остатки. Это "скелет" итоговой витрины.
all_combinations AS (
    SELECT 
        date_col, 
        Organizatsiya, 
        root_folder, 
        type_value 
    FROM 
        movement_combinations
    UNION ALL
    SELECT 
        date_col, 
        Organizatsiya, 
        root_folder, 
        type_value 
    FROM 
        remainder_combinations
),

-- ============================================================
-- Новый блок расчета остатков
-- ============================================================

-- Справочник расчётных счетов / депозитов:
-- для депозитов формируем отдельное название вида 'Депозит <Организация>'.
dim AS (
    SELECT DISTINCT 
        od.Organizatsiya,
        CASE 
            WHEN od.EtoDepozit THEN concat('Депозит', ' ', od.Organizatsiya)
            ELSE od.RaschetniiSchet
        END AS RaschetniiSchet
    FROM 
        OstatkiDeneg2 od 
    UNION DISTINCT
    SELECT DISTINCT 
        dd.Organizatsiya,
        CASE 
            WHEN dd.EtoDepozit THEN concat('Депозит', ' ', dd.Organizatsiya)
            ELSE dd.RaschetniiSchet
        END AS RaschetniiSchet
    FROM 
        DvizhenieDS2 dd 
),

-- Полная сетка: организация × расчётный счёт × дата.
-- Нужна для заполнения остатков на все даты, даже если в источнике нет записи.
full_grid AS (
    SELECT 
        d.Organizatsiya,
        d.RaschetniiSchet,
        c.date_col 
    FROM dim d
    CROSS JOIN calendar c
),

-- Сырые остатки из источника с приведением названия счёта к единому виду
ostatki_raw AS (
    SELECT 
        date(od.PeriodMSK) AS date_col,
        od.Organizatsiya,
        CASE 
            WHEN od.EtoDepozit
            THEN concat('Депозит', ' ', od.Organizatsiya)
            ELSE od.RaschetniiSchet
        END AS RaschetniiSchet,
        od.SummaOstatok
    FROM OstatkiDeneg2 od
),

-- Заполняем остатки вперёд: если на дату остатка нет,
-- берём последнее известное значение по этому счёту и организации.
with_filled AS (
    SELECT
        f.date_col,
        f.Organizatsiya,
        f.RaschetniiSchet,
        o.SummaOstatok,
        last_value(o.SummaOstatok)
            OVER (
                PARTITION BY f.Organizatsiya, f.RaschetniiSchet
                ORDER BY f.date_col
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS SummaOstatok_filled
    FROM full_grid f
    LEFT JOIN ostatki_raw o 
        ON f.date_col        = o.date_col 
       AND f.Organizatsiya   = o.Organizatsiya
       AND f.RaschetniiSchet = o.RaschetniiSchet
),

-- Рассчитываем остаток на начало и конец дня:
--   * начало — предыдущее заполненное значение остатка,
--   * конец — текущее заполненное значение остатка.
ostatki_begin_end AS (
    SELECT
        wf.date_col AS date_col,
        wf.Organizatsiya,
        coalesce(
            lagInFrame(wf.SummaOstatok_filled, 1, 0) OVER (
                PARTITION BY wf.Organizatsiya, wf.RaschetniiSchet
                ORDER BY wf.date_col
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ),
            0
        ) AS SummaOstatok_BEGIN,
        coalesce(wf.SummaOstatok_filled, 0) AS SummaOstatok_END    
    FROM with_filled wf
),

-- Агрегируем остатки по организации и дате:
-- отдельно на начало и отдельно на конец.
ostatki1 AS (
    SELECT 
        date_col,
        Organizatsiya,
        'Остаток на начало' AS type_value,
        SUM(SummaOstatok_BEGIN) AS summa
    FROM ostatki_begin_end
    GROUP BY date_col, Organizatsiya
    UNION ALL
    SELECT 
        date_col,
        Organizatsiya,
        'Остаток на конец' AS type_value,
        SUM(SummaOstatok_END) AS summa
    FROM ostatki_begin_end
    GROUP BY date_col, Organizatsiya
),

-- Итоговая агрегация фактических данных:
--   1) поступления и выплаты по статьям ДДС,
--   2) остатки на начало и конец,
--   3) обороты как разница между поступлениями и выплатами.
original_aggregated AS (
    -- Поступления и выплаты по статьям ДДС
    SELECT
        toDate(dd.PeriodMSK) AS date_col,
        CASE 
            WHEN dd.StatyaDDS = 'Внутреннее перемещение денежных средств'
            THEN dd.StatyaDDS
            WHEN g.root_folder IS NULL
            THEN 'не известна'
            ELSE g.root_folder
        END AS root_folder,
        dd.Organizatsiya,
        CASE
            WHEN dd.EtoPostuplenie THEN 'Поступление'
            ELSE 'Выплата'
        END AS type_value,
        sum(dd.Summa) AS summa
    FROM DvizhenieDS2 dd 
    LEFT JOIN StatiDDS_Hierarchy g 
        ON dd.StatyaDDSID = g.StatyaID
    GROUP BY 1, 2, 3, 4

    UNION ALL

    -- Остатки на начало и конец:
    -- type_value из ostatki1 переносим в root_folder,
    -- а type_value делаем пробелом для совместимости с all_combinations.
    SELECT
        o.date_col,
        o.type_value AS root_folder,   -- здесь type_value становится root_folder
        o.Organizatsiya,
        ' ' AS type_value,              -- type_value становится пустым
        o.summa
    FROM ostatki1 o

    UNION ALL

    -- Обороты: поступление со знаком +, выплата со знаком -
    SELECT
        toDate(dd.PeriodMSK) AS date_col,
        CASE 
            WHEN dd.StatyaDDS = 'Внутреннее перемещение денежных средств'
            THEN dd.StatyaDDS
            WHEN g.root_folder IS NULL
            THEN 'не известна'
            ELSE g.root_folder
        END AS root_folder,
        dd.Organizatsiya,
        'Оборот' AS type_value,
        sum(CASE WHEN dd.EtoPostuplenie THEN dd.Summa ELSE -dd.Summa END) AS summa
    FROM DvizhenieDS2 dd 
    LEFT JOIN StatiDDS_Hierarchy g 
        ON dd.StatyaDDSID = g.StatyaID
    GROUP BY 1, 2, 3
)

-- Финальная витрина:
-- берём все возможные комбинации и подклеиваем к ним фактические суммы.
-- Если данных нет — показываем 0.
SELECT
    ac.date_col,
    CASE
        -- Отдельные корневые статьи относим к операционной деятельности
        WHEN ac.root_folder LIKE '%Госпошлина%' 
          OR ac.root_folder LIKE '%Движение ДС по вкладам%' 
          OR ac.root_folder LIKE '%Единый налоговый платеж%' 
        THEN 'Операционная деятельность'
        ELSE ac.root_folder 
    END AS root_folder,
    ac.Organizatsiya,
    ac.type_value,
    coalesce(oa.summa, 0) AS summa
FROM all_combinations ac
LEFT JOIN original_aggregated oa 
    ON ac.date_col       = oa.date_col
   AND ac.root_folder    = oa.root_folder
   AND ac.Organizatsiya  = oa.Organizatsiya
   AND ac.type_value     = oa.type_value
ORDER BY 
    ac.date_col, 
    ac.Organizatsiya, 
    ac.root_folder, 
    ac.type_value;