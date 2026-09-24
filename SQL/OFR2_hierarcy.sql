-- ============================================================
-- Запрос формирует отчёт по финансовым результатам (ОФР):
--   * собирает данные из разных источников (ОФР, налоги, доходы/расходы,
--     финансовая и инвестиционная деятельность),
--   * раскладывает строки по уровням иерархии (level1, level2, level3),
--   * агрегирует суммы и считает итоги по прибыли,
--   * подтягивает значение за аналогичный период прошлого года (Prev_year_summa).
-- ============================================================

with 
-- Иерархия строк отчёта: раскладываем дерево на 4 уровня.
-- leaf — конечный (самый глубокий) узел ветки, по нему потом матчим строки ОФР.
hierarcy as (
    SELECT 
        lvl1.StrokaOtcheta AS level0,
        lvl2.StrokaOtcheta AS level1,
        lvl3.StrokaOtcheta AS level2,
        lvl4.StrokaOtcheta AS level3,
        -- Берём самый глубокий существующий уровень как "лист" иерархии
        COALESCE(lvl4.StrokaOtcheta, lvl3.StrokaOtcheta, lvl2.StrokaOtcheta, lvl1.StrokaOtcheta) AS leaf
    FROM StrukturaOFR lvl1
    LEFT JOIN StrukturaOFR lvl2 ON lvl2.StrokaOtchetaRoditel = lvl1.StrokaOtcheta
    LEFT JOIN StrukturaOFR lvl3 ON lvl3.StrokaOtchetaRoditel = lvl2.StrokaOtcheta
    LEFT JOIN StrukturaOFR lvl4 ON lvl4.StrokaOtchetaRoditel = lvl3.StrokaOtcheta
    WHERE lvl1.StrokaOtchetaRoditel IS NULL  -- начинаем с корневых узлов
    ORDER BY level0, level1, level2, level3
),

-- Финансовая и инвестиционная деятельность:
-- объединяем два источника в единый формат с уровнями иерархии.
fin_inv_deyat as (
    -- Финансовая деятельность: доходы — только по банковским вкладам, остальное — расходы
    select
        date(NachaloPerioda) as NachaloPerioda,
        date(KonetsPerioda) as KonetsPerioda,
        Organizatsiya as Organizatsiya,
        'Финансовая деятельность' as level1,
        case 
            when StrokaOtcheta='% по банковским вкладам'
            then 'Доходы'
            else 'Расходы'
        end as level2,
        '' as level3,
        StrokaOtcheta as StrokaOtcheta,
        sum(`Сумма`) as Summa
    from 
        OFR_FinDeyatelnost
    group by 1,2,3,4,5,6,7
    union all
    -- Инвестиционная деятельность: расходы — только по лизингу, остальное — доходы
    select
        date(NachaloPerioda) as NachaloPerioda,
        date(KonetsPerioda) as KonetsPerioda,
        Organizatsiya as Organizatsiya,
        'Инвестиционная деятельность' as level1,
        case 
            when StrokaOtcheta='Расходы по лизингу'
            then 'Расходы'
            else 'Доходы'
        end as level2,
        '' as level3,
        StrokaOtcheta as StrokaOtcheta,
        sum(`Сумма`) as Summa
    from 
        OFR_InvDeyatelnost
    group by 1,2,3,4,5,6,7
),

-- Доходы, которые нужно исключить из "прочих доходов",
-- чтобы не задваивать их в итоговой сумме.
-- Сюда попадают: излишки, возмещения, неустойки + все доходы из фин/инвест деятельности.
doh_dlya_vichisl as (
    select 
        t.KonetsPerioda,
        t.NachaloPerioda,
        t.Organizatsiya,
        sum(t.Summa) as Summa
    from (
        -- Излишки / возмещения / неустойки из ОФР
        select
            date(o.NachaloPerioda) as NachaloPerioda,
            date(o.KonetsPerioda) as KonetsPerioda,
            o.Organizatsiya as Organizatsiya,
            sum(o.Summa) as Summa
        from 
            `OOFR2` o
        where
            o.StrokaOtcheta like '%излишков%' or
            o.StrokaOtcheta like '%возмещение%' or
            o.StrokaOtcheta like '%неустойки%'
        group by 1,2,3
        union all
        -- Доходы из фин/инвест деятельности
        select
            f.NachaloPerioda as NachaloPerioda,
            f.KonetsPerioda as KonetsPerioda,
            f.Organizatsiya as Organizatsiya,
            sum(f.Summa) as Summa
        from
            fin_inv_deyat f
        where f.level2='Доходы'
        group by 1,2,3
    ) t
    group by 1,2,3
),

-- Прочие доходы (уже выделенные в отдельный источник)
doh_proch as (
    select
        date(NachaloPerioda) as NachaloPerioda,
        date(KonetsPerioda) as KonetsPerioda,
        Organizatsiya as Organizatsiya,
        sum(Summa) as Summa
    from 
        DokhodyProchie
    group by 1,2,3
),

-- Все прочие доходы (общая сумма)
doh_prochvse as (
    select
        date(NachaloPerioda) as NachaloPerioda,
        date(KonetsPerioda) as KonetsPerioda,
        Organizatsiya as Organizatsiya,
        sum(Summa) as Summa
    from 
        DokhodyProchieVse
    group by 1,2,3
),

-- "Прочее" в доходах = все прочие доходы минус уже учтённые прочие
-- минус доходы, которые уходят в фин/инвест деятельность.
-- Так избегаем двойного счёта.
prochee as (
    select 
        doh_prochvse.NachaloPerioda as NachaloPerioda,
        doh_prochvse.KonetsPerioda as KonetsPerioda,
        doh_prochvse.Organizatsiya as Organizatsiya,
        (coalesce(doh_prochvse.Summa,0)-
         coalesce(doh_proch.Summa,0)-
         coalesce(doh_dlya_vichisl.Summa,0)) as Summa
    from 
        doh_prochvse 
    left join 
        doh_proch on
        doh_prochvse.NachaloPerioda=doh_proch.NachaloPerioda and
        doh_prochvse.KonetsPerioda=doh_proch.KonetsPerioda and
        doh_prochvse.Organizatsiya=doh_proch.Organizatsiya
    left join 
        doh_dlya_vichisl on
        doh_prochvse.NachaloPerioda=doh_dlya_vichisl.NachaloPerioda and
        doh_prochvse.KonetsPerioda=doh_dlya_vichisl.KonetsPerioda and
        doh_prochvse.Organizatsiya=doh_dlya_vichisl.Organizatsiya
), 

-- Основной блок: собираем все источники в единую структуру
-- (NachaloPerioda, KonetsPerioda, Organizatsiya, level1, level2, level3, StrokaOtcheta, Summa).
full_data as (
    -- Основной ОФР: строки сопоставляем с иерархией, чтобы получить level1/level2/level3
    select
        date(o.NachaloPerioda) as NachaloPerioda,
        date(o.KonetsPerioda) as KonetsPerioda,
        o.Organizatsiya as Organizatsiya,
        ifNull(
            case 
                -- Особый случай: доходы по банковским вкладам относим к фин. деятельности
                when o.StrokaOtcheta = '% по банковским вкладам'
                then 'Финансовая деятельность'
                else coalesce(h.level1, o.StrokaOtcheta)
            end,
            ''
        ) as level1,
        ifNull(
            case 
                when o.StrokaOtcheta = '% по банковским вкладам'
                then 'Доходы'
                else coalesce(h.level2, '')
            end,
            ''
        ) as level2,
        ifNull(coalesce(h.level3, ''), '') as level3,
        o.StrokaOtcheta as StrokaOtcheta,
        o.Summa as Summa
    from 
        `OOFR2` o 
    left join hierarcy h on o.StrokaOtcheta = h.leaf 
    union all
    -- Выручка от продаж
    select
        date(NachaloPerioda) as NachaloPerioda,
        date(KonetsPerioda) as KonetsPerioda,
        Organizatsiya as Organizatsiya,
        'Доходы' as level1,
        'от продажи продукции, товаров, работ и услуг' as level2,
        '' as level3,
        'от продажи продукции, товаров, работ и услуг' as StrokaOtcheta,
        sum(SummaVyruchkiBezNDS) as Summa
    from 
        DokhodyOtProdazh
    group by 1,2,3,4,5,6,7
    union all
    -- Себестоимость реализованной готовой продукции
    select
        date(NachaloPerioda) as NachaloPerioda,
        date(KonetsPerioda) as KonetsPerioda,
        Organizatsiya as Organizatsiya,
        'Расходы на реализованную продукцию' as level1,
        'С/с реализованной готовой продукции' as level2,
        '' as level3,
        'С/с реализованной готовой продукции' as StrokaOtcheta,
        sum(Summa) as Summa
    from 
        SsRealizovannoyProduktsii
    group by 1,2,3,4,5,6,7
    union all
    -- Прочие расходы (коммерческие)
    select
        date(NachaloPerioda) as NachaloPerioda,
        date(KonetsPerioda) as KonetsPerioda,
        Organizatsiya as Organizatsiya,
        'Коммерческие расходы (расходы на продажу)' as level1,
        'С/с по прочим продажам' as level2,
        '' as level3,
        'С/с по прочим продажам' as StrokaOtcheta,
        sum(Summa) as Summa
    from 
        RaskhodyProchie
    group by 1,2,3,4,5,6,7
    union all  
    -- Прочие доходы (продажа материалов, транспортные услуги)
    select
        NachaloPerioda as NachaloPerioda,
        KonetsPerioda as KonetsPerioda,
        Organizatsiya as Organizatsiya,
        'Доходы' as level1,
        'прочие (продажа материалов, транспортные услуги)' as level2,
        '' as level3,
        'прочие (продажа материалов, транспортные услуги)' as StrokaOtcheta,
        Summa as Summa
    from 
        doh_proch  
    union all  
    -- "Прочее" в доходах (расчётная величина, см. prochee)
    select
        NachaloPerioda as NachaloPerioda,
        KonetsPerioda as KonetsPerioda,
        Organizatsiya as Organizatsiya,
        'Доходы' as level1,
        'прочее' as level2,
        '' as level3,
        'прочее' as StrokaOtcheta,
        Summa as Summa
    from 
        prochee   
    union all   
    -- Финансовая и инвестиционная деятельность
    select
        f.NachaloPerioda as NachaloPerioda,
        f.KonetsPerioda as KonetsPerioda,
        f.Organizatsiya as Organizatsiya,
        f.level1 as level1,
        f.level2 as level2,
        f.level3 as level3,
        f.StrokaOtcheta as StrokaOtcheta,
        f.Summa as Summa
    from
        fin_inv_deyat f 
    union all  
    -- Налог на прибыль
    select
        date(`НачалоПериода`) as NachaloPerioda,
        date(`КонецПериода`) as KonetsPerioda,
        `Организация` as Organizatsiya,
        'НАЛОГ НА ПРИБЫЛЬ' as level1,
        '' as level2,
        '' as level3,
        `СтрокаОтчета` as StrokaOtcheta,
        sum(`Сумма`) as Summa
    from 
        Nalog
    group by 1,2,3,4,5,6,7
),

-- Группировка всех данных + расчёт итогов по прибыли:
--   * "Итого прибыль / Доходы" — сумма всех доходных строк,
--   * "Итого прибыль / Расходы" — сумма всех расходных строк с обратным знаком.
full_grouped_data as (
    -- Детализированные строки без изменений
    select 
        f.KonetsPerioda,
        f.NachaloPerioda,
        f.Organizatsiya,
        f.level1,
        f.level2,
        f.level3,
        f.StrokaOtcheta,
        sum(f.Summa) as Summa
    from 
        full_data f
    group by 
        f.KonetsPerioda,
        f.NachaloPerioda,
        f.Organizatsiya,
        f.level1,
        f.level2,
        f.level3,
        f.StrokaOtcheta
    union all
    -- Итог по доходам (level1='Доходы' или level2='Доходы')
    select
        f.KonetsPerioda,
        f.NachaloPerioda,
        f.Organizatsiya,
        'Итого прибыль' as level1,
        'Доходы' as level2,
        '' as level3,
        '' as StrokaOtcheta,
        sum(case
                when f.level1='Доходы' or f.level2='Доходы'
                then f.Summa
                else 0
            end) as Summa
    from 
        full_data f 
    group by 
        f.KonetsPerioda,
        f.NachaloPerioda,
        f.Organizatsiya
    union all
    -- Итог по расходам (всё, что не доходы), знак меняем на "-"
    select
        f.KonetsPerioda,
        f.NachaloPerioda,
        f.Organizatsiya,
        'Итого прибыль' as level1,
        'Расходы' as level2,
        '' as level3,
        '' as StrokaOtcheta,
        sum(case
                when f.level1!='Доходы' and f.level2!='Доходы'
                then f.Summa
                else 0
            end)*(-1) as Summa
    from 
        full_data f 
    group by 
        f.KonetsPerioda,
        f.NachaloPerioda,
        f.Organizatsiya
)

-- Финальный SELECT:
-- к каждой строке подтягиваем сумму за аналогичный период прошлого года
-- (по тем же организации и уровням иерархии/строке).
select 
    f.KonetsPerioda,
    f.NachaloPerioda,
    f.Organizatsiya,
    f.level1,
    f.level2,
    f.level3,
    f.StrokaOtcheta,
    f.Summa,
    coalesce(prev.Summa,0) as Prev_year_summa
from 
    full_grouped_data f
left join 
    full_grouped_data prev on
    f.Organizatsiya    = prev.Organizatsiya and
    f.level1           = prev.level1 and
    f.level2           = prev.level2 and
    f.level3           = prev.level3 and 
    f.StrokaOtcheta    = prev.StrokaOtcheta and
    toDate(prev.KonetsPerioda) = toDate(f.KonetsPerioda) - INTERVAL 1 year