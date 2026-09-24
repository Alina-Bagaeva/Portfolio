-- =====================================================================
-- Общий смысл запроса:
-- Для каждого клиента, купившего хотя бы одну розничную лицензию 
-- (Retail_Cash, RETAIL, Retail_Account), находим дату первой продажи 
-- ЛЮБОЙ другой номенклатуры в разрезе папок второго уровня.
-- Затем для каждой комбинации "клиент + папка" вычисляем, была ли эта 
-- первая не-розничная продажа раньше/позже первой розничной, и разницу 
-- в месяцах (по началу месяца).
-- Иерархия папок строится рекурсивно от корневой папки 01 Лицензии СБиС (активируются на reg).
-- =====================================================================

WITH RECURSIVE folder_tree AS (
    -- Базовый запрос: стартуем с заданной корневой папки (уровень 1)
    SELECT 
        nomenclature_id,
        name AS folder_name,
        CAST(NULL AS INT) AS parent_id,
        1 AS level,
        JSON_ARRAY(name) AS full_path   -- массив имён от корня до текущей папки
    FROM nomenclatures
    WHERE 
        nomenclature_id = 19652
        AND deleted = 0
        AND isFolder = 1                -- убедимся, что это действительно папка

    UNION ALL

    -- Рекурсивная часть: спускаемся по подпапкам
    SELECT 
        child.nomenclature_id,
        child.name,
        parent.nomenclature_id,
        parent.level + 1,
        JSON_ARRAY_APPEND(parent.full_path, '$', child.name)  -- добавляем имя текущей папки в путь
    FROM nomenclatures child
    JOIN folder_tree parent 
        ON child.folder = parent.nomenclature_id
    WHERE 
        child.deleted = 0 
        AND child.isFolder = 1
        AND parent.level < 4            -- ограничиваем глубину (можно убрать или увеличить)
),

-- =====================================================================
-- noms: получаем все номенклатуры (не папки), которые лежат
--       внутри найденного дерева папок, и определяем их папку второго уровня
-- =====================================================================
noms as (
    SELECT
        n.nomenclature_code,
        n.name, 
        -- Извлекаем имя папки второго уровня (индекс 1 в массиве full_path)
        CAST(JSON_VALUE(ft.full_path, '$[1]') AS VARCHAR(50)) AS folder_level2,
        -- Для наглядности можно также вывести имя корневой папки
        CAST(JSON_VALUE(ft.full_path, '$[0]') AS VARCHAR(50)) AS root_folder_name
    FROM 
        nomenclatures n
    LEFT JOIN folder_tree ft 
        ON n.folder = ft.nomenclature_id   -- присоединяем информацию о папке, в которой лежит номенклатура
    WHERE 
        n.isFolder IS NULL                 -- только номенклатуры (не папки)
        AND n.deleted = 0
        AND n.folder IN (SELECT nomenclature_id FROM folder_tree)  -- только те, что лежат в нашем дереве папок
),

-- =====================================================================
-- clients_retail: для каждого клиента находим дату первой продажи
--                розничной лицензии и определяем её тип (название)
-- =====================================================================
clients_retail as (
    SELECT
        d.taviat_client_id,
        min(DATE(d.sbis_shipment_date)) as first_ritail_sale,   -- самая ранняя дата отгрузки розничной лицензии
        CASE 
            when dtp.nomenclature_code='Retail_Cash' then 'Права использования Saby, Розница Базовый'
            when dtp.nomenclature_code='RETAIL' then 'Права использования Saby, Розница Оптимальный'
            else 'Права использования Saby, Розница Профи'
        END as first_ritail_license   -- тип первой розничной лицензии (определяем по коду)
    FROM 
        documents d 
    JOIN 
        documents_tabular_part dtp  ON d.document_id = dtp.document_id 
    WHERE 
        dtp.nomenclature_code in ('Retail_Cash','RETAIL','Retail_Account')
        AND d.shipment = 1                -- только отгрузки
        AND d.deleted = 0
        AND (d.completed_status != 0 or d.completed_status is null)
    GROUP BY 
        d.taviat_client_id
),

-- =====================================================================
-- first_sales: для каждого клиента и каждой папки второго уровня
--              находим дату самой первой продажи любой НЕрозничной
--              номенклатуры, попавшей в эту папку.
-- =====================================================================
first_sales as (
    SELECT 
        d.taviat_client_id,
        cr.first_ritail_sale,
        cr.first_ritail_license,
        n.folder_level2 as folder,
        min(DATE(d.sbis_shipment_date)) as first_sale_date   -- самая ранняя дата продажи в данной папке
    FROM 
        documents d 
    JOIN 
        documents_tabular_part dtp ON d.document_id = dtp.document_id 
    JOIN 
        noms n ON n.nomenclature_code = dtp.nomenclature_code   -- связываем с информацией о папке
    JOIN 
        clients_retail cr ON d.taviat_client_id = cr.taviat_client_id   -- подтягиваем первую розничную покупку
    WHERE  
        dtp.nomenclature_code not in ('Retail_Cash','RETAIL','Retail_Account')   -- исключаем сами розничные позиции
        AND d.shipment = 1
        AND d.deleted = 0
        AND (d.completed_status != 0 or d.completed_status is null)
    GROUP BY 
        d.taviat_client_id,
        cr.first_ritail_sale,
        cr.first_ritail_license,
        n.folder_level2   -- группируем по клиенту и папке второго уровня
)

-- =====================================================================
-- Финальная выборка: для каждой комбинации клиент-папка выводим 
-- дату первой не-розничной продажи, сравниваем её с датой первой 
-- розничной и вычисляем разницу в месяцах.
-- =====================================================================
SELECT 
    fs.taviat_client_id,
    fs.first_ritail_sale,
    fs.first_ritail_license,
    fs.folder,
    fs.first_sale_date,
    -- Категория: продажа произошла до, после или одновременно с первой розничной
    CASE 
        when fs.first_ritail_sale > fs.first_sale_date then 'before'
        when fs.first_ritail_sale < fs.first_sale_date then 'after'
        else 'same_date'
    END as before_after,
    -- Разница в месяцах с учётом только начала месяца (первое число).
    -- Если даты попадают в один календарный месяц, разница = 0.
    CASE 
        WHEN DATE_FORMAT(fs.first_ritail_sale, '%Y-%m-01') < DATE_FORMAT(fs.first_sale_date, '%Y-%m-01')
            THEN TIMESTAMPDIFF(MONTH, fs.first_ritail_sale, fs.first_sale_date)
        WHEN DATE_FORMAT(fs.first_ritail_sale, '%Y-%m-01') > DATE_FORMAT(fs.first_sale_date, '%Y-%m-01') 
            THEN -TIMESTAMPDIFF(MONTH, fs.first_sale_date, fs.first_ritail_sale)
        ELSE 0
    END AS month_diff,
    -- Уникальный идентификатор строки для возможной дальнейшей обработки
    ROW_NUMBER() OVER (ORDER BY fs.taviat_client_id, fs.first_ritail_sale, fs.first_ritail_license, fs.folder, fs.first_sale_date) AS row_id
FROM 
    first_sales fs;