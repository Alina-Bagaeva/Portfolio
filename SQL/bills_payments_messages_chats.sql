-- =============================================================================
-- Сводный отчет по сотрудникам: финансы (исходящие счета и платежи) и активность в чатах
-- =============================================================================

-- 1. ОБЪЕДИНЕНИЕ ПЛАТЕЖЕЙ (прямых и через реализации)
WITH payments AS (		
	SELECT 									-- ЧАСТЬ 1А: Платежи, привязанные напрямую к исходящим документам (счетам)
		DATE(dp.`date`) AS date_col,		-- Дата платежа
		CASE WHEN dp.`type` = 'ИсходящийПлатеж' THEN -dp.sum ELSE dp.sum END AS sum, -- Сумма платежа
		dp.taviat_payment_id,				-- Идентификатор документа (счета)
		d.employee_id						-- Ответственный сотрудник
	FROM 
		document_payments dp
	LEFT JOIN 
		documents d ON d.taviat_document_id = dp.taviat_document_id
	WHERE d.document_type = 'outbill'		-- Только счета
		AND d.deleted = 0					-- Не удаленные
		AND (dp.`type` = 'ВходящийПлатеж' OR dp.`type` = 'ДокументЭквайринга' OR dp.`type` = 'ИсходящийПлатеж')  -- Входящие платежи/эквайринг, исходящие – возвраты
		AND dp.completed_status > 0			-- только проведённые
	UNION ALL 
	SELECT 									-- ЧАСТЬ 1Б: Платежи через реализации, связанные со счетами
		DATE(dp.`date`) AS date_col,
		CASE WHEN dp.`type` = 'ИсходящийПлатеж' THEN -dp.sum ELSE dp.sum END AS sum,
		dp.taviat_payment_id,
		d2.employee_id						-- Берем сотрудника из связанного счета
	FROM 
		document_payments dp
	LEFT JOIN 
		documents d ON d.taviat_document_id = dp.taviat_document_id
	LEFT JOIN 
		relation_outbill_realization ror ON d.taviat_document_id = ror.realization_id 
	LEFT JOIN
		documents d2 ON ror.outbill_id = d2.taviat_document_id
	WHERE d.document_type = 'realization'	-- Только реализации
		AND d.deleted = 0
		AND d2.deleted = 0
		AND d2.taviat_document_id IS NOT NULL	-- Связь со счетом существует
		AND (dp.`type` = 'ВходящийПлатеж' OR dp.`type` = 'ДокументЭквайринга' OR dp.`type` = 'ИсходящийПлатеж')
		AND dp.completed_status > 0
), 
-- 2. АГРЕГАЦИЯ ПЛАТЕЖЕЙ ПО ДНЯМ И СОТРУДНИКАМ
grouped_payments AS (
	SELECT 
		p.date_col,
		p.employee_id,
		SUM(p.sum) AS revenue,									-- Общая выручка за день
		COUNT(DISTINCT p.taviat_payment_id) AS count_payments	-- Количество оплаченных документов
	FROM payments p
	WHERE p.sum > 0
	GROUP BY p.date_col, p.employee_id
),
grouped_returns AS (
	SELECT 
		p.date_col,
		p.employee_id,
		SUM(p.sum) * (-1) AS returns,							-- Общая сумма возвратов
		COUNT(DISTINCT p.taviat_payment_id) AS count_returns	-- Количество возвратов
	FROM payments p
	WHERE p.sum < 0
	GROUP BY p.date_col, p.employee_id
),
-- 3. АГРЕГАЦИЯ СЧЕТОВ ПО ДНЯМ И СОТРУДНИКАМ
outbills AS (
	SELECT 
		DATE(d.sbis_date) AS date_col,
		d.employee_id,
		COUNT(DISTINCT d.taviat_document_id) AS count_outbills,	-- Количество выставленных счетов
		SUM(d.invoice_total_sum) AS outbill_summ					-- Общая сумма счетов
	FROM documents d
	WHERE d.document_type = 'outbill'
	GROUP BY DATE(d.sbis_date), d.employee_id
),
-- 4. ЕДИНЫЙ ИСТОЧНИК ДАННЫХ ДЛЯ ЧАТОВ И СООБЩЕНИЙ (одно чтение таблицы)
messages_chats_raw AS (
	SELECT 
		DATE(sccm.message_date) AS date_col,                              -- день сообщения
		STR_TO_DATE(CONCAT(YEAR(sccm.message_date), '-', MONTH(sccm.message_date), '-01'), '%Y-%m-%d') AS month_col,  -- первое число месяца (DATE)
		COALESCE(sccm.author_name, 'Не известен') AS employee_name,
		sccm.id AS message_id,
		sccm.sbis_channel_chats_list_id AS chat_id
	FROM sbis_channel_chat_messages sccm
	WHERE sccm.message_type != 'client'                                   -- только сотрудники
		AND (sccm.service_type IS NULL OR sccm.service_type = 'audio_message') -- обычные или аудиосообщения
),
-- 5. АГРЕГАЦИЯ СООБЩЕНИЙ ПО ДНЯМ
messages AS (
	SELECT 
		date_col,
		employee_name,
		COUNT(DISTINCT message_id) AS count_messages
	FROM messages_chats_raw
	GROUP BY date_col, employee_name
),
-- 6. АГРЕГАЦИЯ ЧАТОВ ПО МЕСЯЦАМ
chats AS (
	SELECT 
		month_col AS date_col,                   -- первое число месяца
		employee_name,
		COUNT(DISTINCT chat_id) AS count_chats,
		0 AS count_messages                      -- заглушка, чтобы структура совпадала с messages
	FROM messages_chats_raw
	GROUP BY month_col, employee_name
),
-- 7. ОБЪЕДИНЕНИЕ ДАННЫХ ПО СЧЕТАМ И ПЛАТЕЖАМ (для последующей агрегации)
outbills_payments AS ( 
	SELECT					-- Исходящие счета 
		o.date_col,
		o.employee_id,
		o.count_outbills,
		o.outbill_summ,
		0 AS count_payments,		
		0 AS revenue,				
		0 AS count_returns,			
		0 AS returns				
	FROM outbills o
	UNION ALL 
	SELECT					-- Платежи 
		gp.date_col,
		gp.employee_id,
		0 AS count_outbills,		
		0 AS outbill_summ,			
		gp.count_payments,
		gp.revenue,
		0 AS count_returns,			
		0 AS returns				
	FROM grouped_payments gp
	UNION ALL 
	SELECT					-- Возвраты 
		gr.date_col,
		gr.employee_id,
		0 AS count_outbills,		
		0 AS outbill_summ,			
		0 AS count_payments,		
		0 AS revenue,				
		gr.count_returns,			
		gr.returns				
	FROM grouped_returns gr
), 
-- 8. ИТОГОВАЯ АГРЕГАЦИЯ ФИНАНСОВЫХ ПОКАЗАТЕЛЕЙ ПО ДНЯМ И СОТРУДНИКАМ
grouped_operations AS (
	SELECT
		op.date_col,
		op.employee_id,
		SUM(op.count_outbills) AS count_outbills,
		SUM(op.outbill_summ)  AS outbill_summ,
		SUM(op.count_payments) AS count_payments,
		SUM(op.revenue)       AS revenue,
		SUM(op.count_returns) AS count_returns,
		SUM(op.returns)       AS returns
	FROM outbills_payments op
	GROUP BY op.date_col, op.employee_id
)
-- 9. ОСНОВНОЙ ЗАПРОС: объединение финансовой и чатовой статистики
SELECT			-- ЧАСТЬ А: Финансовая статистика (счета и платежи)
	go.date_col,
	CASE 
		WHEN e.employee_id IS NULL THEN 'Не известен'
		ELSE CONCAT(
			COALESCE(e.last_name, ''),
			' ', 
			COALESCE(e.first_name, ''),
			' ', 
			COALESCE(e.patronymic, '')
		)
	END AS employee_name,
	0 AS count_chats,					-- Заглушки для чатов
	0 AS count_messages,				-- Заглушки для сообщений
	go.count_outbills,
	go.outbill_summ,
	go.count_payments,
	go.revenue,
	go.count_returns,
	go.returns
FROM grouped_operations go
LEFT JOIN employees e ON e.employee_id = go.employee_id 	
UNION ALL 
SELECT			-- ЧАСТЬ Б: Статистика по сообщениям (по дням)
	m.date_col,	
	m.employee_name,
	0 AS count_chats,
	m.count_messages,
	0 AS count_outbills,	
	0 AS outbill_summ,
	0 AS count_payments,
	0 AS revenue,
	0 AS count_returns,
	0 AS returns
FROM messages m
JOIN (
	SELECT DISTINCT
		CONCAT(
			COALESCE(e.last_name, ''),
			' ', 
			COALESCE(e.first_name, ''),
			' ', 
			COALESCE(e.patronymic, '')
		) AS employee_name
	FROM employees e
) t ON m.employee_name = t.employee_name 
UNION ALL 
SELECT			-- ЧАСТЬ В: Статистика по чатам (по месяцам, дата = первое число месяца)
	c.date_col,	
	c.employee_name,
	c.count_chats,
	0 AS count_messages,				-- здесь count_messages уже 0 из chats
	0 AS count_outbills,	
	0 AS outbill_summ,
	0 AS count_payments,
	0 AS revenue,
	0 AS count_returns,
	0 AS returns
FROM chats c
JOIN (
	SELECT DISTINCT
		CONCAT(
			COALESCE(e.last_name, ''),
			' ', 
			COALESCE(e.first_name, ''),
			' ', 
			COALESCE(e.patronymic, '')
		) AS employee_name
	FROM employees e
) t ON c.employee_name = t.employee_name;