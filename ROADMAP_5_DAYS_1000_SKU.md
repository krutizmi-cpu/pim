# PIM 1000 SKU/Day Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** За 5 дней перестроить PIM из режима "ручная карточка" в массовый conveyor, где 1 менеджер обрабатывает до 1000 артикулов в день и работает в основном по очередям проблем, а не по каждой карточке вручную.

**Architecture:** Базовый поток становится parser-first + AI-verified + client-readiness. Источник URL товара не требуется: система стартует от домена поставщика, сама ищет карточки, валидирует кандидатов, затем массово рерайтит контент, собирает 3–5 фото и формирует клиентские выгрузки только для готовых товаров.

**Tech Stack:** Streamlit, SQLite, текущие AI-провайдеры проекта (`NVIDIA`, `OpenRouter`, `OpenAI-compatible аналоги`), supplier parser, fallback web search, Ozon canonical core, client overlays.

---

## Неподвижные продуктовые решения

- **Не подключаем ChatGPT/Codex OAuth как backend AI-контур для PIM.**
- **Не делаем ставку на 100% script-only parsing.** Парсер ищет данные и кандидатов, но AI остаётся обязательным слоем проверки, рерайта и дозаполнения.
- **Не требуем URL карточки товара.** На входе достаточно:
  - домена поставщика;
  - артикула / supplier_article;
  - названия;
  - бренда;
  - категории, если есть.
- **Карточка товара перестаёт быть основным рабочим экраном.** Основной экран — `Каталог` и очереди готовности.
- **Главная метрика успеха:** `1000 хороших карточек в день` силами `1 менеджера`, цель месяца — `30 000 SKU`.

## Целевой conveyor

### 1. Domain-first parser

Для каждого товара:
- берём домен поставщика;
- пробуем внутренний поиск по сайту поставщика;
- строим список кандидатов карточек;
- проверяем article/code match, brand match, overlap названия;
- собираем фото, описание, характеристики, штрихкод, веса/габариты;
- если поставщик не дал хороший результат, идём во внешний интернет-поиск.

### 2. AI verification + rewrite

После parser-слоя:
- AI проверяет, похож ли найденный контент на нужный SKU;
- AI переписывает название и описание;
- AI выделяет рискованные поля и low-confidence значения;
- AI помогает собрать client-specific поля из master-state, а не перепарсивать всё заново.

### 3. Readiness queues

Каждый товар попадает в одну из очередей:
- `Готово к клиенту`
- `Нужна быстрая правка`
- `Нет фото`
- `Нет штрихкода`
- `Нет категории`
- `Нет client-required поля`
- `Нужен сертификат`
- `Низкая уверенность parser/AI`

### 4. Client-first export

Менеджер не открывает все карточки подряд. Он:
- фильтрует только blocked queues;
- чинит исключения;
- экспортирует только `ready`;
- получает клиентский Excel/API payload без ручной дозаписи по одному полю.

## KPI на 5-й день

- `>= 85%` товаров проходят parser/domain search без открытия карточки вручную.
- `>= 70%` товаров доходят до client-ready без ручной правки текста.
- `>= 80%` товаров имеют `3–5 фото` в gallery memory.
- `>= 90%` карточек имеют переписанное название и описание, а не сырой supplier text.
- `1000 SKU/day` достигается за счёт batch conveyor и работы по очередям исключений.

## День 1. Перестроить parser под root-domain only

**Файлы:**
- Modify: [app.py](C:\Cursor\pim\app.py)
- Modify: [services/supplier_parser.py](C:\Cursor\pim\services\supplier_parser.py)
- Modify: [PROJECT_CONTEXT.md](C:\Cursor\pim\PROJECT_CONTEXT.md)
- Modify: [CHANGELOG.md](C:\Cursor\pim\CHANGELOG.md)

**Результат дня:** parser больше не ждёт URL карточки товара и умеет стартовать от главной страницы поставщика как от домена.

- [ ] Вынести supplier-flow в явную модель `domain -> internal search -> candidate ranking -> fallback web`.
- [ ] Добавить жесткое разделение статусов parser:
  - `product_found`
  - `listing_only`
  - `brand_page_only`
  - `domain_search_failed`
  - `web_fallback_found`
  - `not_relevant`
- [ ] Дополнить parse comment структурой, пригодной для менеджера:
  - где искали;
  - какой candidate выбран;
  - почему rejected.
- [ ] Добавить в `Каталог` колонки/метки:
  - `Parser stage`
  - `Parser confidence`
  - `Photo count`
  - `Barcode status`
- [ ] Сделать массовый режим `Найти товарные карточки по домену поставщика` как отдельный conveyor-шаг.
- [ ] Не считать parser `success`, если найдена только общая категория/бренд-страница.

## День 2. AI как verifier и batch rewriter

**Файлы:**
- Modify: [services/ai_content_service.py](C:\Cursor\pim\services\ai_content_service.py)
- Modify: [app.py](C:\Cursor\pim\app.py)
- Modify: [services/template_matching.py](C:\Cursor\pim\services\template_matching.py)
- Modify: [PROJECT_CONTEXT.md](C:\Cursor\pim\PROJECT_CONTEXT.md)
- Modify: [CHANGELOG.md](C:\Cursor\pim\CHANGELOG.md)

**Результат дня:** AI больше не живёт как набор разрозненных кнопок и становится обязательной batch-стадией после parser.

- [x] Ввести 2 AI-режима:
  - `Fast batch`
  - `Deep repair`
- [x] Для `Fast batch` включить:
  - переписывание названия;
  - переписывание описания;
  - AI-check соответствия parser result нужному SKU;
  - подсказки по обязательным Ozon/client attributes.
- [x] Для `Deep repair` оставить тяжёлые сценарии:
  - сложный рерайт;
  - спорные атрибуты;
  - rich-content/image prompts.
- [x] Добавить batch-статус:
  - `AI verified`
  - `AI rejected parser result`
  - `AI rewrite ready`
- [x] Прекратить смешивать SEO-технический хвост с основным описанием карточки:
  - товарное описание отдельно;
  - SEO fields отдельно.

## День 3. Gallery memory, 3–5 фото и image readiness

**Файлы:**
- Modify: [services/supplier_parser.py](C:\Cursor\pim\services\supplier_parser.py)
- Modify: [app.py](C:\Cursor\pim\app.py)
- Modify: [services/template_matching.py](C:\Cursor\pim\services\template_matching.py)
- Modify: [README.md](C:\Cursor\pim\README.md)
- Modify: [CHANGELOG.md](C:\Cursor\pim\CHANGELOG.md)

**Результат дня:** система собирает не одно фото, а нормальную gallery memory и умеет оценивать визуальную готовность карточки.

- [x] Нормализовать image pipeline в 3 уровня:
  - `main image`
  - `gallery images`
  - `generated images`
- [x] Ввести целевую норму `3–5 фото на карточку`.
- [x] В `Каталог` и `Карточка` показать:
  - сколько фото найдено;
  - есть ли главное фото;
  - есть ли gallery `>= 3`.
- [ ] Отфильтровывать:
  - logo;
  - banner;
  - payment icons;
  - social icons;
  - brand pages.
- [ ] Зафиксировать очередь:
  - `нет главного фото`
  - `фото меньше 3`
  - `только слабые изображения`
- [x] Подготовить память под следующий блок image generation:
  - raw supplier image;
  - cleaned/approved source;
  - future generated variants.

## День 4. Certificate + client-readiness conveyor

**Файлы:**
- Modify: [app.py](C:\Cursor\pim\app.py)
- Modify: [services/certificate_registry.py](C:\Cursor\pim\services\certificate_registry.py)
- Modify: [services/readiness_service.py](C:\Cursor\pim\services/readiness_service.py)
- Modify: [PROJECT_CONTEXT.md](C:\Cursor\pim\PROJECT_CONTEXT.md)
- Modify: [CHANGELOG.md](C:\Cursor\pim\CHANGELOG.md)

**Результат дня:** readiness становится operational, а не декоративным.

- [ ] Разделить readiness на:
  - `master ready`
  - `Ozon ready`
  - `client ready`
  - `certificate ready`
  - `image ready`
- [ ] В `Каталог` добавить очереди:
  - `нужен сертификат`
  - `не хватает client field`
  - `не хватает фото`
  - `barcode issue`
- [ ] Сделать bulk action `Показать только blocked по выбранному клиенту`.
- [ ] Для клиента/шаблона считать readiness до экспорта, а не после ошибки.
- [ ] Встроить сертификаты в conveyor как обязательный gate для категорий, где они нужны.

## День 5. One-manager operating mode и выпуск production-conveyor

**Файлы:**
- Modify: [app.py](C:\Cursor\pim\app.py)
- Modify: [README.md](C:\Cursor\pim\README.md)
- Modify: [PROJECT_CONTEXT.md](C:\Cursor\pim\PROJECT_CONTEXT.md)
- Modify: [CHANGELOG.md](C:\Cursor\pim\CHANGELOG.md)

**Результат дня:** менеджер работает по очередям и массовым операциям, а не по одиночным карточкам.

- [ ] Собрать в `Каталог` главный порядок работы:
  1. Импорт
  2. Domain parser
  3. AI verify + rewrite
  4. Photo/certificate readiness
  5. Client readiness
  6. Export
- [ ] Убрать остатки “технического конструктора” из основного потока и спрятать их в service layer.
- [ ] Добавить KPI-блок для менеджера:
  - `готово сегодня`
  - `blocked today`
  - `без фото`
  - `без barcode`
  - `без сертификата`
  - `готово к Sportmaster / Detmir / Ozon`
- [ ] Подготовить smoke-пакет на реальной партии SKU:
  - не 1 товар, а мини-батч;
  - оценить долю auto-ready;
  - зафиксировать топ причин ручной правки.
- [ ] Зафиксировать production-режим:
  - карточка = repair mode;
  - каталог = command center;
  - клиентский шаблон = export QA.

## Что НЕ делаем в эти 5 дней

- Не расползаемся в новый большой UI-редизайн ради красоты.
- Не уходим в полноценный publish-loop по Ozon/Detmir вместо базового conveyor.
- Не подключаем новый OpenAI fallback только потому, что “так привычнее”.
- Не строим image studio раньше, чем стабилизируем parser, AI rewrite и readiness queues.

## Acceptance criteria на конец 5-го дня

- Менеджер может работать из `Каталога` почти весь день без постоянного открытия `Карточки`.
- Parser стартует от домена поставщика, а не от URL карточки.
- AI массово переписывает название и описание и помогает ловить нерелевантный parser result.
- Сервис умеет собирать `3–5 фото` и явно показывает image gaps.
- Readiness в каталоге превращён в реальный operational filter.
- Появляется рабочий шанс на `1000 SKU/day` для 1 менеджера.

## Первый приоритет на ближайший следующий рабочий блок

1. Добить parser под root-domain only.
2. В `Каталог` вывести очереди parser/image/barcode/client gaps.
3. Перевести AI в batch verifier + rewriter.
4. После этого идти в image generation / infographic layer.
