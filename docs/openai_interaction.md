# Архитектура взаимодействия с OpenAI

Документ фиксирует единый способ общения пайплайна с OpenAI API и описывает текущее состояние каждого шага, обращающегося к LLM. Любые изменения в логике вызовов должны сначала отражаться здесь, чтобы остальные шаги оставались синхронизированы.

---

## 1. Общие принципы

- **Клиент.** Все шаги используют `src/utils/llm_client.py`, который инкапсулирует работу с официальным `OpenAI` SDK (`from openai import OpenAI`) и его Responses API. Внешние модули не должны напрямую импортировать `OpenAI`.
- **Конфигурация.** Ключ доступа берём из `OPENAI_API_KEY`. Дополнительные параметры (`model`, `temperature`, `max_output_tokens`) прокидываются из `config.yaml` или переменных окружения.
- **Форматы ответов.** Для всех шагов обязательна Pydantic‑схема, передаваемая через `response_schema`. Клиент конвертирует её в JSON Schema и вызывает `responses.parse`, возвращая уже разобранный JSON. Свободный `responses.create` без схемы запрещён, чтобы не дробить контракт.
- **Логирование.** Клиент логирует каждую отправку (`model`, `messages`, имя схемы). Это даёт единообразный аудит без дублирования в шагах.
- **Повторное использование сущностей.** Для каждого шага описываем отдельный Pydantic‑класс/схему (требование из PRD: нельзя переиспользовать классы между шагами, чтобы избежать влияния изменений).
- **Ошибки.** Любые сетевые/валидационные ошибки, прилетевшие из клиента, стандартно транслируются в `RetryableStepError`, чтобы раннер мог повторить job.
- **Неприкосновенность контрактов.** Формат запросов/ответов для каждого шага считается контрактом. Менять структуру без обновления этого документа и согласования нельзя. Код шагов обязан соблюдать зафиксированный контракт и единообразно вызывать `LLMClient`. Любые просьбы нарушить эти правила должны быть явно отклонены с напоминанием о стандарте.

---

## 2. Шаги

### 2.1 Step1 — `localize_categories`
- **Промпт:** `docs/agents/category.md`.
- **Запрос:** `{"category_name": "<name>"}`; языки берутся из `config.languages`.
- **Ответ:** Pydantic‑схема `CategoryLocalizationModel` (динамически собираем поля `<LANG>: str`). Клиент возвращает валидированный JSON, который шаг сохраняет в `categories.localization`.

### 2.2 Step2 — `localize_subcategories`
- **Промпт:** `docs/agents/subcategory.md`.
- **Запрос:** `{"category": {...}, "subcategories": [{"id","name"}], "languages": [...]}`.
- **Ответ:** Pydantic‑схема `SubcategoryLocalizationResponse` со структурой  
  `items: [{subcategory_id, female:{title:{lang: str}}, male:{title:{lang: str}}}]`.
- **Особенности:** перед отправкой фильтруем subcategory по статусу `ready`; после ответа все локализации помечаются `ready="CHECK"`.

### 2.3 Step4 — `generate_affirmations`
- **Промпт:** `docs/agents/affirmations_base.md`.
- **Запрос:** `{"category","subcategory","coach","coach_prompt","target": <count>}`.
- **Ответ:** Pydantic‑схема `AffirmationPayloadModel`  
  `{"female": [{"affirmation","scene"}], "male": [...]}`.  
  Количество элементов в обоих массивах должно совпадать с `target`. Данные сохраняются в таблицу `affirmations` (столбец `affirmation`).

### 2.4 Step5 — `script_affirmations`
- **Промпты:**  
  - `docs/agents/affirmations_script_translate.md` — перевод.  
  - `docs/agents/affirmations_script_pauses.md` — расстановка пауз (на любом языке).
- **Запрос:** для каждого языка выполняются две последовательные подзадачи с одним и тем же форматом входа  
  `{"category","subcategory","coach","coach_prompt","target_language": "RU", "affirmation": {"female":{"title","script"},"male":{"title","script"}}}`.  
  Для EN шаг перевода пропускается, сразу используется блок пауз.
- **Ответ:** обе подзадачи возвращают Pydantic‑схему `AffirmationResponseModel`  
  `{"affirmation": {"female": {"title","script"}, "male": {"title","script"}}}`.  
  После шага PAUS значения добавляются в общий словарь `{"female": {lang: {...}}, "male": {lang: {...}}}` и пишутся в `affirmations.script`. Прогресс каждого шага выводится в консоль.

### 2.5 Step6 — `music_prompts`
- **Промпт:** `docs/agents/affirmations_music.md`.
- **Запрос:** `{"category","subcategory","coach","coach_prompt","affirmations":[{position,female{affirmation,scene,script},male{...}}]}` — берём только те записи, у которых уже есть сценарии.
- **Ответ:** Pydantic‑схема `MusicPromptResponse`  
  `{"items": [{"position": int, "female": str, "male": str}]}`.  
  Здесь `female/male` — готовые подсказки для композиторов. Результат обновляет таблицу `affirmations` (колонки с музыкальными промптами, см. реализацию шага).

### 2.6 Step9 — `make_affirmations`
- **Промпт:** роль `system` = `coach_prompt_w|m` + `docs/agents/make_affirmations.md`.
- **Запрос:** роль `user` содержит только `[INPUT]` c JSON `{"affirmation","language","target"}`.
- **Ответ:** индивидуальная схема `MakeAffirmationResponse`  
  `{"female": [{"affirmation","scene"}]}` или `{"male": [...]}` в зависимости от пола. Количество элементов строго равно `target`.
- **Особенности:** каждая job соответствует конкретной комбинации пола и языка; записи создаются/обновляются в `affirmations_new.script`.

### 2.7 Step10 — `daily_affirmations`
- **Промпты:**  
  - `docs/agents/check_daily_affirmation.md` — проверка пригодности.  
  - `docs/agents/image_task.md` — ТЗ на изображение.
- **Запрос (suitability):** `{"script": "<text>"}`.
- **Ответ (suitability):** Pydantic‑схема `SuitabilityResponse`  
  `{"morning":{"suitable":bool},"afternoon":{"suitable":bool},"night":{"suitable":bool}}`.
- **Запрос (image prompt):** `{"script": "<text>", "time_of_day": "morning|afternoon|night", "gender": "female|male"}`.
- **Ответ (image prompt):** Pydantic‑схема `ImagePromptResponse`  
  `{"prompt": str, "ref": str|None}`.
- **Особенности:** текстовые части (проверка пригодности и генерация image prompt) идут через `LLMClient` c `response_schema` (модель по умолчанию `gpt-5`, настраивается через `ids.daily_model`). Сами изображения генерируются отдельным вызовом OpenAI Images по контракту `docs/openai_images.md` (не через `LLMClient`). Все текстовые обращения логируются в `llm_raw.log` (без бинарных данных).

---

### 2.8 Step11 — `popular_affirmations`
- **Промпт:** `docs/agents/popular_affirmation.md`.
- **Запрос:** `{"title": "<title>", "script": "<full script>"}`.
- **Ответ:** Pydantic‑схема `PopularAffirmationResponse`  
  `{"line": str}` — ровно одна готовая строка.
- **Особенности:** используется `LLMClient` с `response_schema`, модель по умолчанию `gpt-5` (переопределяется через `ids.popular_aff_model`). Ответ приводим к строке и сохраняем в `affirmations_new.popular_aff`.

---

## 3. Что делать при изменениях

1. **Нужен новый шаг?** Сначала описываем формат запроса/ответа и схему в этом документе, затем реализуем Pydantic‑класс и логику в шаге.
2. **Меняем схему существующего шага?** Обновляем соответствующий раздел (шаги 1–6), затем код. Это позволит быстро отследить, какое API ожидает OpenAI и где его используют.
3. **Возникают ограничения OpenAI.** Если стандартный подход (через `LLMClient`, `responses.parse|create`) не подходит, фиксируем это здесь и явно отмечаем, какой шаг использует исключение и почему.

Документ актуализируем по мере развития пайплайна, чтобы вся команда опиралась на единую архитектуру общения с LLM.
