# Шаг 99: Экспорт данных

## 🎯 Цель
Собрать всю готовую библиотеку аффирмаций в SQLite‑базы `content_<lang>.db`, пригодные для прямого использования в мобильном приложении.

- **Структура SQLite‑файла**

| Таблица | Поле | Тип | Описание |
| --- | --- | --- | --- |
| `categories` | `id` | INTEGER PRIMARY KEY | Копируем `categories.id` |
| | `position` | INTEGER | Значение `categories.position` |
| | `name` | TEXT NOT NULL | `categories.localization[LANG]` |
| `subcategories` | `id` | INTEGER PRIMARY KEY | `subcategories.id` |
| | `position` | INTEGER | `subcategories.position` |
| | `name` | TEXT NOT NULL | `subcategories.localization[gender].title[LANG]` (предпочитаем female, fallback male) |
| | `shadow_w` | TEXT NOT NULL | `subcategories.shadow_w` |
| | `shadow_m` | TEXT NOT NULL | `subcategories.shadow_m` |
| | `views` | INTEGER NOT NULL | `subcategories.views` |
| | `is_daily_suitable` | INTEGER NOT NULL | 1 если `subcategories.is_daily_suitable` = `true` или `NULL`, иначе 0 |
| | `category_id` | INTEGER NOT NULL | FK на `categories.id` |
| `coaches` | `id` | INTEGER PRIMARY KEY | `coaches.id` |
| | `position` | INTEGER | `coaches.position` |
| | `name` | TEXT NOT NULL | `coaches.coach_name` или `coaches.coach` |
| | `description` | TEXT | `coaches.coach_UI_description[LANG]` |
| `affirmations` | `sub_id` | INTEGER NOT NULL | FK на `subcategories.id` |
| | `coach_id` | INTEGER NOT NULL | FK на `coaches.id` |
| | `position` | INTEGER | `affirmations_new.position` |
| | `gender` | INTEGER NOT NULL | 0 = female, 1 = male |
| | `title` | TEXT NOT NULL | `affirmations_new.script[gender][LANG].title` |
| | `subtitle` | TEXT NOT NULL | `affirmations_new.popular_aff[gender][LANG]` |
| | `script` | TEXT NOT NULL | `affirmations_new.script[gender][LANG].script` |
| | `morning_aff` | TEXT | `affirmations_new.aff_for_banners[gender][LANG].morning` |
| | `afternoon_aff` | TEXT | `affirmations_new.aff_for_banners[gender][LANG].afternoon` |
| | `evening_aff` | TEXT | `affirmations_new.aff_for_banners[gender][LANG]["late evening"]` |
| | `is_morning` | INTEGER NOT NULL | 1/0 по наличию webp `<...>_morning.webp` |
| | `is_afternoon` | INTEGER NOT NULL | Аналогично для `_afternoon.webp` |
| | `is_night` | INTEGER NOT NULL | Аналогично для `_night.webp` |

---

## 🧩 Исходные данные
- Таблицы: `categories`, `subcategories`, `coaches`, `affirmations_new`.
- Локализации:
  - `categories.localization[LANG]` — название категории.
  - `subcategories.localization[gender].title[LANG]` — название подкатегории (если женский вариант отсутствует — берём мужской).
  - `coaches.coach_UI_description[LANG]` — текст описания.
  - `affirmations_new.script[gender][LANG]` — `title` и `script`.
  - `affirmations_new.popular_aff[gender][LANG]` — короткая “вирусная” строка (идёт в `subtitle`).
  - `affirmations_new.aff_for_banners[gender][LANG]` — мантры по времени дня: `morning`, `afternoon`, `late evening` (кладём в `morning_aff/afternoon_aff/evening_aff` соответственно; если нет — `NULL`).
- Флаг пригодности: `subcategories.is_daily_suitable` — обязательно присутствует; `true` или `NULL` → `1`, иначе `0` в экспортируемой таблице `subcategories`.
- Превью (шаг 10): `./export/daily_previews/<cat>_<sub>_<coach_id>_<pos>_<m|w>_<lang>_<time>.webp` — по наличию определяем `is_morning/is_afternoon/is_night`.
- Каталог экспорта: `./export/`.

---

## ⚙️ Конфигурация
```yaml
steps:
  export_data: true

range:
  categories: [..]
  subcategories: [..]
  positions: [..]
versions: [...]          # фильтр наставников
languages: [EN, RU, ...]
```
- Используются стандартные `retry`, `threads`.

---

## 🚀 Логика выполнения
1. Загружаем категории → подкатегории → наставников → аффирмации с учётом диапазонов `range.*` и фильтра `versions`. Все JSON‑поля (локализации, `popular_aff`) парсим сразу и складываем в дерево.
2. Для каждого языка создаём job:
   1. Перезаписываем `./export/content_<lang>.db`.
   2. Создаём таблицы:
      - `categories(id, position, name)`
      - `subcategories(id, position, name, shadow_w, shadow_m, views, category_id)`
      - `coaches(id, position, name, description)`
      - `affirmations(sub_id, coach_id, position, gender, title, subtitle, script, is_morning, is_afternoon, is_night)`
   3. Проходим по дереву:
      - В `categories` пишем строки только с доступной локализацией на языке.
      - `subcategories.name` берём из `localization[gender].title[LANG]` (женский, иначе мужской).
      - `subcategories.shadow_w/shadow_m` — как есть из таблицы `subcategories`; `views` — из `subcategories.views`.
      - `coaches` — `coach_name` (fallback `coach`), `coach_UI_description[LANG]`.
      - Каждая запись `affirmations_new` порождает до двух строк (`gender=female/male`):
        - `title/script` — из `affirmations_new.script`.
        - `subtitle` — из `popular_aff[gender][LANG]`; отсутствие => логируем ошибку и пропускаем запись.
        - `morning_aff/afternoon_aff/evening_aff` — из `aff_for_banners[gender][LANG]` (`late evening` кладём в `evening_aff`). Если какого-то слота нет, оставляем `NULL`.
        - `gender` — 0 для female, 1 для male.
        - `is_*` — ставим `1`, если в `./export/daily_previews` есть файл `<cat>_<sub>_<coach_id>_<pos>_<m|w>_<lang>_<time>.webp`, иначе `0`.
   4. После обработки каждой категории логируем прогресс: `[BUSINESS] Export progress | lang=EN cat=3/10 aff=120`.
3. По завершении закрываем БД, фиксируем итоговое количество вставленных аффирмаций.

---

## 📦 Выходные данные
- `./export/content_<lang>.db` — SQLite‑база со всеми таблицами и данными на выбранном языке.

---

## 🧾 Возможные ошибки
| Ошибка | Причина | Действие |
| --- | --- | --- |
| Missing localization | нет перевода для языка | пропускаем узел, логируем предупреждение |
| Нет изображений | файл webp отсутствует | `is_morning/is_afternoon/is_night`=0 |
| DB/файловая ошибка | Supabase/файловая система | бросаем `RetryableStepError`, job повторится |

---

## ✅ Критерии завершения
- Для всех выбранных языков созданы файлы `content_<lang>.db`.
- Таблицы БД содержат актуальные локализации и аффирмации (со `subtitle` из шага 11).
- В логе видно, сколько категорий и аффирмаций обработано для каждого языка.
