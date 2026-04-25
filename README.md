# Financial Analyzer

Десктоп-приложение для автоматического анализа финансовых закупочных документов на предмет аномалий и отклонений.

> Система выявляет статистические отклонения и структурные несоответствия.  
> Все результаты носят **индикативный характер** и требуют верификации специалистом.

---

## Стек

| Слой | Технология |
|------|------------|
| Desktop | Tauri (Rust) |
| Backend | Python 3.11 + FastAPI |
| Frontend | Vanilla JS / HTML |
| Сборка | PyInstaller + GitHub Actions |

---

## Возможности

- Загрузка документов: **XLSX, DOCX, PDF** (до 15 файлов)
- Автоматическая нормализация позиций и единиц измерения
- Fuzzy-матчинг и кластеризация одинаковых позиций между документами
- **IQR-based ценовой анализ** (Q1/Q3/IQR, soft/hard fence, z-score)
- Аддитивный скоринг (0–100) с объяснением каждого балла
- Анализ концентрации поставщиков (индекс Херфиндаля–Хиршмана)
- Детектор паттернов: дробление закупок, повторяющиеся суммы, подозрительные интервалы
- Граф закупок: позиции ↔ поставщики ↔ документы
- Генерация **DOCX-отчёта** с ФАКТЫ / ОТКЛОНЕНИЕ / ИНТЕРПРЕТАЦИЯ по каждой позиции
- User feedback: разметка false positive через API

---

## Структура проекта

```
project/
├── backend/
│   ├── main.py                  # FastAPI приложение
│   ├── requirements.txt
│   └── pipeline/
│       ├── parser.py            # Парсинг xlsx/docx/pdf
│       ├── ai_extractor.py      # Извлечение позиций через AI (fallback)
│       ├── normalizer.py        # Нормализация полей
│       ├── source_mapper.py     # Привязка к источнику
│       ├── matcher.py           # Fuzzy-группировка позиций
│       ├── clusterer.py         # Кластеризация (Union-Find + rapidfuzz)
│       ├── analyzer.py          # Оркестратор pipeline
│       ├── scorer.py            # Флаги + аддитивный скоринг
│       ├── explainer.py         # ФАКТЫ / ОТКЛОНЕНИЕ / ИНТЕРПРЕТАЦИЯ
│       ├── price_analyzer.py    # IQR-статистика
│       ├── supplier_analyzer.py # HHI, доли, сигналы
│       ├── pattern_detector.py  # Дробление, повторы, интервалы
│       ├── graph_builder.py     # networkx граф
│       └── report_generator.py  # DOCX-отчёт
│
├── frontend/
│   ├── index.html
│   ├── app.js
│   ├── api.js
│   ├── render.js
│   ├── graph.js
│   └── updater.js
│
├── tauri/
│   ├── package.json
│   └── src-tauri/
│       ├── src/main.rs
│       ├── Cargo.toml
│       └── tauri.conf.json
│
├── scripts/
│   ├── start_backend.py         # Dev: запуск frontend + backend
│   └── build_backend.py         # Сборка PyInstaller бинаря
│
└── uploads/                     # Загруженные файлы (dev)
```

---

## Установка и запуск (dev)

### Требования

- Python 3.11+
- Node.js 20+
- Rust (stable)

### Первый запуск

```bash
# 1. Клонировать
git clone https://github.com/numbnessq/financial-analyzer.git
cd financial-analyzer

# 2. Python окружение
python3 -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r backend/requirements.txt

# 3. Node зависимости
cd tauri
npm install

# 4. Запуск
npm run tauri dev
```

---

## Сборка релиза

```bash
# Создать тег — GitHub Actions запустит сборку автоматически
git tag v0.X.X
git push origin v0.X.X
```

Actions соберёт `.dmg` (macOS), `.exe` (Windows), `.AppImage` (Linux) и опубликует в GitHub Releases.

### Secrets для GitHub Actions

| Secret | Описание |
|--------|----------|
| `TAURI_PRIVATE_KEY` | Приватный ключ для подписи обновлений |
| `TAURI_KEY_PASSWORD` | Пароль к ключу |

---

## REST API

Backend доступен на `http://127.0.0.1:8000`.

| Метод | Путь | Описание |
|-------|------|----------|
| GET | `/ping` | Healthcheck |
| POST | `/upload` | Загрузка файлов |
| POST | `/analyze` | Запуск анализа (async, возвращает job_id) |
| GET | `/job/{job_id}` | Статус задачи |
| GET | `/results` | Результаты анализа |
| GET | `/graph` | Граф (`?min_score=40&types=item,contractor`) |
| GET | `/suppliers` | Анализ поставщиков (HHI) |
| GET | `/patterns` | Аномальные паттерны |
| GET | `/analysis` | Полный объект анализа |
| PATCH | `/results/{name}/verdict` | User feedback |
| GET | `/report` | Скачать DOCX-отчёт |
| GET | `/report/save` | Сохранить отчёт в ~/Downloads |

---

## Скоринг

Аддитивная модель: `score = Σ(веса активных флагов)`, максимум 100.

| Уровень | Диапазон | Значение |
|---------|----------|----------|
| CRITICAL | 70–100 | Несколько значимых флагов одновременно |
| HIGH | 40–69 | Один весомый или несколько слабых флагов |
| MEDIUM | 20–39 | Один-два слабых сигнала |
| LOW | 0–19 | Единичный сигнал или отсутствие |

Скор отражает **количество и значимость сигналов**, не вероятность нарушения.

---

## Переменные окружения

| Переменная | Описание |
|------------|----------|
| `ANTHROPIC_API_KEY` | Опционально. Если задан — AI генерирует нарратив для отчёта. Без ключа используется детерминированный fallback. |

---

## Лицензия

MIT
