# Financial Analyzer

Десктоп-приложение для анализа финансовых документов.
Выявляет аномалии в закупках, строит граф связей, оценивает риски.

Стек: **Python / FastAPI** (backend) + **Tauri / Rust** (оболочка) + **HTML/JS** (UI)

---

## Структура проекта

```
project/
├── backend/
│   ├── main.py
│   └── pipeline/
│       ├── parser.py
│       ├── ai_extractor.py
│       ├── normalizer.py
│       ├── source_mapper.py
│       ├── matcher.py
│       ├── analyzer.py
│       ├── scorer.py
│       ├── explainer.py
│       └── graph_builder.py
│
├── frontend/
│   ├── index.html
│   ├── styles.css
│   ├── app.js        ← логика UI
│   ├── api.js        ← запросы к backend
│   └── graph.js      ← Cytoscape визуализация
│
├── tauri/
│   └── src-tauri/
│       ├── src/main.rs
│       ├── Cargo.toml
│       └── tauri.conf.json
│
├── scripts/
│   └── start_backend.py
│
├── requirements.txt
└── README.md
```

---

## Требования

**Python:** 3.11+, Ollama с моделью `mistral` — https://ollama.com

**Rust + Tauri:** Rust — https://rustup.rs, затем:
```bash
cargo install tauri-cli
```

---

## Установка

```bash
python -m venv venv
source venv/bin/activate   # macOS/Linux
pip install -r requirements.txt

ollama pull mistral
ollama serve
```

---

## Запуск

**Только backend + браузер:**
```bash
uvicorn backend.main:app --reload
# открыть frontend/index.html в браузере
```

**Полное Tauri приложение:**
```bash
cd tauri
cargo tauri dev
```

---

## Сборка

```bash
cd tauri
cargo tauri build
```

- macOS → `.dmg`
- Windows → `.msi`
- Linux → `.AppImage`

---

## API

| Метод | URL | Описание |
|---|---|---|
| GET | `/ping` | Проверка |
| POST | `/upload` | Загрузка файлов |
| POST | `/analyze` | Запуск анализа |
| GET | `/results` | Результаты |
| GET | `/graph` | Граф JSON |

---

## Скоринг: `risk = 1 − Π(1 − pᵢ)`

| Флаг | p | Что означает |
|---|---|---|
| `vague_item` | 0.70 | Размытая формулировка |
| `duplicate_3_plus` | 0.65 | Позиция в 3+ отделах |
| `zero_quantity` | 0.65 | Кол-во = 0, цена есть |
| `total_mismatch` | 0.60 | Сумма ≠ цена × кол-во |
| `price_deviation_50` | 0.60 | Цена >50% от средней |
| `quantity_deviation_50` | 0.55 | Объём расходится >50% |
| `unit_mismatch` | 0.50 | Разные единицы одной позиции |
| `split_suspected` | 0.45 | Дробление закупки |

Уровни: **LOW** 0–19 · **MEDIUM** 20–39 · **HIGH** 40–69 · **CRITICAL** 70+

---

## VS Code + Git

Просто открой папку проекта, установи расширения `rust-analyzer` и `Python`. Git работает как обычно:

```bash
git add .
git commit -m "описание"
git push
```

---

## Лицензия

MIT
