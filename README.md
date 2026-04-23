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
│   ├── requirements.txt
│   ├── models/
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
│   ├── updater.js
│   ├── render.js
│   ├── app.js
│   ├── api.js
│   └── graph.js
│
├── tauri/
│   ├── package.json
│   └── src-tauri/
│       ├── binaries/          ← собранный backend-<triple>
│       ├── icons/
│       ├── src/
│       │   └── main.rs
│       ├── Cargo.toml
│       └── tauri.conf.json
│
├── scripts/
│   ├── start_backend.py       ← dev-режим
│   └── build_backend.py       ← сборка бинарника через PyInstaller
│
├── uploads/                   ← загруженные пользователем файлы
└── README.md
```

---

## Требования

- **Python 3.11+**
- **Rust** — https://rustup.rs
- **Node.js 20+**
- **Ollama** с моделью `mistral` — https://ollama.com

---

## Установка

```bash
python -m venv venv
source venv/bin/activate        # macOS/Linux
# venv\Scripts\activate         # Windows

pip install -r backend/requirements.txt

ollama pull mistral
ollama serve
```

Установка зависимостей Tauri:

```bash
cd tauri
npm ci
```

---

## Запуск (dev)

```bash
cd tauri
npm run tauri dev
```

Только backend + браузер:

```bash
uvicorn backend.main:app --reload
# открыть frontend/index.html в браузере
```

---

## Сборка

```bash
cd tauri
npm run tauri build
```

- macOS → `.dmg`
- Windows → `.msi`
- Linux → `.AppImage`

Бинарник backend собирается автоматически через `scripts/build_backend.py` (PyInstaller).

---

## Релизы (GitHub Actions)

Новый релиз запускается тегом:

```bash
git tag v0.1.1 && git push origin v0.1.1
```

CI собирает приложение под macOS, Windows и Linux и публикует assets в GitHub Releases.

---

## macOS: ошибка «приложение повреждено»

macOS блокирует неподписанные приложения. После установки выполни:

```bash
xattr -cr /Applications/financial-analyzer.app
```

---

## Git

```bash
git add -A && git commit -m "описание" && git push
```

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

## Лицензия

MIT
