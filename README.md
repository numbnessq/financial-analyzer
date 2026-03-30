# 📊 Financial Document Analyzer

Десктоп-приложение для анализа финансовых документов организации. Система агрегирует данные из множества документов, выявляет аномалии в закупках, строит граф связей между подразделениями и контрагентами, оценивает уровень риска и объясняет отклонения с помощью AI.

---

## 🚀 Возможности

- Загрузка и парсинг PDF, DOCX, XLSX документов
- AI-извлечение структурированных позиций через Ollama (локально)
- Нормализация названий и единиц измерения
- Fuzzy-matching похожих позиций между документами
- Анализ отклонений цен и выявление аномалий
- Риск-скоринг каждой позиции (0–100)
- AI-объяснения аномалий на русском языке
- Построение графа связей: подразделения → контрагенты → позиции
- Десктоп-интерфейс на Electron с таблицей результатов и JSON-графом

---

## 🗂 Структура проекта

```
project/
│
├── backend/
│   ├── main.py                  # FastAPI сервер, все endpoints
│   ├── api/
│   ├── pipeline/
│   │   ├── parser.py            # Парсинг PDF, DOCX, XLSX
│   │   ├── ai_extractor.py      # Извлечение позиций через Ollama
│   │   ├── normalizer.py        # Нормализация названий
│   │   ├── source_mapper.py     # Привязка к источнику
│   │   ├── matcher.py           # Fuzzy-matching позиций
│   │   ├── analyzer.py          # Анализ отклонений
│   │   ├── scorer.py            # Риск-скоринг
│   │   ├── explainer.py         # AI-объяснения через Ollama
│   │   └── graph_builder.py     # Построение графа (networkx)
│   │
│   ├── models/
│   │   └── schemas.py           # Pydantic схемы
│   └── prompts/
│
├── frontend/
│   ├── main.js                  # Electron окно
│   ├── preload.js               # Мост Electron ↔ JS
│   ├── index.html               # UI
│   └── renderer.js              # Логика интерфейса
│
├── package.json
└── requirements.txt
```

---

## 🛠 Технологии

**Backend:**
- Python 3.11+
- FastAPI — HTTP API
- pandas — работа с таблицами
- rapidfuzz — fuzzy matching позиций
- networkx — построение графа связей
- pydantic — валидация данных
- pdfplumber — парсинг PDF
- python-docx — парсинг DOCX
- openpyxl — парсинг Excel

**AI:**
- Ollama (локально) — модель mistral
- Извлечение структуры из текста
- Объяснение аномалий на русском языке

**Frontend:**
- Electron — десктоп-приложение
- HTML/CSS/JS — интерфейс

---

## ⚙️ Установка и запуск

### 1. Клонировать репозиторий

```bash
git clone https://github.com/numbnessq/financial-analyzer.git
cd financial-analyzer
```

### 2. Создать виртуальное окружение и установить зависимости

```bash
python -m venv venv
source venv/bin/activate  # macOS/Linux
# или
venv\Scripts\activate     # Windows

pip install -r requirements.txt
```

### 3. Установить и запустить Ollama

Скачай Ollama: https://ollama.com

```bash
ollama pull mistral
ollama serve
```

### 4. Запустить backend

```bash
cd backend
uvicorn main:app --reload
```

Backend будет доступен на: `http://localhost:8000`

### 5. Установить зависимости frontend и запустить

```bash
cd ..
npm install
npm start
```

---

## 🔌 API Endpoints

| Метод | Endpoint | Описание |
|-------|----------|----------|
| GET | `/ping` | Проверка статуса сервера |
| POST | `/upload` | Загрузка файлов |
| POST | `/analyze` | Запуск полного pipeline |
| GET | `/results` | Таблица с рисками и объяснениями |
| GET | `/graph` | Граф в формате JSON (nodes + edges) |

### Пример запроса к /analyze

```json
[
  {
    "filename": "zakupki.pdf",
    "department": "Отдел строительства",
    "contractor": "ООО СтройСнаб",
    "items": [
      { "name": "бетон М300", "price": 4500, "quantity": 50, "unit": "м3" }
    ]
  }
]
```

---

## 📊 Риск-скоринг

| Правило | Баллы |
|---------|-------|
| Позиция не найдена в других документах | +25 |
| Отклонение цены > 20% от средней | +30 |
| Отклонение цены > 50% от средней | +20 |
| Высокий разброс цен (CV > 30%) | +20 |
| Малая выборка (менее 3 цен) | +10 |

**Уровни риска:**
- 🟢 `LOW` — 0–19
- 🟡 `MEDIUM` — 20–44
- 🔴 `HIGH` — 45–69
- 🟣 `CRITICAL` — 70–100

---

## 📦 Зависимости

**Python (requirements.txt):**
```
fastapi
uvicorn
pandas
rapidfuzz
networkx
pydantic
pdfplumber
python-docx
openpyxl
httpx
requests
```

**Node.js (package.json):**
```json
{
  "devDependencies": {
    "electron": "^28.0.0"
  }
}
```

---

## 📝 Лицензия

MIT
