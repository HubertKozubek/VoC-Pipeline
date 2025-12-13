# Voice of Customer (VoC) Pipeline

A production-ready AI Engineering project that aggregates Steam reviews, analyzes sentiment/topics, and presents insights via a dashboard.

## Features
- **Data Gathering**: Incremental fetching of Steam reviews.
- **Processing**: Data cleaning and feature engineering.
- **ML Pipeline**: 
    - Sentiment Analysis (Pre-trained Transformers).
    - Topic Modeling (BERTopic/Clustering).
    - Experiment Tracking with Weights & Biases.
- **Orchestration**: Workflow management with Prefect.
- **Deployment**: FastAPI backend and Streamlit dashboard.

## Setup

### Prerequisites
- Python 3.12+
- `uv`
- Docker & Docker Compose

### Installation
1. Clone the repository:
   ```bash
   git clone <repo-url>
   cd voc
   ```

2. Install dependencies:
   ```bash
   uv sync
   ```

3. Run the application:
   ```bash
   uv run fastapi dev src/voc/main.py
   ```

## Architecture
- `src/voc`: Main application source code.
- `src/voc/ingestion`: Data fetching modules.
- `notebooks`: Exploratory Data Analysis.
- `tests`: Unit and integration tests.
