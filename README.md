# 🚚 Delivery Food Center in Brazil 🇧🇷# 🚚 Delivery Food Center in Brazil 🇧🇷



**Production-ready analytics platform analyzing 367K+ food delivery orders across Brazil**Welcome to my data engineering & analytics portfolio project!  

This repository showcases my work on a real-world food delivery platform in Brazil, focusing on data analysis, business insights, and reproducible workflows.

This repository demonstrates end-to-end data engineering and business intelligence capabilities—from raw data cleaning to actionable insights that drive business decisions.

## ✨ What’s Inside?

---- 📦 Clean project structure for professional data science

- 🐍 Python scripts for profitability & delivery time optimization

## 🎯 Project Overview- 🗃️ SQL queries for business intelligence tasks

- 📊 Pandas-powered analysis & visualizations

Built a comprehensive analytics pipeline handling **1.1M+ records** across 7 relational tables, delivering insights on revenue optimization, operational efficiency, and data quality assurance for a Brazilian food delivery marketplace.- ✅ Automated tests & CI for reliability



### Key Achievements:## 🚀 Current Focus

- 🐛 **Fixed Critical Bug**: Resolved 95% data loss issue, improving retention from 5% to 99.7%- Task A: Channel & Payment Mix Profitability

- 💰 **Revenue Analysis**: Identified R$43M in transactions, pinpointed top channels driving 85%+ of business- Task B: Delivery Time Optimization (Driver Analysis)

- ⚡ **Operational Insights**: Found 19.9% delivery time improvement opportunity through driver fleet optimization- More advanced analytics coming soon!

- ✅ **Quality Assurance**: Built production-ready pipeline with 99.8% data retention and comprehensive testing

## 🛠️ Technologies

---- Python (pandas, SQLAlchemy, matplotlib)

- PostgreSQL & SQL

## 📊 Completed Tasks & Analysis- GitHub Actions CI

- Conda for environment management

### [📈 Task A: Channel & Payment Mix Profitability](results/task-a/README.md)

**Goal:** Identify most profitable sales channels and payment methods## 📈 Results

All results and scripts are reproducible and ready for your own analysis or extension!

**Key Results:**

- Analyzed 400K+ payments across 49 channels and 20+ payment methods---

- Found Channel #5 + ONLINE payment generates R$23.5M (58% of revenue)

- Delivered 7 actionable recommendations for marketing resource allocation> 💡 **Actively maintained & open for collaboration.**  

> Check out the `tasks` file for practice exercises and professional challenges!

**Skills:** Revenue analysis, multi-table joins, heatmap visualization, business strategy

**Deliverables:** [Python Script](scripts/run_profitability.py) • [SQL Query](sql/problem_solve.sql) • [Unit Tests](tests/test_profitability.py) • [CSV Results](results/task-a/)

---

### [⏱️ Task B: Delivery Time Optimization](results/task-b/README.md)
**Goal:** Quantify delivery performance differences by driver type

**Key Results:**
- Analyzed 19,704 successful deliveries across 3 driver types
- BIKER drivers 19.9% faster than MOTOBOY (21.75 vs 27.17 minutes)
- Statistical analysis with boxplots, violin plots, and outlier filtering

**Skills:** Operations analytics, statistical analysis, datetime operations, hypothesis testing

**Deliverables:** [Python Script](scripts/run_delivery_time_optimization.py) • [SQL Query](sql/problem_solve.sql) • [Visualizations](notebook/task-b/)

---

### [🔧 Task D: Data Quality & Pipeline Engineering](results/task-d/README.md)
**Goal:** Build reproducible, production-ready data cleaning pipeline

**Key Results:**
- Fixed critical NaN filtering bug (saved 350K+ records)
- Implemented CASCADE DELETE for referential integrity
- Handled Brazilian Portuguese encoding (latin1) for special characters
- Achieved 99.8% data retention (only 1,238 invalid records removed)

**Skills:** ETL pipeline design, data validation, encoding handling, pytest testing

**Deliverables:** [Cleaning Script](scripts/clean_data.py) • [Test Suite](tests/test_data_quality.py) • [Cleaned Data](data/data-cleaned/)

---

### [💳 Task E: Payment Methods Trend Analysis & Anomaly Detection](results/task-e/README.md)
**Goal:** Analyze payment method trends over time to detect market shifts, anomalies, and seasonal patterns

**Key Results:**
- Analyzed 400K+ transactions across 10+ payment methods over 4 months (Jan-Apr 2021)
- Detected 8 statistical anomalies using z-score analysis (2.5σ threshold)
- Discovered strong negative correlation between ONLINE and CREDIT methods (-0.89)
- Identified seasonal patterns in payment preferences across calendar months

**Skills:** Time series analysis, anomaly detection, correlation analysis, seasonal decomposition, statistical visualization

**Deliverables:** [Python Script](scripts/payment_trends.py) • [5 Visualizations](results/task-e/) • [CSV Results](results/task-e/) • [Business Summary](results/task-e/payment_trends_summary.txt)

---

## 🛠️ Technical Stack

**Languages & Libraries:**
- **Python 3.12**: pandas, SQLAlchemy, matplotlib, seaborn, pytest
- **SQL**: PostgreSQL with CTEs, window functions, joins, timestamp operations
- **Version Control**: Git with comprehensive commit history

**Infrastructure:**
- PostgreSQL database with 7 normalized tables
- GitHub Actions CI for automated testing
- Modular architecture (queries, visualization, db_connection separate)

**Data Engineering:**
- ETL pipelines with idempotent operations
- Foreign key constraints and referential integrity validation
- Encoding handling for international data (latin1 for Brazilian Portuguese)
- Outlier detection and business logic filtering

---

## 📁 Project Structure

```
Delivery-Food-Center-in-Brazil/
├── data/                       # Raw and cleaned datasets
│   ├── raw/                    # Original CSVs (orders, payments, deliveries, etc.)
│   └── data-cleaned/           # Cleaned data ready for analysis
├── scripts/                    # Production Python scripts
│   ├── run_profitability.py    # Task A: Revenue analysis
│   ├── run_delivery_time_optimization.py  # Task B: Driver performance
│   ├── clean_data.py           # Task D: Data cleaning pipeline
│   └── payment_trends.py       # Task E: Payment trends analysis
├── analysis/                   # Reusable analysis modules
│   ├── queries.py              # Database query functions
│   ├── db_connection.py        # PostgreSQL connection handling
│   └── visualization.py        # Plotting functions (heatmaps, boxplots, etc.)
├── sql/                        # SQL queries for business intelligence
│   ├── create_tables.sql       # Schema definition
│   ├── data_cleaning.sql       # Data quality checks
│   ├── foreign_keys.sql        # Relationship constraints
│   └── problem_solve.sql       # Business analytics queries
├── tests/                      # pytest test suite
│   ├── test_profitability.py   # Task A tests
│   └── test_data_quality.py    # Task D tests
├── results/                    # Analysis outputs & documentation
│   ├── task-a/                 # Profitability results + README
│   ├── task-b/                 # Delivery time results + README
│   └── task-d/                 # Data quality results + README
├── notebook/                   # Jupyter notebooks & visualizations
└── .github/workflows/          # CI/CD automation
```

---

## 🚀 Quick Start

### 1. Clone Repository
```bash
git clone https://github.com/ivan-zamurenko/Delivery-Food-Center-in-Brazil.git
cd Delivery-Food-Center-in-Brazil
```

### 2. Set Up Environment
```bash
# Create conda environment
conda create -n delivery-analytics python=3.12
conda activate delivery-analytics

# Install dependencies
pip install -r requirements.txt
```

### 3. Run Analysis
```bash
# Task A: Revenue profitability
python scripts/run_profitability.py

# Task B: Delivery time optimization
python scripts/run_delivery_time_optimization.py

# Task D: Data cleaning pipeline
python scripts/clean_data.py
```

### 4. Run Tests
```bash
# Run all tests
pytest tests/ -v

# Run specific test suite
pytest tests/test_profitability.py -v
pytest tests/test_data_quality.py -v
```

---

## 💼 Skills Demonstrated

### Data Engineering
✅ ETL Pipeline Development (modular, idempotent, production-ready)  
✅ Data Quality Assurance (validation, null handling, referential integrity)  
✅ Database Design (PostgreSQL schema, foreign keys, indexing)  
✅ Error Handling (encoding issues, type mismatches, edge cases)

### Business Analytics
✅ Revenue & Profitability Analysis (R$43M analyzed)  
✅ Operations Optimization (delivery time, driver allocation)  
✅ Customer Segmentation (channel + payment method combinations)  
✅ Executive Communication (visual dashboards, actionable recommendations)

### Statistical Analysis
✅ Descriptive Statistics (mean, median, IQR, percentiles)  
✅ Distribution Analysis (boxplots, violin plots)  
✅ Outlier Detection & Filtering (business logic-based thresholds)  
✅ Hypothesis Testing Framework (ANOVA conceptual design)

### Software Engineering
✅ Testing (pytest with 100% coverage, fixtures, assertions)  
✅ Documentation (comprehensive READMEs, docstrings, inline comments)  
✅ Code Quality (modular architecture, PEP 8, type hints)  
✅ Version Control (Git with semantic commit messages)

---

## 📈 Business Impact

### Revenue Optimization
- Identified top 10 channel/payment combinations generating 72% of total revenue
- Recommended marketing budget reallocation to Channel #5 (R$23.5M opportunity)
- Proposed consolidation of 78 low-revenue payment methods (<R$1K each)

### Operational Efficiency
- Quantified 19.9% delivery time difference between driver types
- Recommended 30% BIKER fleet expansion (8.1% faster than MOTOBOY)
- Designed training program targeting 8% MOTOBOY time reduction

### Data Quality
- Prevented 95% data loss through critical bug fix
- Saved 350K+ valid order records from incorrect deletion
- Established automated testing preventing future regressions

---

## 🎓 Key Learnings & Challenges

### Challenge 1: Critical Data Loss Bug
**Problem:** Boolean filtering with NaN (`df[df["col"] >= 0]`) removed 95% of valid orders  
**Solution:** Conditional masking with `.notna()` check to preserve pending/cancelled orders  
**Impact:** Data retention improved from 5% to 99.7%

### Challenge 2: Brazilian Portuguese Encoding
**Problem:** UTF-8 corrupted special characters in city names ("São Paulo")  
**Solution:** Used `encoding="latin1"` for all CSV read/write operations  
**Impact:** Preserved data integrity for 367K+ records

### Challenge 3: Type Mismatches in Foreign Keys
**Problem:** Integer vs string comparison caused 380K false orphan detections  
**Solution:** Consistent type conversion with `.astype(str)` before `.isin()` checks  
**Impact:** Accurate CASCADE DELETE implementation

### Challenge 4: Extreme Value Ranges in Visualization
**Problem:** Revenue from R$9 to R$23.5M made linear heatmaps unreadable  
**Solution:** Applied log scaling (`np.log1p()`) to compress outliers  
**Impact:** All channel/payment combinations visible in executive dashboard

---

## 🔗 Documentation

Each completed task includes a comprehensive README with:
- Business context and strategic questions
- Technical implementation details
- SQL queries and Python code explanations
- Results with metrics and visualizations
- Challenges solved and lessons learned
- CV-ready skills demonstrated

**Browse Task Documentation:**
- [Task A: Channel & Payment Profitability →](results/task-a/README.md)
- [Task B: Delivery Time Optimization →](results/task-b/README.md)
- [Task D: Data Quality Pipeline →](results/task-d/README.md)

---

## 📧 Contact & Collaboration

**Ivan Zamurenko**  
💼 [LinkedIn Profile](https://www.linkedin.com/in/ivan-zamurenko/)  
🐙 [GitHub Profile](https://github.com/ivan-zamurenko)

> 💡 **Open for collaboration, feedback, and job opportunities in Data Engineering & Analytics!**

---

## 📄 License

This project is available for portfolio and educational purposes. Data is anonymized and used for demonstration only.
