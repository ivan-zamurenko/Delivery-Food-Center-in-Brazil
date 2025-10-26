# Task D — Data Quality & Pipeline (Data Engineering)

**Completed:** October 26, 2025  
**Goal:** Build a reproducible, production-ready data cleaning pipeline with comprehensive validation and testing.

---

## 📋 Overview

This task demonstrates professional **data engineering practices** by creating an automated pipeline that:
- Cleans raw Brazilian food delivery data (367K+ orders, 400K+ payments, 378K+ deliveries)
- Validates data quality and referential integrity
- Handles encoding issues (Brazilian Portuguese characters)
- Implements business logic for edge cases (NULL drivers, orphaned records)
- Provides full auditability through logging and reports

---

## 🎯 Business Context

### Data Quality Challenges Identified:
1. **Encoding Issues:** Brazilian Portuguese special characters (São Paulo, etc.) require latin1 encoding
2. **Missing Driver Data:** 4.19% of deliveries (15,886 records) have NULL driver_id
   - Analysis showed 8,601 are successfully DELIVERED
   - Business decision: Valid pattern (customer pickups, third-party services)
3. **Invalid Delivery Times:** Negative or extremely long delivery times (>180 minutes)
4. **Data Type Mismatches:** Integer time components vs. timestamp strings mixed in same dataset
5. **Orphaned Child Records:** Payments/deliveries referencing deleted orders after cleaning

### Key Business Decisions Made:
- ✅ Keep deliveries without drivers (replace NULL with -1, add `has_driver_data` flag)
- ✅ Remove only truly invalid delivery times (keep NaN for pending/cancelled orders)
- ✅ Implement CASCADE DELETE for referential integrity (remove orphaned child records)
- ✅ Don't convert integer time components to datetime (hour, minute, day, month, year)

---

## 🛠️ Technical Implementation

### Pipeline Architecture

```
data/raw/                           data/data-cleaned/
├── orders.csv (369K rows)         ├── orders_cleaned.csv (367,923 rows)
├── payments.csv (400K rows)   →   ├── payments_cleaned.csv (400,377 rows)
├── deliveries.csv (378K rows)     ├── deliveries_cleaned.csv (377,937 rows)
├── stores.csv                     ├── stores_cleaned.csv
├── channels.csv                   ├── channels_cleaned.csv
├── drivers.csv                    ├── drivers_cleaned.csv
└── hubs.csv                       └── hubs_cleaned.csv

                                    results/task-D/
                                    └── cleaning_report.txt
```

### Data Model

```
┌─────────────┐
│  channels   │──┐
└─────────────┘  │
                 │    ┌──────────┐     ┌────────────┐
┌─────────────┐  ├───→│  orders  │←───→│  payments  │
│   stores    │──┘    └──────────┘     └────────────┘
└─────────────┘            │
                           │
┌─────────────┐       ┌────▼──────┐
│   drivers   │←──────│ deliveries│
└─────────────┘       └───────────┘
```

**Relationships:**
- `orders.store_id` → `stores.store_id` (N:1)
- `orders.channel_id` → `channels.channel_id` (N:1)
- `payments.payment_order_id` → `orders.order_id` (N:1)
- `deliveries.delivery_order_id` → `orders.order_id` (N:1)
- `deliveries.driver_id` → `drivers.driver_id` (N:1, with -1 for unknown)

---

## 📝 Cleaning Steps Applied

### 1. Load Data with Proper Encoding
```python
encoding = "latin1"  # Handles Brazilian Portuguese characters
orders = pd.read_csv("data/raw/orders.csv", encoding=encoding)
```
**Why:** Default UTF-8 encoding corrupts special characters like "São Paulo"

### 2. Clean Orders Dataset

**Step-by-step:**

a) **Remove duplicates** by `order_id` (keep first)

b) **Parse datetime columns** (8 timestamp columns only - NOT integer components)
   ```python
   # Only convert timestamp strings
   datetime_cols = [
       "order_moment_created",    # "1/1/2021 12:01:36 AM"
       "order_moment_accepted",
       # ... (8 total, NOT hour/minute/day/month/year)
   ]
   df[col] = pd.to_datetime(df[col], format="mixed", errors="coerce")
   ```

c) **Calculate and validate delivery times**
   ```python
   # Calculate delivery time
   df["delivery_time_minutes"] = (
       df["order_moment_delivered"] - df["order_moment_delivering"]
   ).dt.total_seconds() / 60
   
   # Remove ONLY invalid times (keep NaN for pending orders)
   has_delivery_time = df["delivery_time_minutes"].notna()
   invalid_times = has_delivery_time & (
       (df["delivery_time_minutes"] < 0) | (df["delivery_time_minutes"] > 180)
   )
   df = df[~invalid_times]
   ```
   **Critical:** This pattern prevents removing 95% of dataset (orders not yet delivered)

d) **Validate order amounts** (must be positive)

e) **Drop critical NULLs** (order_id, store_id, channel_id)

**Results:** 368,999 → 367,923 rows (1,076 removed, 0.3% loss)

### 3. Clean Deliveries - Special NULL Driver Handling

```python
# Analyze pattern before deciding
null_count = df["driver_id"].isnull().sum()  # 15,886 (4.19%)

# Check if legitimate
delivered = df[df["driver_id"].isnull() & 
               df["delivery_status"] == "DELIVERED"]
# Result: 8,601 successful deliveries without driver!

# Business Decision: Keep them
df["driver_id"] = df["driver_id"].fillna(-1).astype(int)
df["has_driver_data"] = df["driver_id"] != -1
```

**Why -1?** Standard surrogate key for "Unknown" in dimensional modeling

### 4. Validate Foreign Key Relationships (CASCADE DELETE)

```python
# Check parent relationships
orphaned_stores = ~orders["store_id"].isin(stores["store_id"])

# Remove orphaned child records
orphaned_payments = ~payments["payment_order_id"].astype(str).isin(
    orders["order_id"].astype(str)
)
payments = payments[~orphaned_payments]  # 332 removed
```

**Why `.astype(str)`?** Prevents type mismatch (without this: 380K false orphans!)

---

## 📊 Final Results

### Data Retention Summary

| Table | Initial | Final | Removed | Retention |
|-------|---------|-------|---------|-----------|
| orders | 368,999 | 367,923 | 1,076 | 99.7% |
| payments | 400,709 | 400,377 | 332 | 99.9% |
| deliveries | 378,843 | 377,937 | 906 | 99.8% |

**Total Retention: 99.8%** (excellent data quality)

### Key Quality Metrics

✅ 0 duplicate IDs across all tables  
✅ 0 NULL values in critical fields  
✅ 100% valid foreign key relationships  
✅ All amounts positive  
✅ All delivery times valid (0-180 min for completed orders)

---

## 🧪 Testing & Validation

### Test Coverage

**Location:** `tests/test_data_quality.py`

- ✓ Uniqueness tests (6 tables)
- ✓ NULL validation tests
- ✓ Foreign key integrity tests (4 relationships)
- ✓ Business logic tests (delivery times, amounts)
- ✓ Encoding compatibility tests

**Run tests:**
```bash
pytest tests/test_data_quality.py -v
```

**All tests pass ✅**

---

## 🔧 Technical Challenges & Solutions

### Challenge 1: 95% Data Loss Bug

**Problem:** `df = df[df["delivery_time_minutes"] >= 0]` removed ALL orders with NaN

**Root Cause:** `NaN >= 0` evaluates to `False`

**Solution:** Only validate non-null values
```python
has_data = df["delivery_time_minutes"].notna()
invalid = has_data & ((df["delivery_time_minutes"] < 0) | (df["delivery_time_minutes"] > 180))
df = df[~invalid]
```

### Challenge 2: Type Conversion for Integer Columns

**Problem:** Converting `order_created_hour = 0` to datetime → `1970-01-01` (Unix epoch)

**Solution:** Don't convert integer time components, only timestamp strings

### Challenge 3: Type Mismatch in Foreign Keys

**Problem:** int vs str comparison gave 380K false orphans

**Solution:** Convert both to same type with `.astype(str)`

### Challenge 4: Encoding Issues

**Problem:** UTF-8 corrupts "São Paulo"

**Solution:** Use `encoding="latin1"` for read and write

---

## 🚀 Usage

### Running the Pipeline

```bash
python scripts/clean_data.py
```

**Output:**
```
2025-10-26 20:44:30 - INFO - ✓ Orders cleaned: 367923 rows
2025-10-26 20:44:30 - INFO - ✓ Payments cleaned: 400377 rows
2025-10-26 20:44:30 - INFO - ✓ Deliveries cleaned: 377937 rows
...
======================================================================
CLEANING SUMMARY
======================================================================
orders          | Initial: 368999 | Final: 367923 | Removed: 1076
payments        | Initial: 400709 | Final: 400377 | Removed:  332
deliveries      | Initial: 378843 | Final: 377937 | Removed:  906
======================================================================
```

---

## 💼 CV-Ready Skills Demonstrated

### Data Engineering
✅ ETL Pipeline Design (modular, class-based)  
✅ Data Validation (nulls, duplicates, referential integrity)  
✅ Error Handling (encoding, type mismatches, edge cases)  
✅ Idempotency (reproducible results)  
✅ Auditability (logging, reporting)

### Software Engineering
✅ Testing (pytest, 100% coverage)  
✅ Documentation (inline comments, docstrings, README)  
✅ Code Quality (PEP 8, type hints, meaningful names)

### Business Acumen
✅ Data-Driven Decisions (analyzed NULL driver pattern)  
✅ Domain Knowledge (delivery business models)  
✅ Stakeholder Communication (clear rationale documentation)

---

## 📚 Key Code Patterns

**Dimensional Modeling:**
```python
df["driver_id"] = df["driver_id"].fillna(-1).astype(int)
df["has_driver_data"] = df["driver_id"] != -1
```

**CASCADE DELETE:**
```python
orphaned = ~payments["payment_order_id"].isin(orders["order_id"])
payments = payments[~orphaned]
```

**Robust NaN Handling:**
```python
has_data = df["column"].notna()
invalid = has_data & (df["column"] < 0)
df = df[~invalid]  # Keeps NaN rows
```

---

## 📁 Deliverables

```
scripts/clean_data.py              # 340 lines, production-ready
tests/test_data_quality.py         # 226 lines, comprehensive tests
data/data-cleaned/                 # 7 cleaned CSV files
results/task-D/cleaning_report.txt # Audit trail
results/task-D/README.md           # This file
```

---

## 🎓 Lessons Learned

1. **Always investigate before deleting** - 8,601 NULL drivers were valid data
2. **Type consistency matters** - `.astype(str)` prevented 380K false detections
3. **NaN handling is tricky** - `NaN >= 0` is `False`, not `True`
4. **Encoding is critical** - Brazilian data needs latin1
5. **Test everything** - Unit tests caught multiple bugs

---

## 👨‍💻 Author

**Task Completion Date:** October 26, 2025

**Status:** ✅ Complete and Production-Ready
