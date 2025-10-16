# 📊 从 ClickZetta 表中查询空值的 SQL 详解

## 🎯 核心查询语句

系统默认使用以下 SQL 从 ClickZetta 表中采样列值（包括空值）：

```sql
SELECT DISTINCT column_name FROM workspace.schema.table LIMIT 10
```

**代码位置**: `semantic_model_generator/clickzetta_utils/clickzetta_connector.py` 第 225 行

---

## 🔍 完整查询流程

### 1️⃣ **SQL 生成逻辑**

```python
def _get_column_sample_values(
    session: Session,
    workspace: str,
    schema_name: str,
    table_name: str,
    column_name: str,
    ndv: int,  # 默认值为 10（可通过 UI 配置更大）
) -> Optional[List[str]]:
    # 构建完全限定的表名
    qualified_table = f"{workspace}.{schema}.table"
    
    # 生成查询 SQL
    query = f"SELECT DISTINCT {column_part} FROM {qualified_table} LIMIT {ndv}"
    
    # 执行查询并转换为 Pandas DataFrame
    df = session.sql(query).to_pandas()
    
    # 提取第一列的所有值（包括 NULL）
    first_col = df.columns[0]
    return [str(value) for value in df[first_col].tolist()]
```

**关键参数**:
- `ndv`: Number of Distinct Values，默认值为 10（在 `generate_model.py` 中定义为 `_DEFAULT_N_SAMPLE_VALUES_PER_COL`），建议在需要时调大（例如 50/100）。
- 采样结果会在生成器侧截取前 25 项用于 NULL 检测。

### 🔒 严格模式下的补充查询

当勾选 “Strict join inference” 时，还会对每个候选外键执行一条确认空值的探测 SQL：

```sql
SELECT 1
FROM <workspace>.<schema>.<table>
WHERE <column> IS NULL
LIMIT 1;
```

- 只要该查询返回记录，就会认定该列存在 NULL，从而强制推断为 `LEFT OUTER JOIN`。
- 查询结果按 `<database>.<schema>.<table>.<column>` 缓存，避免重复执行。

---

### 2️⃣ **SQL 示例**

假设查询 `FACT_SALES` 表的 `promotion_id` 列（可能包含空值）：

```sql
-- 实际执行的 SQL
SELECT DISTINCT promotion_id 
FROM my_workspace.sales_schema.FACT_SALES 
LIMIT 10;
```

**返回结果示例**:
```
promotion_id
------------
1
2
5
NULL        ← ClickZetta 返回的空值
12
NULL        ← 可能有多个 NULL（DISTINCT 会保留一个）
15
...
```

---

## 🔎 空值在 Python 中的表示

### ClickZetta SQL → Python 转换

当 SQL 查询返回 NULL 值时，ClickZetta SDK 的转换行为：

| ClickZetta SQL | Pandas DataFrame | Python List | NULL 检测 |
|---------------|------------------|-------------|-----------|
| `NULL` | `NaN` (float) | `None` | `val is None` ✅ |
| `"NULL"` (字符串) | `"NULL"` (str) | `"NULL"` | `str(val).upper() == "NULL"` ✅ |
| `""` (空字符串) | `""` (str) | `""` | `str(val).strip() == ""` ✅ |
| `"  "` (空白符) | `"  "` (str) | `"  "` | `str(val).strip() == ""` ✅ |

### 实际代码转换

```python
# ClickZetta SDK 返回的 DataFrame
df = session.sql("SELECT DISTINCT promotion_id FROM FACT_SALES LIMIT 10").to_pandas()

# 转换为 Python 列表（NULL → None）
values = df['promotion_id'].tolist()
# 结果: [1, 2, 5, None, 12, 15, ...]
#                   ^^^^
#                   SQL NULL 被转换为 Python None
```

---

## ✅ NULL 检测逻辑

### 默认采样检测（`generate_model.py` 中 `_infer_join_type`）

```python
sample_window = left_values[: min(len(left_values), 25)]
has_nulls = any(_is_nullish(val) for val in sample_window)
```

`_is_nullish` 会识别：`None`、浮点 `NaN`、以及去除空白后为 `"NULL" / "NONE" / "NAN" / "NA" / ""` 的字符串。

### 严格模式补充检测

```python
if strict_join_inference:
    has_null_fk = _column_has_null_via_query(session, fqn, column)
    if has_null_fk:
        return JoinType.left_outer
```

其中 `_column_has_null_via_query` 会执行上文的 `WHERE column IS NULL LIMIT 1` 探测 SQL，并将结果缓存。

### 检测步骤

```
输入值 → 类型检查 → 字符串转换 → 去空格 → 大写 → 集合匹配
  ↓         ↓           ↓          ↓       ↓        ↓
None  → is None  →    N/A     →   N/A  →  N/A  → ✅ 检测到
"NULL"→ 跳过     → "NULL"    → "NULL" → "NULL"→ ✅ 检测到
"null"→ 跳过     → "null"    → "null" → "NULL"→ ✅ 检测到
"  "  → 跳过     → "  "      → ""     →  ""   → ✅ 检测到
123   → 跳过     → "123"     → "123"  → "123" → ❌ 未检测到
```

---

## 🔬 完整示例

### 场景：检测 `FACT_SALES.promotion_id` 是否可空

#### 步骤 1: SQL 查询
```sql
-- 系统自动执行
SELECT DISTINCT promotion_id 
FROM analytics.sales.FACT_SALES 
LIMIT 100;
```

#### 步骤 2: 结果集
```python
# ClickZetta 返回的数据（Pandas DataFrame）
   promotion_id
0            1
1            2
2         None    ← SQL NULL 转换为 Python None
3            5
4           12
...
```

#### 步骤 3: 转换为列表
```python
left_values = df['promotion_id'].tolist()
# [1, 2, None, 5, 12, ...]
```

#### 步骤 4: NULL 检测
```python
has_nulls = any(
    val is None or str(val).strip().upper() in {"NULL", "NONE", ""}
    for val in left_values[: min(len(left_values), 25)]  # 默认检查前 25 个样本
)
# 结果: True（因为第 3 个值是 None）
```

#### 步骤 5: JOIN 类型推断
```python
if has_nulls:
    join_type = semantic_model_pb2.JoinType.left_outer
    logger.debug(
        "推断 JOIN TYPE: LEFT OUTER (检测到 promotion_id 列有 NULL 值)"
    )
```

---

## 📈 性能分析

### 查询开销

| 操作 | 成本 | 说明 |
|-----|------|------|
| **SQL 查询** | 中等 | `SELECT DISTINCT ... LIMIT N`（默认 10，可配置） - 需要扫描部分数据 |
| **网络传输** | 低 | 默认仅传输 10 行数据（可随采样上限增加） |
| **DataFrame 转换** | 低 | ClickZetta SDK 自动处理 |
| **NULL 检测** | 极低 | O(25) 常数时间，仅检查前 25 个样本 |

### 优化措施

1. **DISTINCT 去重**: 减少重复值，提高样本多样性
2. **LIMIT 10（可调）**: 限制返回行数，避免大表全扫描；可视需要提升上限
3. **仅检查前 25 个**: 降低 NULL 检测开销并兼顾准确度

---

## 🎯 SQL 查询的关键特点

### ✅ 优点

1. **包含 NULL 值**: `SELECT DISTINCT` 会保留 NULL 值
   - NULL 在 SQL 中是独立的值类型
   - DISTINCT 会将所有 NULL 视为一个唯一值

2. **高效采样**: LIMIT 10 避免全表扫描
   - 对于大表（百万行级别）性能友好
   - 如需更高置信度，可将采样上限调到 50、100 甚至更多

3. **ClickZetta 兼容性**: 标准 SQL 语法，无需特殊处理
   - 支持多层级命名空间（workspace.schema.table）
   - 自动处理大小写敏感性

### ⚠️ 注意事项

1. **样本代表性**: LIMIT 10 可能无法覆盖所有边缘情况
   - 如果 NULL 值占比 < 1%，可能采样不到
   - 建议：根据需要将采样数提升到 100、200 或更多

2. **DISTINCT 行为**: 多个 NULL 会被合并为一个
   ```sql
   -- 原表数据
   promotion_id
   ------------
   1
   NULL
   NULL  ← 这些 NULL 在 DISTINCT 后只保留一个
   NULL
   2
   
   -- SELECT DISTINCT 结果
   promotion_id
   ------------
   1
   NULL  ← 仅保留一个 NULL
   2
   ```

3. **字符串 "NULL" vs SQL NULL**:
   - SQL 的 NULL：被 ClickZetta SDK 转换为 Python `None`
   - 字符串 `"NULL"`：保持为字符串 `"NULL"`
   - 系统两者都能检测（见 NULL 检测逻辑）

---

## 🔧 调试方法

### 如何验证 SQL 查询

```python
# 在 clickzetta_connector.py 中添加日志
logger.debug(f"执行 SQL: {query}")

# 查看返回的原始数据
logger.debug(f"返回的 DataFrame:\n{df}")
logger.debug(f"转换后的列表: {[str(v) for v in df[first_col].tolist()]}")
```

### 手动测试 SQL

```sql
-- 直接在 ClickZetta 控制台执行
SELECT DISTINCT promotion_id 
FROM your_workspace.your_schema.FACT_SALES 
LIMIT 100;

-- 检查是否有 NULL 值返回
-- 如果有，会显示为空白或 "NULL"（取决于客户端）
```

---

## 📚 相关文档

- **NULL 检测详解**: `NULL_DETECTION_EXPLAINED.md`
- **JOIN 类型推断**: `JOIN_TYPE_INFERENCE.md`
- **基数推断逻辑**: `CARDINALITY_FIX_REPORT.md`
- **采样优化**: `SAMPLING_OPTIMIZATION.md`

---

## 🚀 最佳实践建议

### 1. 确保表中有足够样本

```sql
-- 检查表的总行数
SELECT COUNT(*) FROM workspace.schema.table;

-- 如果表行数 < 10，LIMIT 10 会返回所有数据
-- 如果表行数远大于 10，可按需将 LIMIT 提升到更高数值（如 100、1000）以提高准确性
```

### 2. 验证 NULL 值占比

```sql
-- 计算 NULL 值占比
SELECT 
    COUNT(*) AS total_rows,
    COUNT(promotion_id) AS non_null_rows,
    COUNT(*) - COUNT(promotion_id) AS null_rows,
    (COUNT(*) - COUNT(promotion_id)) * 100.0 / COUNT(*) AS null_percentage
FROM workspace.schema.FACT_SALES;
```

### 3. 针对低 NULL 占比的列增加采样

如果某列的 NULL 占比 < 1%，可能需要增加采样数：

```python
# 在 generate_model.py 中修改
_DEFAULT_N_SAMPLE_VALUES_PER_COL = 1000  # 从 100 增加到 1000
```

---

## ✅ 总结

| 问题 | 答案 |
|-----|------|
| **使用什么 SQL？** | `SELECT DISTINCT column_name FROM qualified_table LIMIT 10`（可配置） |
| **能查询到 NULL 吗？** | ✅ 能，ClickZetta 会返回 NULL 值 |
| **NULL 如何表示？** | SQL NULL → Pandas NaN → Python `None` |
| **如何检测 NULL？** | `_is_nullish(val)`（包含 `None`、`NaN`、`"NULL"` 等） |
| **性能开销？** | SQL 查询：中等；NULL 检测：极低（O(25)） |
| **准确性如何？** | 默认 10 个样本，可在 UI 中提升至 50/100/1000 以覆盖更多场景 |

**关键代码文件**:
1. SQL 查询生成：`clickzetta_utils/clickzetta_connector.py:225`
2. NULL 检测逻辑：`generate_model.py:456-465`
3. JOIN 类型推断：`generate_model.py:474-554`
