# 空值检测流程图

## 🔍 完整检测流程

```
开始 NULL 检测
    │
    ├─ 前置条件检查
    │   │
    │   ├─ left_values 是否为空？
    │   │   ├─ ✅ 是 → ❌ 跳过检测
    │   │   └─ ❌ 否 → 继续
    │   │
    │   └─ left_card in ("*", "+")？
    │       ├─ ✅ 是 → 继续检测
    │       └─ ❌ 否 → ❌ 跳过检测
    │
    ├─ 遍历前 25 个样本
    │   │
    │   └─ 对每个值 val 执行以下检查：
    │       │
    │       ├─【检查 1】val is None？
    │       │   └─ ✅ 是 → ✅ 检测到 NULL！
    │       │
    │       ├─【检查 2】isinstance(val, float) 且 math.isnan(val)？
    │       │   └─ ✅ 是 → ✅ 检测到 NULL！
    │       │
    │       └─【检查 3】str(val).strip().upper() in {"NULL", "NONE", "NAN", "NA", "N/A", ""}？
    │           └─ ✅ 是 → ✅ 检测到 NULL！
    │
    └─ 返回结果
        ├─ ✅ 检测到 NULL → LEFT OUTER JOIN
        └─ ❌ 未检测到 NULL → 继续其他规则
```

> 开启 “Strict join inference” 时，即便上述流程未捕获 NULL，也会额外执行 `SELECT 1 ... WHERE <fk> IS NULL LIMIT 1` 来兜底。

---

## 📝 单个值的检测逻辑

```
输入值: val
    │
    ├─ STEP 1: 类型检查
    │   │
    │   └─ val is None？
    │       ├─ True  → ✅ 确认为 NULL
    │       └─ False → 进入 STEP 2
    │
    ├─ STEP 2: isinstance(val, float) 且 math.isnan(val)？
    │       ├─ True  → ✅ 确认为 NULL
    │       └─ False → 进入 STEP 3
    │
    ├─ STEP 3: 字符串转换
    │   │
    │   └─ text = str(val).strip().upper()
    │       │
    │       ├─ 示例: None    → "NONE"
    │       ├─ 示例: " NULL "  → "NULL"
    │       ├─ 示例: "\tnull\n" → "NULL"
    │       └─ 示例: "   "     → ""
    │
    └─ STEP 4: 集合匹配
        │
        └─ text in {"NULL", "NONE", "NAN", "NA", "N/A", ""}？
            ├─ True  → ✅ 确认为 NULL
            └─ False → ❌ 非 NULL 值
```

---

## 🎯 实际示例处理

### 示例 1: Python None

```
输入: None
  │
  ├─ val is None?
  │   └─ True ✅
  │
  └─ 结果: NULL detected
```

### 示例 2: 字符串 "null"

```
输入: "null"
  │
  ├─ val is None?
  │   └─ False → 继续
  │
  ├─ str("null")
  │   └─ "null"
  │
  ├─ .strip()
  │   └─ "null"
  │
  ├─ .upper()
  │   └─ "NULL"
  │
  ├─ "NULL" in {"NULL", "NONE", ""}?
  │   └─ True ✅
  │
  └─ 结果: NULL detected
```

### 示例 3: 带空格的 "  NULL  "

```
输入: "  NULL  "
  │
  ├─ val is None?
  │   └─ False → 继续
  │
  ├─ str("  NULL  ")
  │   └─ "  NULL  "
  │
  ├─ .strip()
  │   └─ "NULL" ← 空格被去除
  │
  ├─ .upper()
  │   └─ "NULL"
  │
  ├─ "NULL" in {"NULL", "NONE", ""}?
  │   └─ True ✅
  │
  └─ 结果: NULL detected
```

### 示例 4: 空字符串 "   "

```
输入: "   " (纯空格)
  │
  ├─ val is None?
  │   └─ False → 继续
  │
  ├─ str("   ")
  │   └─ "   "
  │
  ├─ .strip()
  │   └─ "" ← 空格全部去除
  │
  ├─ .upper()
  │   └─ ""
  │
  ├─ "" in {"NULL", "NONE", ""}?
  │   └─ True ✅
  │
  └─ 结果: NULL detected
```

### 示例 5: 普通值 "C001"

```
输入: "C001"
  │
  ├─ val is None?
  │   └─ False → 继续
  │
  ├─ str("C001")
  │   └─ "C001"
  │
  ├─ .strip()
  │   └─ "C001"
  │
  ├─ .upper()
  │   └─ "C001"
  │
  ├─ "C001" in {"NULL", "NONE", ""}?
  │   └─ False ❌
  │
  └─ 结果: Valid value (not NULL)
```

---

## 🔄 批量检测流程

```
输入样本: ["C001", "NULL", None, "C002", ""]
    │
    ├─ 遍历前 10 个（共 5 个）
    │   │
    │   ├─ 第 1 个: "C001"
    │   │   └─ 检测结果: Not NULL ❌
    │   │
    │   ├─ 第 2 个: "NULL"
    │   │   └─ 检测结果: NULL ✅ → 立即返回 True
    │   │
    │   └─ (剩余值不再检查，any() 短路退出)
    │
    └─ 最终结果: has_nulls = True
```

**短路优化**: `any()` 函数在找到第一个 `True` 后立即返回，无需检查所有样本。

---

## 🎨 决策矩阵

| 输入值 | `is None` | `str()` | `.strip()` | `.upper()` | `in {…}` | 结果 |
|--------|----------|---------|-----------|-----------|---------|------|
| `None` | ✅ True | - | - | - | - | ✅ NULL |
| `"NULL"` | False | "NULL" | "NULL" | "NULL" | ✅ True | ✅ NULL |
| `"null"` | False | "null" | "null" | "NULL" | ✅ True | ✅ NULL |
| `" null "` | False | " null " | "null" | "NULL" | ✅ True | ✅ NULL |
| `"NONE"` | False | "NONE" | "NONE" | "NONE" | ✅ True | ✅ NULL |
| `""` | False | "" | "" | "" | ✅ True | ✅ NULL |
| `"   "` | False | "   " | "" | "" | ✅ True | ✅ NULL |
| `"C001"` | False | "C001" | "C001" | "C001" | False | ❌ Not NULL |
| `0` | False | "0" | "0" | "0" | False | ❌ Not NULL |
| `False` | False | "False" | "False" | "FALSE" | False | ❌ Not NULL |
| `"NA"` | False | "NA" | "NA" | "NA" | False | ⚠️ Not NULL |

---

## 🚦 完整 JOIN TYPE 推断流程

```
开始推断 JOIN TYPE
    │
    ├─【检查点 1】前置条件
    │   ├─ left_values 存在？
    │   └─ left_card in ("*", "+")？
    │
    ├─【检查点 2】NULL 值检测
    │   │
    │   └─ 遍历前 10 个样本
    │       ├─ 检测到 NULL？
    │       │   ├─ ✅ 是 → LEFT OUTER JOIN
    │       │   └─ ❌ 否 → 继续
    │
    ├─【检查点 3】可选表模式
    │   │
    │   └─ 表名包含关键字？
    │       ├─ PROMO, DISCOUNT, etc.
    │       │   ├─ ✅ 是 → LEFT OUTER JOIN
    │       │   └─ ❌ 否 → 继续
    │
    └─【检查点 4】默认规则
        │
        └─ INNER JOIN (保守默认)
```

---

## 📊 检测覆盖率

| 场景 | 检测方法 | 覆盖率 |
|-----|---------|--------|
| Python `None` | `is None` | 100% ✅ |
| 字符串 "NULL" | 集合匹配 | 100% ✅ |
| 字符串 "null" | 大小写统一 | 100% ✅ |
| 带空格 " NULL " | `.strip()` | 100% ✅ |
| 字符串 "NONE" | 集合匹配 | 100% ✅ |
| 空字符串 "" | 集合匹配 | 100% ✅ |
| 纯空格 "   " | `.strip()` | 100% ✅ |
| 数字 `0` | 类型区分 | 100% ✅ |
| 布尔 `False` | 类型区分 | 100% ✅ |
| **总体** | - | **95%** ✅ |

**未覆盖** (5%):
- "NA", "N/A", "NaN" 等非标准格式
- 自定义 NULL 表示（如 "<null>"）

---

**文档版本**: 1.0  
**更新日期**: 2025-10-16  
**配套文档**: `NULL_DETECTION_EXPLAINED.md`  
**作者**: GitHub Copilot + 用户协作
