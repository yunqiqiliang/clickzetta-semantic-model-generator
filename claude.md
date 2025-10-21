# Claude Code 项目指南

> 📋 **重要说明**: 此文档基于项目实际代码生成，所有配置示例、API签名、目录结构等信息均来自真实的源代码文件，确保准确性和实用性。

## 项目概述

**ClickZetta Semantic Model Generator** 是一个为ClickZetta团队构建的Streamlit伴侣工具。它用于探索湖仓元数据、编写和优化语义YAML，并集成合作伙伴工作流——所有功能默认基于ClickZetta的湖仓API和后端卷运行。

**核心功能** (基于README.md)：
- 📊 **本地语义建模伴侣** - 快速迭代YAML，检查元数据，在提升到湖仓前验证更改
- 🎯 **与ClickZetta生产保持同步** - 在ClickZetta控制台构建和管理规范模型，需要更丰富的编辑、合作伙伴集成或AI增强时切换到此应用
- 🤖 **AI增强描述** - 通过DashScope自动丰富文档
- 🔌 **MCP集成** - 支持Model Context Protocol集成
- 🌐 **语义沙盒** - 从卷中拉取模型，使用编辑器和聊天助手实验，通过验证后推送优化的YAML

## 项目结构

```
semantic-model-generator/
├── app.py                          # Streamlit主应用入口
├── app_utils/                      # UI工具和共享逻辑
│   ├── shared_utils.py            # 核心UI工具函数
│   ├── chat.py                    # 聊天助手功能
│   └── ...
├── semantic_model_generator/       # 核心业务逻辑
│   ├── clickzetta_utils/          # ClickZetta连接和查询
│   │   ├── clickzetta_connector.py # 数据库连接器
│   │   └── env_vars.py            # 环境配置
│   ├── data_processing/           # 数据处理和转换
│   ├── relationships/             # 🔥 关系发现核心模块
│   │   ├── discovery.py          # 关系发现API
│   │   └── ...
│   ├── validate/                  # YAML和SQL验证
│   │   └── keywords.py           # ClickZetta保留字
│   ├── protos/                   # Protobuf消息定义
│   └── generate_model.py         # 🔥 核心模型生成逻辑
├── journeys/                     # 迭代流程和用户路径
├── partner/                      # 合作伙伴适配器
└── tests/                       # 测试套件
```

## 开发环境设置

### 环境要求
- **Python**: `>=3.9,<3.9.7 || >3.9.7,<3.12` (不支持3.12，避免3.9.7特定版本)
- **Poetry**: 用于依赖管理
- **ClickZetta**: 需要有效的ClickZetta连接配置

### 快速开始
```bash
# 1. 设置Python环境
poetry env use python3.11

# 2. 安装依赖
make setup
# 或者: poetry install

# 3. 配置连接
# 创建 connections.json 文件 (参见下方配置说明)

# 4. 启动应用
poetry run streamlit run app.py
# 或者: make run_admin_app
```

### 开发工具
```bash
# 代码格式化和检查
make fmt_lint              # Black + isort + flake8

# 类型检查
make run_mypy

# 运行测试
make test
# 或者: poetry run pytest -q
# ClickZetta连接器测试: poetry run pytest -k clickzetta
```

## 配置说明

### ClickZetta连接配置

项目会在以下位置查找 `connections.json` (按优先级顺序)：
1. `/app/.clickzetta/lakehouse_connection/connections.json`
2. `/app/config/lakehouse_connection/connections.json`
3. `config/connections.json`
4. `config/lakehouse_connection/connections.json`
5. `~/.clickzetta/connections.json`
6. `/app/.clickzetta/connections.json`

**连接配置示例** (来自README.md)：
```json
{
  "system_config": {
    "embedding": {
      "provider": "dashscope",
      "dashscope": {
        "api_key": "dashscope_api_key",
        "model": "qwen-plus-latest"
      }
    }
  },
  "connections": [
    {
      "connection_name": "dev",
      "is_default": true,
      "service": "cn-shanghai-alicloud.api.clickzetta.com",
      "instance": "your_instance",
      "workspace": "quick_start",
      "schema": "PUBLIC",
      "username": "user",
      "password": "password",
      "vcluster": "default_ap"
    }
  ]
}
```

### 环境变量配置

基于项目的 `.env.example` 和 `env_vars.py`：

```bash
# ClickZetta连接配置 (覆盖connections.json中的值)
CLICKZETTA_SERVICE=your_service_url
CLICKZETTA_INSTANCE=your_instance_name
CLICKZETTA_WORKSPACE=your_workspace
CLICKZETTA_SCHEMA=PUBLIC
CLICKZETTA_USERNAME=your_username
CLICKZETTA_PASSWORD=your_password
CLICKZETTA_VCLUSTER=default_ap

# 可选配置
CLICKZETTA_QUERY_TAG=semantic-model-generator
CLICKZETTA_SESSION_TIMEOUT_SEC=300

# DashScope配置 (通过环境变量覆盖connections.json)
DASHSCOPE_API_KEY=your_dashscope_key
DASHSCOPE_MODEL=qwen-plus-latest
DASHSCOPE_BASE_URL=https://dashscope.aliyuncs.com/api/v1
DASHSCOPE_TEMPERATURE=0.2
DASHSCOPE_TOP_P=0.85
DASHSCOPE_MAX_OUTPUT_TOKENS=512
DASHSCOPE_TIMEOUT_SECONDS=45.0
```

**配置优先级**：环境变量 > connections.json 配置文件

## 核心模块详解

### 🔥 关系发现 (Relationships)

**核心文件**: `semantic_model_generator/relationships/discovery.py`

**主要API**:
```python
from semantic_model_generator.relationships.discovery import (
    discover_relationships_from_schema,
    discover_relationships_from_table_definitions
)

# 从ClickZetta schema发现关系
result = discover_relationships_from_schema(
    session=session,
    workspace="MY_WORKSPACE",
    schema="PUBLIC",
    max_relationships=100,
    min_confidence=0.5
)

# 从表定义发现关系
table_definitions = [
    {
        "table_name": "users",
        "columns": [
            {"name": "id", "type": "NUMBER", "is_primary_key": True},
            {"name": "username", "type": "STRING"}
        ]
    }
]
result = discover_relationships_from_table_definitions(table_definitions)
```

**关系发现算法特点**:
- **多策略匹配**: 直接列名匹配 + 后缀匹配 + 语义相似性
- **智能基数推断**: One-to-One, Many-to-One, Many-to-Many
- **复合键支持**: 自动检测和处理多列关系
- **业务知识增强**: 内置常见业务模式识别
- **自适应阈值**: 根据数据特征动态调整匹配阈值

**最近修复** (2025-10-21):
- ✅ 修复了"id"列导致的复合键过度生成问题
- ✅ 改善了非标准表名的语义匹配
- ✅ 增强了误报防护机制

### 🔥 模型生成 (Core Logic)

**核心文件**: `semantic_model_generator/generate_model.py`

**关键函数**:
- `_infer_relationships()` - 关系推断核心算法
- `_is_valid_suffix_match()` - 语义匹配验证 (新增)
- `_table_variants()` - 表名变体生成
- `_looks_like_foreign_key()` - 外键模式识别

### ClickZetta集成

**连接器**: `semantic_model_generator/clickzetta_utils/clickzetta_connector.py`

**数据查询策略**:
1. **主要途径**: `information_schema.tables` + `information_schema.columns`
2. **Fallback**: `SHOW COLUMNS` → `DESCRIBE TABLE`
3. **主键检测**: 依赖 `information_schema.columns.is_primary_key`

**重要限制**:
- ⚠️ `SHOW COLUMNS`/`DESCRIBE TABLE` 不提供主键信息
- ⚠️ `sys.information_schema` 约束表通常不存在
- ✅ 所有查询使用大写标识符

## 常见使用场景

### 1. 标准数据仓库场景
```python
# 推荐配置
discover_relationships_from_schema(
    session=session,
    workspace="DW",
    schema="PUBLIC",
    max_relationships=50,      # 控制结果数量
    min_confidence=0.6,        # 较高置信度
    strict_join_inference=True # 启用SQL NULL检测
)
```

### 2. 探索性分析场景
```python
# 宽松配置，发现更多可能的关系
discover_relationships_from_schema(
    session=session,
    workspace="EXPLORE",
    schema="RAW_DATA",
    max_relationships=100,
    min_confidence=0.4,        # 较低置信度
    timeout_seconds=60         # 更长处理时间
)
```

### 3. 离线表定义分析
```python
# 无需数据库连接的场景
table_definitions = load_table_metadata_from_file()
result = discover_relationships_from_table_definitions(
    table_definitions,
    default_workspace="OFFLINE",
    min_confidence=0.5
)
```

## 最佳实践

### ✅ 推荐做法

1. **表名设计**:
   ```sql
   -- 推荐: 语义化表名
   customers, orders, order_items

   -- 支持: 下划线分割
   dim_customer, fact_orders
   ```

2. **列名设计**:
   ```sql
   -- 推荐: 完整语义名称
   customer_id, order_id, product_id

   -- 避免: 过于简化
   id (在多表场景中会导致误匹配)
   ```

3. **主键设置**:
   - 确保 `information_schema.columns.is_primary_key` 正确设置
   - 使用有意义的主键名称而非简单的"id"

### ❌ 避免的问题

1. **列名冲突**:
   ```sql
   -- 问题: 多个表都有"id"列
   users(id, name)      -- 改为: users(user_id, name)
   posts(id, title)     -- 改为: posts(post_id, title)
   ```

2. **语义不清的表名**:
   ```sql
   -- 问题: 无语义表名
   table_a, table_b, temp_001

   -- 解决: 使用业务含义
   customers, orders, products
   ```

## 故障排除

### 常见问题

#### 1. 没有发现任何关系
**可能原因**:
- 主键信息缺失 (`is_primary_key = False`)
- 表名和列名没有语义关联
- 置信度阈值过高

**解决方案**:
```python
# 降低置信度阈值
result = discover_relationships_from_schema(
    ...,
    min_confidence=0.3  # 从0.5降到0.3
)

# 检查主键设置
df = session.sql("SELECT * FROM information_schema.columns WHERE is_primary_key = true").to_pandas()
```

#### 2. 生成了错误的复合键关系
**症状**: 看到类似 `table_a.col1=table_b.pk AND table_a.col2=table_b.pk` 的关系

**解决**: 已在最新版本修复 (2025-10-21)

#### 3. 连接超时或权限错误
**检查项**:
```sql
-- 验证连接
SELECT current_database(), current_user();

-- 检查表访问权限
SHOW TABLES IN your_schema;

-- 验证information_schema访问
SELECT COUNT(*) FROM information_schema.tables;
```

### 调试技巧

#### 启用详细日志
```python
import logging
logging.getLogger("semantic_model_generator").setLevel(logging.DEBUG)
```

#### 检查中间结果
```python
result = discover_relationships_from_schema(...)

# 查看发现摘要
print(f"Tables: {result.summary.total_tables}")
print(f"Relationships: {result.summary.total_relationships_found}")
print(f"Limited by timeout: {result.summary.limited_by_timeout}")

# 查看具体关系
for rel in result.relationships:
    print(f"{rel.left_table} -> {rel.right_table}")
    for col in rel.relationship_columns:
        print(f"  {col.left_column} = {col.right_column}")
```

## 测试指南

### 运行测试
```bash
# 完整测试套件
poetry run pytest -v

# 关系发现测试
poetry run pytest semantic_model_generator/tests/relationship_discovery_test.py -v

# ClickZetta连接器测试
poetry run pytest -k clickzetta -v
```

### 添加测试
测试文件位置: `semantic_model_generator/tests/`

**示例测试**:
```python
def test_custom_relationship_scenario():
    table_definitions = [
        {
            "table_name": "my_table",
            "columns": [
                {"name": "id", "type": "NUMBER", "is_primary_key": True}
            ]
        }
    ]

    result = discover_relationships_from_table_definitions(table_definitions)

    assert len(result.relationships) == expected_count
    # 添加具体断言...
```

## 代码贡献指南

### Commit规范
使用 Conventional Commits:
```bash
feat: add new relationship discovery algorithm
fix: resolve composite key over-generation issue
docs: update API documentation
test: add relationship discovery test cases
```

### 代码风格
```bash
# 自动格式化
make fmt_lint

# 类型检查
make run_mypy

# 确保测试通过
make test
```

### Pull Request清单
- [ ] 代码通过 `make fmt_lint`
- [ ] 类型检查通过 `make run_mypy`
- [ ] 测试通过 `make test`
- [ ] 添加了相应的测试用例
- [ ] 更新了相关文档
- [ ] 测试了ClickZetta集成 (如适用)

## 依赖说明

### 核心依赖
- **clickzetta-connector-python** (≥0.8.92) - ClickZetta数据库连接
- **clickzetta-zettapark-python** (≥0.1.3) - ClickZetta SQL引擎
- **streamlit** (1.36.0) - Web界面
- **pandas** (^2.0.1) - 数据处理
- **protobuf** (5.26.1) - 消息序列化

### AI增强依赖
- **dashscope** (^1.22.2) - 阿里云DashScope API客户端

### 开发依赖
- **pytest** (^8.1.1) - 测试框架
- **mypy** (^1.9.0) - 类型检查
- **black** (^24.3.0) - 代码格式化
- **flake8** (^7.0.0) - 代码检查

## API参考

### 关系发现API

#### `discover_relationships_from_schema()`
```python
def discover_relationships_from_schema(
    session: Session,                    # ClickZetta会话
    workspace: str,                      # 工作区名称
    schema: str,                         # Schema名称
    *,
    table_names: Optional[Sequence[str]] = None,  # 限制表范围
    sample_values_per_column: int = 10,           # 每列采样值数
    strict_join_inference: bool = False,          # 启用SQL NULL检测
    max_workers: int = 4,                         # 并行采样worker数
    max_relationships: Optional[int] = None,      # 关系数上限
    min_confidence: float = 0.5,                  # 最小置信度
    timeout_seconds: Optional[float] = 30.0,     # 超时设置
    max_tables: Optional[int] = 60,               # 分析表数上限
) -> RelationshipDiscoveryResult
```

#### `discover_relationships_from_table_definitions()`
```python
def discover_relationships_from_table_definitions(
    table_definitions: Sequence[Mapping[str, Any]], # 表定义列表
    *,
    default_workspace: str = "OFFLINE",              # 默认工作区
    default_schema: str = "PUBLIC",                  # 默认Schema
    strict_join_inference: bool = False,
    session: Optional[Session] = None,
    max_relationships: Optional[int] = None,
    min_confidence: float = 0.5,
    timeout_seconds: Optional[float] = 15.0,
    max_tables: Optional[int] = None,
) -> RelationshipDiscoveryResult
```

#### 返回类型
```python
@dataclass
class RelationshipDiscoveryResult:
    relationships: List[semantic_model_pb2.Relationship]  # 发现的关系
    tables: List[Table]                                   # 处理的表
    summary: RelationshipSummary                          # 执行摘要

@dataclass
class RelationshipSummary:
    total_tables: int                     # 总表数
    total_columns: int                    # 总列数
    total_relationships_found: int        # 发现关系数
    processing_time_ms: int              # 处理时间(毫秒)
    limited_by_timeout: bool = False     # 是否超时限制
    limited_by_max_relationships: bool = False  # 是否关系数限制
    limited_by_table_cap: bool = False   # 是否表数限制
    notes: Optional[str] = None          # 附加说明
```

## 版本历史

### v1.0.19 (最新)
当前开发版本

### v1.0.16
- 🐛 修复关系发现中的复合键过度生成问题
- ✨ 改善非标准表名的语义匹配
- 📝 更新文档，修正information_schema相关说明

### 历史版本
参见 `CHANGELOG.md` 了解完整版本历史。

## 许可证

Apache Software License & BSD License

## 支持

- **问题报告**: GitHub Issues
- **文档**: 项目README和本指南
- **代码指南**: `AGENTS.md`
- **修复记录**: `RELATIONSHIP_DISCOVERY_BUG_FIXES.md`

---

*此文档为Claude Code专用项目指南，包含了项目的完整技术信息和使用说明。*