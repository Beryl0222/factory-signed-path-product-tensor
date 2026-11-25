# 有符号工序依赖的稀疏乘积维护

工序关系为带符号的稀疏特征，整数是无比例的合成贡献。数据为本地合成。

本仓库现在包含一条可直接调用的链路：

1. 按贡献身份接收两侧增删；
2. 增量计算带符号 `C = A @ B` 的 SpGEMM，并保留每条 join 依赖；
3. 发布不可变修订和按接收方策略裁剪的行包；
4. 比较修订、追踪仍在使用旧行的接收方；
5. 将未改变的行包按内容哈希复用，不重写文件。

## 交换约定

- `data/capture.json` 保存采集原始内容。`records` 是交换记录，`tensors` 字段分开描述名称、类型、形状和值。整数索引从零开始，未指定闭合的区间均为左闭右开。
- `data/capacity.json` 保存制造端的容量、规模或分片条件。大尺寸是接口需考虑的使用规模。`peak_scratch_bytes` 是临时解包、转换、归约、版本暂存和交接缓冲的工作区上限；只读输入和已消费的最终输出不计入其中。
- 左侧条目坐标为零件、工序，右侧为工序、特征。条目采用 `id,row,col,value`。
- 分片名的数字表示其覆盖行左端，覆盖区间见 `shards`。
- 提交保存与 `base` 对应的新增条目及撤回身份。
- 接收方 `required` 按行列出必需列，`capacity` 为各行容量，`protect_at` 是绝对值保护线，含等号。
- 数值按精确有符号整数计算；交付 wire 格式只允许 signed int64，超出范围的选中行报 `int64_overflow`，不会绕回。

## 本地检查

使用 Python 3.11。仓库只在读取采集张量的辅助脚本中使用 NumPy；核心链路仅用标准库做精确整数和依赖管理。

```sh
python -m pip install -r requirements.txt
python caseio.py data/capture.json
python -m unittest discover -s tests -v
```

运行样例链路并写行包：

```sh
python -m adjlink process data/capture.json \
  --capacity data/capacity.json \
  --trace detector:0 \
  --diff r1:r2 --diff-receiver detector \
  --out dist/packages
```

## Python 调用

### 一键重放采集资料

```python
from adjlink import process_capture

pipeline = process_capture(
    "data/capture.json",
    capacity="data/capacity.json",
    out="dist/packages",
)

pipeline.store.order                 # ['r1', 'r2']；stale 提交被拒绝
pipeline.stuck_rows("detector")      # 第 0 行 protected_overflow
trace = pipeline.trace_row("detector", 0)
diff = pipeline.compare("r1", "r2", receiver_id="solver")
effective = pipeline.read_effective_row("detector", 0)
```

`CorrectionPipeline.from_capture(..., apply_edits=True)` 也可以直接接收已经读出的 JSON 字典。若只想构造后自行提交，传 `apply_edits=False`。

### 直接使用版本化 SpGEMM

```python
from adjlink import RevisionStore, ReceiverPolicy, package_row

store = RevisionStore(
    initial_revision="r1",
    left_records=[
        ["l-a", 0, 0, 2],
        ["l-dupe", 0, 0, 1],
        ["l-b", 0, 1, 3],
    ],
    right_records=[
        ["r-a", 0, 1, 2],
        ["r-b", 1, 1, -2],
    ],
    left_shards=[[0, 2]],
    right_shards=[[0, 2]],
)

report = store.submit({
    "id": "r2",
    "base": "r1",
    "left_remove": ["l-dupe"],
    "right_remove": ["r-b"],
    "left_add": [["l-new", 0, 2, 2]],
})

if not report.accepted:
    print(report.code, report.readable_revision, report.issues)

row_view = store.row_view(0, "r2")
print(row_view.dependency_summary.to_dict())
```

也可以用函数式入口：

```python
from adjlink import signed_spgemm, spgemm_values

version = signed_spgemm(left_records, right_records)
values = spgemm_values(left_records, right_records)
```

## 关键语义

### 带符号零、缺席和依赖

- 不同 `id` 即使坐标相同也是独立贡献；乘积坐标保留所有 `(left_id, right_id)` join 路径。
- 撤回只按贡献 `id` 生效。引用了当前基版中不活跃/不存在的身份会返回 `unknown_contribution`，当前可读版本不变。
- 只要存在至少一条活跃 join 路径，即使聚合值为零，该坐标仍在 `GraphVersion.product_rows` 中。
- `ProductCoordinate.zero_kind`：
  - `canceled`：存在非零路径但相互抵消；
  - `explicit_zero_path`：所有路径值均为零，即某侧显式零贡献参与了 join；
  - 非零坐标为 `None`。
- 完全没有 join 路径时坐标才是 `None`/缺席。必需但缺席的接收项会以 `present=False, zero_kind="absent"` 保留在诊断结构中，wire 值为零；训练侧仍可区分它和显式零。

样例中的 `(0,1)` 在 r1 为 `2*2 + 1*2 + 3*(-2)=0`，撤回 `l-dupe` 和 `r-b` 后在 r2 恢复为 4。

### 乐观基版与原子可读快照

- 提交必须给 `base`。只有基版等于当前可读 head 且所有引用有效时才接受。
- 过期基版返回 `accepted=False, code="stale_base"`；未知基版返回 `unknown_base`；坏身份返回 `invalid_commit`。
- `CommitReport.readable_revision` 给出仍可读取的版本，`issues` 列出全部原因。
- 一次提交中的 `left_add/remove` 和 `right_add/remove` 一起构造候选快照。`CorrectionPipeline.current` 是不可变 `Release`，读者要么看到旧 release，要么看到新 release。
- 文件发布使用不可变的 `packets/<package_hash>.json`、`releases/<rev>.json`，最后原子替换 `current-release.json`。

### 接收方行包

`ReceiverPolicy.from_dict(...)` 支持：

- `capacity`：整数、按行 list、或 `{"0": 2, "default": N}`；
- `protect_at`：数值或按行配置；
- `required`：`{"行号": [列号, ...]}` 或按行 list。

每行选择顺序：

1. 必需坐标全部保留，包括数值为零者；
2. `abs(value) >= protect_at` 的非必需坐标加入强制集合，等号包含；
3. 剩余候选按 `(-abs(value), col)` 排序，即绝对值降序、同幅值较小列优先；
4. 剩余容量取排序靠前的可选坐标。

若必需坐标数超过容量，返回 `required_overflow`；若必需坐标放得下但保护坐标使强制集合超过容量，返回 `protected_overflow`。两者都使该行 `deliverable=False`，从而阻止该接收方完整换到目标版，但不影响其他接收方。默认部署视图采用“接收方原子切换”：只要有一行 blocked，该接收方所有行继续沿用上一完整版本，`deferred_rows` 会列出被连带推迟的可交付行变更。若边缘侧需要按行灰度，`CorrectionPipeline.row_wise_preview(...)` 会只钉住 blocked 旧行而推进健康行。所有普通可选坐标仍在 `dropped`/`optional_ranked` 中标记为可舍弃；它们保留在图版本里，后续竞争对手被撤回或自身幅值变化后可重新入行包。

### 增量行与文件复用

提交只计算受影响的输出零件行：

- 左侧增删直接标记其零件行；
- 右侧工序行 `k` 的增删，通过 `left_by_process[k]` 反查所有经过该工序的零件行；
- 同一修订同时改两侧时，取新旧反查索引并集。

`package_revision(..., previous=old_set, changed_rows=report.affected_rows)` 对不在集合中的行直接复用原 `RowPackage` 对象，结果中分别给出 `rebuilt_rows` 和 `reused_rows`。文件存储按完整包哈希写不可变 blob；未改变的行不会得到新哈希，也不会重写。

### 比较与追查

```python
diff = pipeline.compare("r1", "r2", receiver_id="detector")

for change in diff.numeric_changes:
    print(change.row, change.col, change.before_value, change.after_value)

for change in diff.eligibility_only_changes:
    print(change.row, change.col, change.eligibility_before, change.eligibility_after)
```

- `numeric_changes`：两侧都存在且聚合整数值变化；从零恢复会带 `recovered_from_zero`。
- 出现/消失、依赖变化、零类型变化分别在 `change_types` 中标记，避免把缺席当成数值零。
- `eligibility_only_changes`：数值和存在性未变，但 required/protected/selected/dropped 或行可交付状态改变。

当接收方仍使用旧行时：

```python
pipeline.stuck_rows("detector")
trace = pipeline.trace_row("detector", 0)
trace.to_dict()
```

追踪结果包含卡住行、目标版 blocker、旧包、逐修订的输入增删、哪些右侧工序修订通过哪些左侧 identity 关联到该行、坐标前后值以及高幅值坐标的完整路径依赖。

## 稀疏复杂度

令一次修订影响的输出行集合为 `R`，所有活跃 join 路径数为

`P = Σ_k nnz(A[:, k]) · nnz(B[k, :])`。

首次构造为 `O(P)` 量级（加上行内排序），依赖和路径按稀疏对象保留；没有分配 `m × n` 稠密乘积。增量提交只重算 `R` 中各行。主要留存空间是稀疏输入、乘积坐标和路径依赖；单行临时归约空间取决于该行的唯一列和路径数。`WorkspaceBudget.batch_size(...)` 可按 `peak_scratch_bytes` 为调用方自己的批量缓冲计算批次大小。
