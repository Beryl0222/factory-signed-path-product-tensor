# 有符号工序依赖的稀疏乘积维护

工序关系为带符号的稀疏特征，整数是无比例的合成贡献。 数据为本地合成。

- `data/capture.json` 保存采集原始内容。records 是交换记录，tensors 字段分开描述名称、类型、形状和值。整数索引从零开始，未指定闭合的区间均为左闭右开。
- `data/capacity.json` 保存制造端的容量、规模或分片条件。大尺寸是接口需考虑的使用规模。工作区计入新分配的解包、转换、归约、版本暂存及交接缓冲。调用方已有只读输入和已消费的输出不计入工作区，额外物化副本计入。

## 交换约定

左侧条目坐标为零件、工序，右侧为工序、特征。条目采用 `id,row,col,value`。分片名的数字表示其覆盖行左端，覆盖区间见 `shards`，坐标零起点。提交保存与 `base` 对应的新增条目及撤回身份。接收方 `required` 按行列出必需列，`capacity` 为各行容量，`protect_at` 是绝对值保护线，含等号。数值按精确有符号整数，交付包的允许范围为 int64，超出范围的行不得以绕回后的值交付。

## 读取资料

使用 Python 3.11。首次创建本地环境时安装锁定依赖。只读查看显示原始字段和张量尺寸。

```sh
python -m pip install -r requirements.txt
python caseio.py data/capture.json
python -m unittest discover -s tests -v
```

现有检查核对采集格式和所附数据的结构。
