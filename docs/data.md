# 数据集获取

原始卡口数据共三份规模，按 `vid,loc,unix_ts` 三列存放。开发先用 1d 跑通，扩到 7d/31d 做里程碑验证。数据格式见 [伴随车数据说明.md](../伴随车数据说明.md)。

## 三种规模

| 名称 | 大小 | 行数 | 时间范围（UTC） | 用途 |
|---|---|---|---|---|
| `mini` | 2.0 MB | 100,000 | 2015-01-01 00:00:00 – 00:46:51 | **仅供自动化测试 / CI**，不用作开发样例 |
| `1d` | 195 MB | 9,279,659 | 2015-01-01 00:00 – 23:59 | M1 开发与正确性验证（本地手跑） |
| `7d` | 1.4 GB | 66,885,793 | 2015-01-01 – 2015-01-07 | M2 集群跑通与热点切分验证 |
| `31d` | 5.8 GB | 275,893,209 | 2015-01-01 – 2015-01-31 | M3 全量评测 |

`1d.csv` / `7d.csv` 都由 `31d.csv` 按时间戳前缀切出（数据本身按 ts 升序），保证是 31d 的真子集，便于跨规模做正确性 diff：

```bash
# 1d: ts ∈ [1420041600, 1420128000)
awk -F',' '$3>=1420041600 && $3<1420128000 {print} $3>=1420128000 {exit}' 31d.csv > 1d.csv
# 7d: ts ∈ [1420041600, 1420646400)
awk -F',' '$3>=1420041600 && $3<1420646400 {print} $3>=1420646400 {exit}' 31d.csv > 7d.csv
```

## 校验

```text
sha256  31d.csv             ce1cfc066248af5db6745c14ed63431213443ba53e45547d92ed6eed0960f7ea
sha256  7d.csv              ce00305a48bd1fdc1933faa17a2cc1f669004ce1e26b53039919bd29da426d13
sha256  1d.csv              63bc24213f6701373924fdadcab0ec3ff76e9fa4b342ca013e60d9510adc0287
sha256  tests/data/mini.csv 4dc507f55766492b28ea4b4adaa2e69127febcf94a7ab8ac9d4b0aaefb2ab79f
```

下载完用 `sha256sum` 比对，避免半传文件污染流水线。

## 分发方式

**集群路径才是唯一可信源。** 维护者在本地通过 `scripts/upload_to_hdfs.sh`（脚本内部走 scp + ssh，本地无需 hadoop）把 csv 推到 HDFS 的 `${COMPANION_ROOT}/input/raw/`（默认 `/companion/input/raw/`），所有阶段都从这里读输入：

```bash
# 维护者：一次性推送（之后增量更新也走这条命令，-put -f 覆盖）
scripts/upload_to_hdfs.sh 1d.csv 7d.csv 31d.csv
```

组员有两种用法：

### 1. 直接对 HDFS 跑（推荐）

不必把 5.8 GB 拉回本地。Stage0 直接读 `${COMPANION_ROOT}/input/raw/${phase}.csv`：

```bash
scripts/cluster_run.sh --days 1 --build
```

### 2. 把样例拉到本地做单机调试

```bash
scripts/fetch_dataset.sh 1d      # 195 MB，开发常用
scripts/fetch_dataset.sh 7d      # 1.4 GB，M2 本地调试或 baseline diff
scripts/fetch_dataset.sh 31d     # 5.8 GB，仅当确实需要本地全量时
```

脚本会校验大小、跳过已存在的同名同大小文件，幂等。

`mini` 不走 HDFS——它已经在 [tests/data/mini.csv](../tests/data/mini.csv) 跟随 git 仓库一起 clone 下来，**仅供测试代码引用**，不要在它上面做开发调试或算法演示（时间跨度只有 47 分钟，会让你对真实数据分布产生错误印象）。

## 不在 git 仓库里的原因

[.gitignore](../.gitignore) 用 `*.csv` 排除所有 CSV，仅以 `!tests/**/*.csv` 一条白名单放过测试 fixture。所以 `1d.csv` / `7d.csv` / `31d.csv` / baseline 输出 / 任何临时 csv 都永远走分发渠道而不是 git 历史，`tests/data/mini.csv` 是唯一进 git 的 CSV。
