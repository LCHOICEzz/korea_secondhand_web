# Karrot Brand Pipeline

这套脚本用于在 Karrot/Daangn 上按品牌抓取在售商品，并尽快产出可读的中文 HTML 页面。

当前版本已经改成“流水线版”：

- 品牌抓取和品牌后处理分离
- 去掉抓取结果去重
- 去掉按时间排序
- 一个品牌进入后处理时，下一个品牌可以立即开始抓取
- 标题、描述、区域会继续清洗，尽量保证 HTML 里只出现中文

## 目录结构

核心入口脚本：

- `run_pipeline.sh`
- `run_multi_brand_pipeline.py`
- `run_brand_pipeline.py`
- `multi_region_karrot_report.py`
- `fix_multi_region_output.py`

辅助脚本：

- `collect_region_codes.py`
- `repair_brand_results.py`

输出目录默认在：

- `output/brand_runs`

区域 slug 文件默认在：

- `output/daangn_region_slug_list.txt`

## 流水线架构

单品牌完整流程分成两段：

1. `scrape`
   - 跨多个区域搜索品牌词
   - 抓取搜索结果页
   - 抓取详情页
   - 下载图片
   - 生成 `raw` 结果

2. `postprocess`
   - 修复图片引用
   - 翻译标题、描述、区域
   - 清洗掉英文/韩文残留
   - 输出最终 HTML 和 JSON

多品牌批量运行时的调度方式：

1. 品牌 A 先执行 `scrape`
2. 品牌 A 一旦 `scrape` 完成，立即启动 `postprocess`
3. 同时开始品牌 B 的 `scrape`
4. 这样后处理和下一品牌抓取可以并行进行

这就是“流水线版”的核心优化点。

## 运行环境

只依赖 Python 标准库，不需要 `pip install` 第三方包。

最低要求：

- `python3`
- 能访问 `https://www.daangn.com`
- 能访问翻译接口：
  - `https://translate.googleapis.com`
  - `https://api.mymemory.translated.net`

检查 Python：

```bash
python3 --version
```

Ubuntu / Debian 安装：

```bash
sudo apt-get update
sudo apt-get install -y python3
```

macOS：

```bash
xcode-select --install
```

## 区域 slug 文件

脚本依赖区域 slug 列表。格式是每行一个区域，例如：

```txt
강남동-1946
강동면-1932
강문동-4251
견소동-4252
경포동-1948
```

默认使用：

```txt
output/daangn_region_slug_list.txt
```

如果部署到另一台机器，需要把这个文件一并带过去，或者先自行生成。

## 启动方式

### 1. 推荐：统一 Shell 入口

新增统一入口脚本：

```bash
./run_pipeline.sh
```

支持模式：

- `batch`
- `batch-bg`
- `brand`
- `scrape`
- `postprocess`

查看帮助：

```bash
./run_pipeline.sh --help
```

#### 1.1 前台跑多品牌流水线

```bash
./run_pipeline.sh batch
```

#### 1.2 后台跑多品牌流水线

```bash
./run_pipeline.sh batch-bg
```

#### 1.3 跑单个品牌完整流程

```bash
./run_pipeline.sh brand 헤이에스 heys
```

#### 1.4 只跑单个品牌抓取

```bash
./run_pipeline.sh scrape 헤이에스 heys
```

#### 1.5 对已有目录只跑后处理

```bash
./run_pipeline.sh postprocess 헤이에스 output/brand_runs/헤이에스_20260315_120000 heys
```

#### 1.6 通过环境变量覆盖参数

```bash
SLUG_FILE=/opt/openclaw/karrot/output/daangn_region_slug_list.txt \
OUT_ROOT=/opt/openclaw/karrot/output/brand_runs \
LOG_DIR=/opt/openclaw/karrot/logs \
REGION_WORKERS=8 \
DETAIL_WORKERS=5 \
FIX_WORKERS=2 \
./run_pipeline.sh batch-bg
```

### 2. 单品牌完整运行

```bash
cd /path/to/karrot

python3 -u run_brand_pipeline.py \
  --brand 헤이에스 \
  --search-variant heys \
  --slug-file output/daangn_region_slug_list.txt \
  --out-root output/brand_runs \
  --region-workers 8 \
  --detail-workers 5 \
  --final-workers 8 \
  --fix-workers 2 \
  --request-interval 0.32 \
  --request-jitter 0.05 \
  --region-batch-size 120 \
  --region-batch-sleep 3 \
  --detail-batch-size 80 \
  --detail-batch-sleep 2
```

这等价于：

- 先跑 `scrape`
- 再跑 `postprocess`

### 3. 单品牌只跑抓取

```bash
python3 -u run_brand_pipeline.py \
  --brand 헤이에스 \
  --search-variant heys \
  --slug-file output/daangn_region_slug_list.txt \
  --out-root output/brand_runs \
  --mode scrape
```

运行后会生成一个新的品牌目录，例如：

```txt
output/brand_runs/헤이에스_20260315_120000
```

### 4. 单品牌只跑后处理

```bash
python3 -u run_brand_pipeline.py \
  --brand 헤이에스 \
  --run-dir output/brand_runs/헤이에스_20260315_120000 \
  --mode postprocess \
  --fix-workers 2
```

这个模式适用于：

- 抓取已经完成，后处理要重跑
- 节点中断后只重建 HTML / 中文输出

### 5. 多品牌流水线批量运行

```bash
cd /path/to/karrot

python3 -u run_multi_brand_pipeline.py \
  --slug-file output/daangn_region_slug_list.txt \
  --out-root output/brand_runs \
  --sleep-between-brands 35 \
  --region-workers 8 \
  --detail-workers 5 \
  --final-workers 8 \
  --fix-workers 2 \
  --request-interval 0.32 \
  --request-jitter 0.05 \
  --region-batch-size 120 \
  --region-batch-sleep 3 \
  --detail-batch-size 80 \
  --detail-batch-sleep 2
```

默认品牌顺序：

1. `더로랑`
2. `라플라`
3. `라벨르블랑`
4. `헤이에스`
5. `리즈`

### 6. 后台运行

```bash
mkdir -p logs

nohup python3 -u run_multi_brand_pipeline.py \
  --slug-file output/daangn_region_slug_list.txt \
  --out-root output/brand_runs \
  --sleep-between-brands 35 \
  --region-workers 8 \
  --detail-workers 5 \
  --final-workers 8 \
  --fix-workers 2 \
  --request-interval 0.32 \
  --request-jitter 0.05 \
  --region-batch-size 120 \
  --region-batch-sleep 3 \
  --detail-batch-size 80 \
  --detail-batch-sleep 2 \
  > logs/karrot_batch_$(date +%Y%m%d_%H%M%S).log 2>&1 &
```

## 输出说明

每个品牌会生成一个目录，例如：

- `output/brand_runs/헤이에스_20260314_025205`

典型文件结构：

```txt
output/brand_runs/헤이에스_20260314_025205/
  raw/
    candidates.json
    details.json
    karrot_multi_region_ongoing_cn_local.json
    karrot_multi_region_ongoing_cn.html
    translation_cache_ko_zh.json
    images/
  karrot_헤이에스_ongoing_cn_fixed.html
  karrot_헤이에스_ongoing_cn_local_fixed.json
  karrot_헤이에스_ongoing_cn_by_time.html
  karrot_헤이에스_ongoing_cn_by_time.json
  translation_cache.json
```

含义：

- `raw/candidates.json`
  - 原始候选结果，保留抓到的顺序，不去重

- `raw/details.json`
  - 原始详情页数据

- `raw/karrot_multi_region_ongoing_cn_local.json`
  - 抓取完成后的原始聚合结果

- `karrot_品牌_ongoing_cn_fixed.html`
  - 修复后的中文 HTML

- `karrot_品牌_ongoing_cn_by_time.html`
  - 最终品牌 HTML
  - 文件名保留了历史命名，但当前版本不再按时间排序

- `translation_cache.json`
  - 翻译缓存

多品牌批量运行还会生成两个汇总文件：

- `output/brand_runs/brand_batch_YYYYMMDD_HHMMSS.json`
- `output/brand_runs/brand_batch_YYYYMMDD_HHMMSS.html`

## 当前版本和旧版本的差异

当前版本为了更快地产出可读 HTML，已经做了这几个改变：

- 不再去重
- 不再按时间排序
- 批量任务改成“抓取/后处理流水线”
- 翻译阶段会继续尝试把英文或韩文清理成中文

注意：

- 最终页面里的结果顺序更接近抓取顺序，而不是发布时间
- 同一商品如果在多个区域搜索命中，可能会重复出现

这是有意的速度优先策略。

## 日志怎么看

这套脚本没有单独的日志框架，主要依赖标准输出。

常见进度日志：

```txt
[INFO] regions to scan: 4865
[INFO] search terms per region: 2
[INFO] region scan progress 100/9730, ongoing candidates=38
[INFO] detail progress 100/1428
[INFO] finalize progress 100/1428
[INFO] fix progress 50/1428
[DONE] ongoing items: 1428
[DONE] json: output/...
[DONE] html: output/...
```

查看实时日志：

```bash
tail -f logs/karrot_batch_*.log
```

只筛错误：

```bash
rg -n "Traceback|WARN|failed|SIGTERM|Broken pipe|RuntimeError|fetch failed|json fetch failed" logs/karrot_batch_*.log
```

查看当前在跑什么：

```bash
ps -ef | rg 'run_multi_brand_pipeline.py|run_brand_pipeline.py|multi_region_karrot_report.py|fix_multi_region_output.py' | rg -v rg
```

## 如何判断进展

### 1. 看正在运行的进程

如果看到：

- `multi_region_karrot_report.py`
  - 说明品牌还在抓取阶段

- `fix_multi_region_output.py`
  - 说明品牌已抓取完，正在构建中文 HTML / 修复阶段

- 只有 `run_brand_pipeline.py`
  - 说明可能在做最后收尾

### 2. 看品牌目录里有哪些文件

如果只有 `raw/`：

- 说明还没完成后处理

如果已经出现：

- `karrot_品牌_ongoing_cn_fixed.html`
- `karrot_品牌_ongoing_cn_local_fixed.json`

说明翻译修复已经完成。

如果已经出现：

- `karrot_品牌_ongoing_cn_by_time.html`
- `karrot_品牌_ongoing_cn_by_time.json`

说明品牌已完成。

### 3. 看批量汇总文件

汇总 JSON 里常见状态：

- `scraped`
  - 已抓取完成，后处理中

- `done`
  - 该品牌已全部完成

- `failed`
  - 该品牌失败

## 常见失败与排查

### `SIGTERM`

表示进程被外部终止，常见原因：

- 节点被调度器杀掉
- 人工停止
- 运行环境回收

### `Broken pipe`

一般是：

- 父进程退出
- 日志管道中断

### `fetch failed` / `json fetch failed`

一般是：

- 网络波动
- 目标站点临时限流
- DNS 或出口网络问题

### 翻译很慢

这通常发生在：

- `fix_multi_region_output.py`

因为这里会做标题、描述、区域的中文清洗。当前已经做了流水线优化，但翻译本身仍然受外部翻译接口速度影响。

## OpenClaw 节点部署建议

建议节点上的目录结构固定：

```txt
/opt/openclaw/karrot/
  run_pipeline.sh
  README.md
  run_multi_brand_pipeline.py
  run_brand_pipeline.py
  multi_region_karrot_report.py
  fix_multi_region_output.py
  output/
  logs/
```

建议使用固定启动方式：

```bash
cd /opt/openclaw/karrot
mkdir -p output logs

./run_pipeline.sh batch-bg
```

上层调度系统建议只认两个最终成功信号：

1. 对应品牌目录存在：
   - `karrot_品牌_ongoing_cn_by_time.html`
   - `karrot_品牌_ongoing_cn_by_time.json`

2. 批量汇总 JSON 里该品牌状态为：
   - `done`

## 重要说明

当前版本是速度优先，不是严格去重优先、严格发布时间优先。

如果后面要改回“按时间排序”或“品牌内去重”，可以再恢复，但会明显拖慢 HTML 可读结果的产出速度。

## OpenClaw 任务模板

下面这些模板可以直接改路径后交给 OpenClaw 节点执行。

### 模板 1：批量流水线任务

任务名：

```txt
karrot-batch-pipeline
```

工作目录：

```txt
/opt/openclaw/karrot
```

执行命令：

```bash
SLUG_FILE=/opt/openclaw/karrot/output/daangn_region_slug_list.txt \
OUT_ROOT=/opt/openclaw/karrot/output/brand_runs \
LOG_DIR=/opt/openclaw/karrot/logs \
REGION_WORKERS=8 \
DETAIL_WORKERS=5 \
FINAL_WORKERS=8 \
FIX_WORKERS=2 \
REQUEST_INTERVAL=0.32 \
REQUEST_JITTER=0.05 \
REGION_BATCH_SIZE=120 \
REGION_BATCH_SLEEP=3 \
DETAIL_BATCH_SIZE=80 \
DETAIL_BATCH_SLEEP=2 \
SLEEP_BETWEEN_BRANDS=35 \
./run_pipeline.sh batch
```

### 模板 2：批量流水线后台任务

任务名：

```txt
karrot-batch-pipeline-bg
```

工作目录：

```txt
/opt/openclaw/karrot
```

执行命令：

```bash
SLUG_FILE=/opt/openclaw/karrot/output/daangn_region_slug_list.txt \
OUT_ROOT=/opt/openclaw/karrot/output/brand_runs \
LOG_DIR=/opt/openclaw/karrot/logs \
./run_pipeline.sh batch-bg
```

### 模板 3：单品牌完整任务

任务名：

```txt
karrot-brand-heyes
```

工作目录：

```txt
/opt/openclaw/karrot
```

执行命令：

```bash
SLUG_FILE=/opt/openclaw/karrot/output/daangn_region_slug_list.txt \
OUT_ROOT=/opt/openclaw/karrot/output/brand_runs \
./run_pipeline.sh brand 헤이에스 heys
```

### 模板 4：单品牌只抓取

任务名：

```txt
karrot-brand-scrape-heyes
```

工作目录：

```txt
/opt/openclaw/karrot
```

执行命令：

```bash
SLUG_FILE=/opt/openclaw/karrot/output/daangn_region_slug_list.txt \
OUT_ROOT=/opt/openclaw/karrot/output/brand_runs \
./run_pipeline.sh scrape 헤이에스 heys
```

### 模板 5：已有目录重建 HTML / 中文输出

任务名：

```txt
karrot-brand-postprocess-heyes
```

工作目录：

```txt
/opt/openclaw/karrot
```

执行命令：

```bash
SLUG_FILE=/opt/openclaw/karrot/output/daangn_region_slug_list.txt \
OUT_ROOT=/opt/openclaw/karrot/output/brand_runs \
./run_pipeline.sh postprocess 헤이에스 /opt/openclaw/karrot/output/brand_runs/헤이에스_20260315_120000 heys
```

### 模板 6：查看日志

任务名：

```txt
karrot-tail-log
```

工作目录：

```txt
/opt/openclaw/karrot
```

执行命令：

```bash
tail -f /opt/openclaw/karrot/logs/karrot_batch_*.log
```

### 模板 7：查看当前运行进程

任务名：

```txt
karrot-ps
```

工作目录：

```txt
/opt/openclaw/karrot
```

执行命令：

```bash
ps -ef | rg 'run_multi_brand_pipeline.py|run_brand_pipeline.py|multi_region_karrot_report.py|fix_multi_region_output.py' | rg -v rg
```
