# 测试及修订记录

- `zipline\_protocol.pyx`
  - 多asset、多字段时，使用MultiIndex DataFrame

## 代码

- `zipline\gens\sim_engine.pyx`
  - pd.to_datetime 弃用`box`参数
- `zipline\gens\sim_engine.pyx`
  - `MinuteSimulationClock`考虑午休时间
  - 增加测试`tests\data\test_minute_bar_internal.py`

## 测试

在目标环境下运行
```python
python -m pytest -vv <test_file.py>
```

- `tests\test_clock.py`
