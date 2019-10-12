
[Quantopian风险模型白皮书(原文翻译)](https://www.quantopian.com/papers/risk)

[使用nbviewer浏览](https://nbviewer.jupyter.org/github/liudengfeng/zipline/blob/master/docs/notebooks/%E9%A3%8E%E9%99%A9%E6%A8%A1%E5%9E%8B%E7%99%BD%E7%9A%AE%E4%B9%A6.ipynb)

# 修订记录

## 行业因子

### 使用行业内股票简单加权收益替代ETF

### Sector
+ 国证 Sector
+ 申万 SWSector

首先计算每个行业因子的收益，然后估计各股票对行业的因子风险敞口。

# 方法
如前所述，QRM由两个子模型组成。 子模型（1a）使用线性回归估算所有股票的行业因子敞口，并传递剩余收益$\epsilon_{i,t}^{sect}$给子模型（1b）。然后子模型(1b)用$\epsilon_{i,t}^{sect}$估计与主题因子风险敞口相关的主题因子回报。

## 计算行业因子

### Pipeline实现
使用过去两年的股票收益率及其各自的行业收益率来估算每种资产的行业收益率敞口。
单个行业的实现：
1. 窗口长度为$L = PPY * 2, PPY=244$
2. 行业历史收益率向量列$f$
3. 行业内股票$1...k$在时期$1...L$收益率矩阵$R$
4. 使用`vectorized_beta`函数，其中$f$为自变量，$R$为因变量
5. 获得回归系数$\beta$，并将其设置为相应的行业因子敞口
6. 将其他行业因子敞口设为0
7. $r_i - \beta f = \epsilon_i^{sect}$计算尾部2年的历史收益率残余项


- [x] 未分类行业采用补充股票方式？

单个股票案例如下。如`结束日期`为`2013-01-02`，股票为`美的集团`：
+ `开始日期`为`结束日期`前移`2 * PPY`
+ 向量列$f$将为`美的集团`所处行业自`开始日期`到`结束日期`所有股票日收益率简单平均值
+ 向量列$r_i$为`美的集团`自`开始日期`到`结束日期`的日收益率
+ $\beta$为`美的集团`的行业因子敞口
+ 向量列$\epsilon_i^{sect} = r_i - \beta f$`开始日期`到`结束日期`的收益率残余项

如果我们将此示例插入子模型（1a），则可以将其写为：
$$r_{i,t} = \sum_{j=1}^n\beta_{i,j,t}^{sect}f_{j,t}^{sect} + \beta f_t + \epsilon_{i,t}^{sect}$$
此处
- $f_t$是$f$的最后一项
- $\epsilon_{i,t}^{sect}$是$\epsilon_i^{sect}$的最后一项
- 项$\sum_{j=1}^n\beta_{i,j,t}^{sect}f_{j,t}^{sect}$等于0

## 计算主题因子

要估算主题因子的收益，不宜使用市场上的所有股票。 我们需要定义一个总体，即估计总体，它可以代表市场，同时排除诸如REIT，ADR，流动性不佳的股票等“问题”资产。在估计总体中选择股票是主观的。 QRM中的估计范围大约有2100个股票。 选择标准包括：

+ 为普通股
+ 有足够的数据来计算主题因子指标
+ 在流动性最高的股票前3000名中

估计范围之外的股票称为补充股票。既包含估算总体的股票又包含补充股票的总体被称为覆盖总体。我们将演示如何计算估计总体内的股票的主题因子敞口，如何估算主题因子收益以及如何计算互补股票的风格因子敞口。

### 估计总体中的股票主题因子敞口

第t天股票的主题因子指标进行z评分，可以计算出第t天股票在估计总体中的主题因子敞口。 它们相对于估计总体是标准化的（z评分）。

### 估计主题因子收益

使用横截面回归两年内逐日的数据，估算样式因子回报。

在尾部2年窗口内，对于每天$t$：
1. 计算估计总体中股票的5种主题因子敞口，并将它们存储在矩阵$B$的列中
2. 收集估计总体中第t天的股票行业残差，并将它们形成列向量$\epsilon_t^{sect}$
3. 在矩阵$B$基础上回归$\epsilon_t^{sect}$
4. 从回归系数$f_{1,t}^{style}, f_{2,t}^{style}, ..., f_{5,t}^{style}$取得主题因子收益
5. 生成5个向量列$f_k^{style} (k = 1, 2, ... 5)$，存储$f_{k,t}^{style}$到$f_k^{style}$中
6. 收集$t$日估计总体中股票残差，$\epsilon_t^{sect} = \sum_{k=1}^5B_{:k} f_{k,t}^{style}$

图1显示矩阵中$\epsilon_t^{sect}$、$\epsilon_i^{sect}$和$\epsilon_{i,t}^{sect}$之间的关系

<img src='images/e.png'></img>

图2显示$f_k^{style}$和$f_{kt}^{style}$之间的关系

<img src='images/f.png'></img>

### 补充股票的主题因子敞口
补充股票的主题因子敞口通过求解5种主题因子收益与行业残差的时间序列多线性回归来计算。 在这里，我们使用主题因子收益率和行业残差的收益率序列的两年期窗口。 步骤如下：
对于$t$日的每个补充股票$i$：
1. 收集主题因子收益$f_k^{style}, k=1,2,...,5$
2. 收集尾部2年历史行业残差收益$\epsilon_i^{sect}$
3. 使用因变量$\epsilon_i^{sect}$和自变量$f_k^{style}, k=1,2,...,5$运行多元线性回归
4. 取得回归系数$(\beta_{k,t}^{style},k=1,2,...,5)$，设定为相应的主题因子敞口
5. 计算2年期尾部历史收益残差，$\epsilon_i = \epsilon_i^{sect} - \sum_{k=1}^5 \beta_{i,k,t}^{style} f_{k}^{style}$

## 计算风险

在整个$T$周期内资产$i$的风险定义如下：
$$\sigma = {\sqrt{{1 \over T} {\sum_{l=1}^r} (r_l - \bar{r}_i)^2 }}\tag2$$
此处
- $r_{i,l}$为资产$i$在时点$l$的收益
- $\bar{r}_i$资产$i$在整个$T$周期内的平均收益

每个因子收益的风险可以直接通过公式（2）计算。例如$k^{th}$在整个$T$周期内的主题因子为
$${\sqrt{{1 \over T} {\sum_{l=1}^T} (f_{k,l}^{style} - \bar{f}_k^{style})^2 }}$$

同样，也可以通过公式（2）计算每个敞口加权因子收益的风险。 例如，在T个时间段内，第k个敞口加权主题因子的风险为：
$${\sqrt{{1 \over T} {\sum_{l=1}^T} (\beta_{i,k,t}^{style} f_{k,l}^{style} - \overline{\beta_k^{style} f_k^{style}}    )^2 }}$$

# 参考文献

Axioma, Inc. 2011. Axioma Robust Risk Model Handbook. Axioma, Inc.

BARRA, Inc. 1998. United States Equity. BARRA, Inc.

Fama, Eugene F, and Kenneth R French. 1993. "Common risk factors in the returns on stocks and bonds." Journal of Financial Economics 3-56.

Morningstar, Inc. n.d. Morningstar® Data for Equities.

Ross, Stephen A. 1976. "The Arbitrage Theory of Capital Asset Pricing." Journal of Economic Theory 341-360.

Sharpe, William F. 1964. "Capital Asset Prices: A Theory of Market Equilibrium under Conditions of Risk." The Journal of Finance 425-442.

# 附录
## 投资组合换手率假设
用于QRM的数据的周期性是每天的，这导致了以下基本假设：每种资产的最短持有期至少为1天。日内交易量很大或周转率很高的投资策略不适用于当前QRM。

## 工具覆盖
QRM涵盖了美国股票市场中约4000支股票，但是它并不包含所有资产。如果投资组合具有相当大的权重投资于覆盖范围之外的资产，则不适合使用QRM对其进行分析。

当前的QRM除行业ETF和一些预选ETF（用于测试）外，不涵盖ETF。

## 计算摘要
<table class="quanto-table-v2 summary-table">
<thead>
<tr>
<td>Factor type</td>
<td>Stock type</td>
<td>Factor exposures</td>
<td>Factor returns</td>
</tr>
</thead>
<tbody>
<tr>
<td>Sector</td>
<td>Stocks in coverage universe</td>
<td>Time-series linear regression</td>
<td>Given sector ETFs</td>
</tr>
<tr>
<td rowspan="2">Style</td>
<td>Stocks in the estimation universe</td>
<td>Normalized risk metrics</td>
<td>Cross-sectional regression</td>
</tr>
<tr>
<td class="special">Complementary stocks</td>
<td>Time-series linear regression</td>
<td>Time-series linear regression</td>
</tr>
</tbody>
</table>

# 下载PDF文件

英文白皮书可[在此下载](https://media.quantopian.com/quantopian_risk_model_whitepaper.pdf)。
