# U.S. Consumer Dashboard Strategy

## What Is Already in This Folder

The existing files already establish a strong first-principles framework:

- `Consumer reports general information.docx` defines the consumer as a system with five layers:
  income, spending, inflation, financial cushion/stress, and psychology.
- `Consumer data master tracking table.xlsx` converts that framework into a release tracker.
- `US_Consumer_Dashboard.xlsx` converts the tracker into a dashboard structure with monthly, weekly, quarterly, and memo tabs.

In other words, the conceptual work is mostly done. The next step is operationalizing the data pipeline.

## First-Principles Dashboard Model

To understand the U.S. consumer, the dashboard should answer these questions every week and month:

1. Is household income still growing?
2. Is spending still expanding in real terms?
3. Is inflation eroding purchasing power?
4. Are households leaning on savings or credit to keep spending?
5. Is the labor market supporting the consumer?
6. Is sentiment aligned with the hard data?
7. Is wealth or housing amplifying or weakening the cycle?

That leads to seven dashboard modules:

- Income engine
- Spending engine
- Inflation constraint
- Labor support
- Financial cushion
- Household stress
- Psychology and expectations

## Required Reports

### Tier 1: Core reports you should always track

These are the minimum viable consumer dashboard.

| Report | Why it matters | Frequency | Recommended status |
| --- | --- | --- | --- |
| Personal Income & Outlays (BEA) | Best single report for income, spending, savings, PCE inflation | Monthly | Required |
| Retail Sales (Census) | High-frequency goods spending signal | Monthly | Required |
| Consumer Price Index (BLS) | Inflation faced by consumers | Monthly | Required |
| Employment Situation / Jobs Report (BLS) | Income engine: payrolls, unemployment, wages | Monthly | Required |
| Initial Jobless Claims (DOL) | Most timely labor stress signal | Weekly | Required |
| Consumer Credit G.19 (Fed) | Whether credit is funding consumption | Monthly | Required |
| Household Debt & Credit (NY Fed) | Credit cards, mortgages, auto, delinquencies | Quarterly | Required |
| Financial Accounts Z.1 (Fed) | Household net worth and liabilities | Quarterly | Required |

### Tier 2: Important complementary reports

These sharpen interpretation and help avoid false conclusions.

| Report | Why it matters | Frequency | Recommended status |
| --- | --- | --- | --- |
| University of Michigan Sentiment | Sentiment plus inflation expectations | Monthly | Required if allowed/licensed |
| Conference Board Consumer Confidence | Labor-market-oriented confidence read | Monthly | Nice to have; partially proprietary |
| JOLTS (BLS) | Labor demand and quits confidence | Monthly | Recommended |
| Existing Home Sales (NAR) | Housing turnover and confidence | Monthly | Recommended, but not government |
| New Home Sales (Census) | Forward-looking housing demand | Monthly | Recommended |
| Housing Starts & Building Permits (Census) | Housing cycle and future supply | Monthly | Recommended |
| Distributional Financial Accounts (Fed) | Which households hold the wealth | Quarterly | Recommended |

### Tier 3: Derived metrics the dashboard should compute

These matter as much as the raw reports.

| Derived metric | Build from |
| --- | --- |
| Real disposable personal income | BEA income data + inflation adjustment |
| Real personal spending | BEA PCE + price index |
| Savings rate trend | BEA |
| Real wage growth | BLS wages vs CPI or PCE inflation |
| Credit-fueled spending risk | G.19 + BEA income + savings rate |
| Consumer regime classification | All major modules combined |

## Gaps in the Current Dashboard Workbook

The workbook structure is good, but a few additions would improve it:

- Add `Disposable Personal Income` explicitly, not just personal income.
- Add `Real Disposable Personal Income` explicitly, not only in the notes.
- Split inflation into `Core CPI`, `Shelter`, and `Core Services ex Housing` if you want a stronger inflation dashboard.
- Add a `Debt stress` section for delinquencies, not just balances.
- Add a `distribution layer` so you can distinguish average consumer health from lower-income stress.
- Add a `source_status` or `last_updated` field for each series so the dashboard can display freshness.

## How To Acquire The Data

### Best case: official machine-readable source

Use APIs or downloadable CSV/XLSX files from the publishing agency whenever possible.

### Second-best: official page scrape

Use only when the report is public but not exposed cleanly through an API or stable file.

### Third-best: licensed/manual ingestion

Use when the data is copyrighted, subscriber-controlled, or operationally fragile to scrape.

## Acquisition Recommendations By Source

### Strong automation candidates

These are the best places to start because they have official APIs or structured downloads.

| Report group | Source | Acquisition path | Confidence |
| --- | --- | --- | --- |
| Personal Income & Outlays / PCE | BEA | BEA API | High |
| CPI / Jobs / JOLTS | BLS | BLS Public Data API | High |
| Retail Sales / New Home Sales / Housing Starts | Census | Census Economic Indicators API | High |
| Consumer Credit G.19 | Federal Reserve Board | Data Download Program CSV | High |
| Financial Accounts Z.1 | Federal Reserve Board | Release CSV and DDP downloads | High |
| Distributional Financial Accounts | Federal Reserve Board | Raw CSV ZIP download | High |
| Household Debt & Credit | New York Fed | CMD Data Bank downloadable data | High |

### Moderate automation candidates

These can probably be automated, but they deserve careful handling.

| Report group | Source | Acquisition path | Risk |
| --- | --- | --- | --- |
| Initial Jobless Claims | DOL | Parse official weekly release page or official attached tables | Medium |
| University of Michigan Sentiment | University of Michigan | Public site and delayed public data pages; confirm usage limits | Medium |

### Weak automation candidates / likely manual or licensed

| Report group | Source | Acquisition path | Risk |
| --- | --- | --- | --- |
| Conference Board Consumer Confidence | Conference Board | Likely press-release extraction or licensed access | High |
| Existing Home Sales | NAR | Likely manual ingestion or licensed extraction | High |
| Private card-spend / Redbook / bank data | Private providers | Subscription or manual upload | High |

## Practical Recommendation

Build the dashboard in two phases.

### Phase 1: Fully automatable official-data dashboard

Start with only the reports that are easy to automate from official government or quasi-official sources:

- BEA income and spending
- BLS CPI, payrolls, unemployment, wages, JOLTS
- Census retail sales, new home sales, housing starts, permits
- DOL initial claims
- Federal Reserve G.19 and Z.1
- New York Fed household debt and delinquency data
- Fed DFA wealth distribution data

This already gives you a very strong consumer dashboard.

### Phase 2: Add semi-manual or licensed overlay

Layer in the harder reports later:

- Michigan sentiment
- Conference Board confidence
- Existing home sales
- Private card-spending and high-frequency consumer data

## Recommended Pipeline Architecture

The cleanest operating model is:

1. A scheduled agent checks release calendars or fetches data from known endpoints.
2. Raw files are saved into dated folders by source.
3. A normalization step maps each source into a standard schema.
4. Derived metrics are computed.
5. The dashboard reads from the normalized tables, not from raw files.

Suggested storage layout:

```text
data/
  raw/
    bea/
    bls/
    census/
    fed/
    nyfed/
    dol/
  processed/
    monthly_consumer_series.csv
    weekly_consumer_series.csv
    quarterly_consumer_series.csv
  manifests/
    consumer_reports_manifest.csv
```

## What An AI Agent Could Do

An agent can absolutely handle a large part of this workflow.

### Good tasks for an agent

- Poll official release pages or APIs on a schedule
- Download new CSV/XLSX/PDF files
- Extract headline values
- Update normalized datasets
- Write a short monthly summary memo
- Flag missing or delayed releases

### Tasks that still may need manual review

- Copyright-sensitive sentiment data
- Fragile scrapes from pages that change layout
- Any source where terms of use are unclear
- Interpreting revisions or methodology changes

## Suggested Automation Rules

For reliability, use three rules:

1. Prefer official APIs over page scraping.
2. Save the original raw artifact every time.
3. Never overwrite a prior release without versioning it by release date.

## Immediate Next Step I Recommend

If we continue from here, the best next move is to build a source manifest and a lightweight downloader around the fully automatable sources first. That gives you a working dashboard backbone before we deal with the harder sentiment and housing overlays.

## Verified Source Pages

These are the main official pages I verified while preparing this plan:

- BEA API: https://apps.bea.gov/api/signup/
- BEA Personal Income data: https://www.bea.gov/data/income-saving/personal-income
- BLS API overview: https://www.bls.gov/bls/api_features.htm
- BLS API v2 signatures: https://www.bls.gov/developers/api_signature_v2.htm
- Census Economic Indicators API: https://www.census.gov/data/developers/data-sets/economic-indicators.html
- Census Economic Indicator release calendar: https://www.census.gov/economic-indicators/calendar-listview.html
- Census Retail Sales: https://www.census.gov/retail/sales.html
- Census New Residential Sales: https://www.census.gov/construction/nrs/
- Fed Data Download Program: https://www.federalreserve.gov/datadownload/
- Fed G.19 consumer credit: https://www.federalreserve.gov/releases/g19/about.htm
- Fed Z.1 current release: https://www.federalreserve.gov/releases/z1/
- Fed Distributional Financial Accounts: https://www.federalreserve.gov/releases/efa/efa-distributional-financial-accounts.htm
- New York Fed CMD main page: https://www.newyorkfed.org/microeconomics
- New York Fed Data Bank: https://www.newyorkfed.org/microeconomics/databank.html
- DOL weekly claims report: https://www.dol.gov/node/72199
- University of Michigan main site: https://www.sca.isr.umich.edu/
- University of Michigan FAQ: https://data.sca.isr.umich.edu/faq.php
- University of Michigan usage agreement: https://data.sca.isr.umich.edu/agreement.php
- Conference Board confidence page: https://www.conference-board.org/topics/consumer-confidence/index.cfm

