"""Build investor-friendly dashboard datasets from processed outputs."""

from __future__ import annotations

from datetime import datetime
import math

from consumer_dashboard.metrics.regime import classify_regime
from consumer_dashboard.models.observation import Observation
from consumer_dashboard.storage.filesystem import ensure_project_directories, read_json, write_json

CARD_SPECS = {
    "unemployment_rate": {
        "title": "Unemployment",
        "why": "A clean read on labor slack and household income risk.",
    },
    "cpi_headline_yoy_pct": {
        "title": "Headline CPI YoY",
        "why": "Tells us how much broad prices are squeezing households right now.",
    },
    "real_wage_growth": {
        "title": "Real Wage Growth",
        "why": "Shows whether wage gains are actually beating inflation.",
    },
    "real_personal_spending_yoy_pct": {
        "title": "Real Spending YoY",
        "why": "The cleanest top-level test of whether the consumer is still spending in real terms.",
    },
    "initial_jobless_claims_4_week_average": {
        "title": "Claims 4-Week Avg",
        "why": "The fastest official signal that labor conditions may be softening.",
    },
    "household_debt_90_plus_delinquent_rate": {
        "title": "90+ Day Delinquency",
        "why": "Captures stress building underneath the consumer's balance sheet.",
    },
    # Phase 4: inflation detail
    "cpi_shelter_yoy_pct": {
        "title": "Shelter CPI YoY",
        "why": "Shelter is the largest CPI component and lags market rents by 12-18 months. Tracking it separately tells you how much of headline inflation is structural vs. fading.",
    },
    "cpi_owners_equivalent_rent_yoy_pct": {
        "title": "Owners' Equiv. Rent YoY",
        "why": "OER is the single largest CPI subcomponent (~25% weight). Its slow mean-reversion after the 2022 rental spike is the main reason core CPI has stayed elevated.",
    },
    "cpi_services_ex_energy_yoy_pct": {
        "title": "Services Ex-Energy YoY",
        "why": "Services inflation ex-energy (the Fed's 'supercore' proxy) reflects sticky labor costs. If this stays elevated after shelter cools, the inflation problem is not going away.",
    },
    "cpi_shelter_vs_services_spread": {
        "title": "Shelter vs Services Spread",
        "why": "When positive and falling, shelter is mean-reverting faster than services — the benign scenario. When services overtake shelter, the inflation problem has shifted to a harder-to-fix source.",
    },
    # Phase 2: credit stress
    "consumer_credit_revolving_yoy_pct": {
        "title": "Revolving Credit Growth YoY",
        "why": "Revolving credit (mainly credit cards) growing faster than income is a late-cycle warning — households are borrowing to sustain spending, not funding it from earnings.",
    },
    "consumer_credit_total_yoy_pct": {
        "title": "Total Consumer Credit YoY",
        "why": "Total credit growth rate contextualizes whether households are accumulating debt faster or slower than historical norms.",
    },
    "household_credit_card_balance_yoy_pct": {
        "title": "Credit Card Balance Growth YoY",
        "why": "Credit card balance growth above income growth signals stress accumulation — households are not paying down balances at the same pace they are spending.",
    },
    "household_credit_card_90_plus_delinquent_rate": {
        "title": "Card 90+ Delinquency",
        "why": "Credit card delinquency is one of the fastest-moving stress signals. Card delinquency tends to rise before mortgage or auto delinquency because it is unsecured and lower-income-exposed.",
    },
    "household_auto_loan_90_plus_delinquent_rate": {
        "title": "Auto Loan 90+ Delinquency",
        "why": "Auto loan delinquency, particularly in subprime, is an early warning for broader consumer stress. It tends to respond before the headline labor market weakens.",
    },
    # Phase 3: savings depth
    "savings_rate_3m_avg": {
        "title": "Savings Rate (3M Avg)",
        "why": "Smooths the noisy monthly savings print to give a cleaner read on whether households are building or eroding their financial buffer.",
    },
    "savings_rate_yoy_chg": {
        "title": "Savings Rate Change YoY",
        "why": "An absolute percentage-point change captures whether the savings cushion is growing or shrinking relative to a year ago — a direction signal the level alone cannot provide.",
    },
    "excess_savings_cumulative_proxy": {
        "title": "Excess Savings Proxy ($B)",
        "why": "Cumulative savings above or below pre-pandemic trend. When this turns deeply negative, the pandemic buffer is gone and spending must be funded from current income or new debt.",
    },
    # Phase 5: sentiment
    "michigan_sentiment_index": {
        "title": "Michigan Sentiment",
        "why": "Consumer sentiment leads spending by one to two quarters. A sustained decline in sentiment often precedes a real slowdown in consumption before it appears in hard data.",
    },
    "michigan_inflation_expectations_1y": {
        "title": "1Y Inflation Expectations",
        "why": "Short-term inflation expectations drive immediate spending behavior — if households expect prices to keep rising, they may pull forward purchases or demand higher wages.",
    },
    "michigan_inflation_expectations_5y": {
        "title": "5Y Inflation Expectations",
        "why": "The Fed watches 5-year expectations closely as a measure of credibility. If long-run expectations become unanchored above 3.5%, the Fed faces a harder policy path.",
    },
    # Phase 7: DFA distributional
    "dfa_net_worth_top1pct": {
        "title": "Top 1% Net Worth ($M)",
        "why": "Provides the numerator for the concentration ratio. The top 1% own roughly 30% of all household wealth, so their balance sheet dynamics heavily influence aggregate wealth and wealth-effect spending.",
    },
    "dfa_net_worth_bottom50pct": {
        "title": "Bottom 50% Net Worth ($M)",
        "why": "The bottom 50% have almost no financial buffer. When their net worth declines, they cut spending immediately. This is the most stress-sensitive segment of the household sector.",
    },
    "dfa_wealth_concentration_ratio": {
        "title": "Wealth Concentration Ratio",
        "why": "Top-1% net worth divided by bottom-50% net worth. A rising ratio means the aggregate picture is increasingly driven by a small group, making headline consumer data less representative of most households.",
    },
    "dfa_bottom50_net_worth_yoy_pct": {
        "title": "Bottom 50% Net Worth YoY",
        "why": "The most sensitive indicator of whether the median household is gaining or losing financial resilience. Falling bottom-50% net worth is a leading indicator of broad consumer stress even when aggregate numbers look healthy.",
    },
    # Phase 6: housing
    "shelter_affordability_squeeze": {
        "title": "Shelter Affordability Squeeze",
        "why": "When shelter costs grow faster than real disposable income, housing is a headwind to household cash flow. This spread is the clearest proxy for housing affordability pressure.",
    },
    "home_equity_extraction_proxy": {
        "title": "Home Equity Extraction Proxy",
        "why": "Rising home values relative to flat liabilities = equity accumulation. When home values plateau and liabilities rise, households may be extracting equity to fund spending — a late-cycle pattern.",
    },
    "housing_starts_to_permits_ratio": {
        "title": "Starts / Permits Ratio",
        "why": "Ratio below 0.85 means permitted supply is still in the pipeline — more homes coming. Ratio above 1.05 is anomalous and can signal cancellations or financing stress.",
    },
}

SECTION_SPECS = [
    {
        "id": "fast-read",
        "title": "Fast Read",
        "label": "Fast Read",
        "intro": "Start here when you want the shortest possible investor-grade read on the consumer.",
        "card_series_ids": [
            "unemployment_rate",
            "cpi_headline_yoy_pct",
            "real_wage_growth",
            "real_personal_spending_yoy_pct",
            "initial_jobless_claims_4_week_average",
            "household_debt_90_plus_delinquent_rate",
        ],
        "chart_series_ids": [
            "unemployment_rate",
            "cpi_headline_yoy_pct",
            "real_wage_growth",
            "real_personal_spending_yoy_pct",
        ],
        "chart_title": "Fast Read Trendboard",
        "chart_note": "Rebased to 100 at the start of the visible window so unlike series can be compared directionally.",
        "report_ids": ["jobs_report", "inflation_metrics", "real_spending_metrics", "household_debt_credit"],
    },
    {
        "id": "labor",
        "title": "Can Consumers Earn and Keep Cash?",
        "label": "Labor",
        "intro": "Household health starts with jobs, hours, wages, cash income, and whether workers feel secure enough to keep spending.",
        "card_series_ids": [
            "unemployment_rate",
            "nonfarm_payrolls",
            "average_hourly_earnings",
            "disposable_personal_income",
            "savings_rate",
            "initial_jobless_claims_4_week_average",
            "jolts_job_openings",
            "jolts_quits_rate",
            # Phase 3: savings depth
            "savings_rate_3m_avg",
            "savings_rate_yoy_chg",
            "excess_savings_cumulative_proxy",
        ],
        "chart_series_ids": [
            "unemployment_rate",
            "real_wage_growth",
            "real_disposable_personal_income_yoy_pct",
            "initial_jobless_claims_4_week_average",
        ],
        "chart_title": "Labor and Cash Flow Trajectory",
        "chart_note": "Directional view across labor tightness, real wages, real income, and claims.",
        "report_ids": ["jobs_report", "jolts", "initial_jobless_claims", "personal_income_outlays", "labor_metrics"],
    },
    {
        "id": "inflation",
        "title": "Are Prices Eating Purchasing Power?",
        "label": "Inflation",
        "intro": "This is the pricing pressure layer. If inflation outruns wages and real income, the consumer eventually slows.",
        "card_series_ids": [
            "cpi_headline_yoy_pct",
            "cpi_core_yoy_pct",
            "pce_price_index_yoy_pct",
            "core_pce_price_index_yoy_pct",
            "cpi_headline_mom_pct",
            "pce_price_index_mom_pct",
            "real_wage_growth",
            "real_disposable_personal_income_yoy_pct",
            # Phase 4: inflation detail
            "cpi_shelter_yoy_pct",
            "cpi_owners_equivalent_rent_yoy_pct",
            "cpi_services_ex_energy_yoy_pct",
            "cpi_shelter_vs_services_spread",
        ],
        "chart_series_ids": [
            "cpi_headline_yoy_pct",
            "cpi_core_yoy_pct",
            "cpi_shelter_yoy_pct",
            "cpi_services_ex_energy_yoy_pct",
        ],
        "chart_title": "Inflation Scoreboard",
        "chart_note": "Shelter drives roughly a third of core CPI. Tracking it alongside services tells you whether the inflation problem is fading (shelter mean-reverts) or shifting to a stickier source (services).",
        "report_ids": ["cpi", "personal_income_outlays", "inflation_metrics", "real_income_metrics", "labor_metrics"],
    },
    {
        "id": "spending",
        "title": "Are Consumers Still Spending?",
        "label": "Spending",
        "intro": "Income and prices matter only because they feed into actual spending behavior. This section tests whether that behavior still holds up.",
        "card_series_ids": [
            "personal_consumption_expenditures",
            "real_personal_spending_yoy_pct",
            "retail_sales",
            "real_disposable_personal_income_yoy_pct",
            "housing_starts",
            "building_permits",
            "new_home_sales",
        ],
        "chart_series_ids": [
            "real_personal_spending_yoy_pct",
            "real_disposable_personal_income_yoy_pct",
            "real_wage_growth",
            "cpi_headline_yoy_pct",
        ],
        "chart_title": "Spending Power Versus Spending Behavior",
        "chart_note": "Real spending should be interpreted alongside real income and inflation, not in isolation.",
        "report_ids": [
            "personal_income_outlays",
            "retail_sales",
            "housing_starts_permits",
            "new_home_sales",
            "real_spending_metrics",
            "real_income_metrics",
        ],
    },
    {
        "id": "stress",
        "title": "Is Stress Building Under the Surface?",
        "label": "Stress",
        "intro": "Stress tends to show up first in claims, credit, and delinquency rather than in the headline narrative.",
        "card_series_ids": [
            "initial_jobless_claims",
            "initial_jobless_claims_4_week_average",
            "consumer_credit_total",
            "consumer_credit_revolving",
            "household_credit_card_balance",
            "household_debt_90_plus_delinquent_rate",
            "new_delinquent_total_rate",
            "new_serious_delinquent_total_rate",
            "household_net_worth",
            "household_total_liabilities",
            # Phase 2: credit growth rates and granular delinquency
            "consumer_credit_revolving_yoy_pct",
            "consumer_credit_total_yoy_pct",
            "household_credit_card_balance_yoy_pct",
            "household_credit_card_90_plus_delinquent_rate",
            "household_auto_loan_90_plus_delinquent_rate",
        ],
        "chart_series_ids": [
            "household_debt_90_plus_delinquent_rate",
            "household_credit_card_90_plus_delinquent_rate",
            "household_auto_loan_90_plus_delinquent_rate",
            "consumer_credit_revolving_yoy_pct",
        ],
        "chart_title": "Stress Signals",
        "chart_note": "Delinquency and credit growth are shown alongside claims so level, velocity, and loan-type breakdowns can be read together.",
        "report_ids": ["initial_jobless_claims", "consumer_credit_g19", "financial_accounts_z1", "household_debt_credit"],
    },
    {
        "id": "distribution",
        "title": "Who Holds the Wealth and Who Is Being Squeezed?",
        "label": "Distribution",
        "intro": (
            "Aggregate consumer data can mask enormous divergence across the wealth spectrum. "
            "The top 20% of earners drive roughly 40% of PCE spending. A stressed bottom 50% "
            "can coexist with healthy headline numbers. This section uses the Federal Reserve's "
            "Distributional Financial Accounts (DFA) to reveal the wealth-distribution dimension "
            "behind the aggregate picture."
        ),
        "card_series_ids": [
            "dfa_net_worth_top1pct",
            "dfa_net_worth_next9pct",
            "dfa_net_worth_next40pct",
            "dfa_net_worth_bottom50pct",
            "dfa_wealth_concentration_ratio",
            "dfa_bottom50_net_worth_yoy_pct",
        ],
        "chart_series_ids": [
            "dfa_wealth_concentration_ratio",
            "dfa_bottom50_net_worth_yoy_pct",
        ],
        "chart_title": "Wealth Distribution Dynamics",
        "chart_note": "Wealth concentration ratio shows how many times larger the top 1% wealth stock is vs the bottom 50%. Bottom-50% net worth YoY reveals whether the median household's financial resilience is growing or shrinking.",
        "report_ids": ["distributional_financial_accounts"],
    },
    {
        "id": "housing",
        "title": "Is Housing Amplifying or Absorbing Pressure?",
        "label": "Housing",
        "intro": (
            "Housing is the largest asset on most household balance sheets. It simultaneously affects "
            "wealth (home price appreciation), cash flow (mortgage payments), confidence (ownership equity), "
            "and broader demand (construction activity). When housing is under pressure, it rarely stays "
            "confined — it tends to amplify stress across labor, credit, and spending."
        ),
        "card_series_ids": [
            "housing_starts",
            "building_permits",
            "new_home_sales",
            "shelter_affordability_squeeze",
            "home_equity_extraction_proxy",
            "housing_starts_to_permits_ratio",
            "cpi_shelter_yoy_pct",
            "cpi_owners_equivalent_rent_yoy_pct",
        ],
        "chart_series_ids": [
            "shelter_affordability_squeeze",
            "cpi_shelter_yoy_pct",
            "home_equity_extraction_proxy",
        ],
        "chart_title": "Housing Pressure and Affordability",
        "chart_note": "Shelter affordability squeeze shows when housing costs outrun income. The equity extraction proxy reveals whether homeowners are drawing down equity — a late-cycle pattern.",
        "report_ids": ["housing_starts_permits", "new_home_sales", "housing_metrics"],
    },
    {
        "id": "psychology",
        "title": "What Are Consumers Feeling and Expecting?",
        "label": "Psychology",
        "intro": (
            "Hard data tells you what happened. Sentiment tells you what happens next. "
            "Consumer confidence and inflation expectations consistently lead actual spending by one to two quarters. "
            "Unanchored 5-year inflation expectations are also a direct policy risk — the Fed watches them as a credibility gauge."
        ),
        "card_series_ids": [
            "michigan_sentiment_index",
            "michigan_inflation_expectations_1y",
            "michigan_inflation_expectations_5y",
        ],
        "chart_series_ids": [
            "michigan_sentiment_index",
            "michigan_inflation_expectations_1y",
            "michigan_inflation_expectations_5y",
        ],
        "chart_title": "Consumer Psychology Dashboard",
        "chart_note": "Sentiment index (left scale, rebased) vs inflation expectations. When the gap between short and long-run expectations widens, it signals anchoring risk.",
        "report_ids": ["michigan_sentiment"],
    },
]

REPORT_SPECS = [
    {
        "id": "jobs_report",
        "title": "Jobs Report",
        "source": "BLS",
        "cadence": "Monthly",
        "section_id": "labor",
        "series_ids": ["unemployment_rate", "nonfarm_payrolls", "average_hourly_earnings"],
        "chart_series_ids": ["unemployment_rate", "nonfarm_payrolls", "average_hourly_earnings"],
        "thesis": "The jobs report tells you whether the labor engine is still generating income and confidence.",
        "compare_with": ["initial_jobless_claims", "jolts", "labor_metrics"],
        "reasoning_tips": [
            "Compare payroll growth with unemployment. A strong payroll number with a rising unemployment rate can still imply a softening labor market.",
            "Use wage growth with CPI and PCE, not by itself. Nominal wages are only useful after inflation.",
        ],
    },
    {
        "id": "cpi",
        "title": "Consumer Price Index",
        "source": "BLS",
        "cadence": "Monthly",
        "section_id": "inflation",
        "series_ids": ["cpi_headline", "cpi_core", "cpi_headline_mom_pct", "cpi_headline_yoy_pct", "cpi_core_mom_pct", "cpi_core_yoy_pct"],
        "chart_series_ids": ["cpi_headline_yoy_pct", "cpi_core_yoy_pct", "cpi_headline_3m_annualized_pct", "cpi_core_3m_annualized_pct"],
        "thesis": "CPI is the fastest broad gauge of how consumer prices are affecting the household budget.",
        "compare_with": ["personal_income_outlays", "inflation_metrics", "labor_metrics"],
        "reasoning_tips": [
            "Headline tells you what households feel. Core tells you whether the inflation problem is becoming more entrenched.",
            "Three-month annualized rates help you see whether inflation is cooling now rather than just versus a year ago.",
        ],
    },
    {
        "id": "jolts",
        "title": "JOLTS",
        "source": "BLS",
        "cadence": "Monthly",
        "section_id": "labor",
        "series_ids": ["jolts_job_openings", "jolts_quits_rate"],
        "chart_series_ids": ["jolts_job_openings", "jolts_quits_rate", "unemployment_rate"],
        "thesis": "JOLTS shows labor demand and worker confidence before those shifts fully hit the payroll data.",
        "compare_with": ["jobs_report", "initial_jobless_claims"],
        "reasoning_tips": [
            "Job openings speak to employer demand. Quits speak to worker confidence. Together they tell you how tight labor really is.",
            "If openings and quits weaken before unemployment rises, that usually means labor is cooling beneath the surface.",
        ],
    },
    {
        "id": "personal_income_outlays",
        "title": "Personal Income and Outlays",
        "source": "BEA",
        "cadence": "Monthly",
        "section_id": "spending",
        "series_ids": [
            "personal_income",
            "disposable_personal_income",
            "personal_consumption_expenditures",
            "personal_saving",
            "savings_rate",
            "pce_price_index",
            "core_pce_price_index",
            "real_disposable_personal_income",
        ],
        "chart_series_ids": ["pce_price_index_yoy_pct", "core_pce_price_index_yoy_pct", "real_disposable_personal_income_yoy_pct", "real_personal_spending_yoy_pct"],
        "thesis": "This is the best integrated report for income, spending, savings, and the Fed's preferred inflation measure.",
        "compare_with": ["cpi", "real_spending_metrics", "real_income_metrics"],
        "reasoning_tips": [
            "Watch disposable income, spending, and savings rate together. If spending stays high while savings falls, the consumer may be borrowing through the gap.",
            "PCE inflation is often cleaner than CPI for macro reasoning, especially when comparing against Fed policy.",
        ],
    },
    {
        "id": "retail_sales",
        "title": "Retail Sales",
        "source": "Census",
        "cadence": "Monthly",
        "section_id": "spending",
        "series_ids": ["retail_sales", "real_retail_sales_proxy"],
        "chart_series_ids": ["retail_sales", "real_retail_sales_proxy", "real_personal_spending"],
        "thesis": "Retail sales give you a quick goods-spending pulse before the full spending picture arrives.",
        "compare_with": ["personal_income_outlays", "inflation_metrics"],
        "reasoning_tips": [
            "Retail sales are nominal, so compare them against inflation before concluding demand is strong.",
            "Use retail as a quick check on goods demand, not as a full measure of consumer spending.",
        ],
    },
    {
        "id": "housing_starts_permits",
        "title": "Housing Starts and Permits",
        "source": "Census",
        "cadence": "Monthly",
        "section_id": "spending",
        "series_ids": ["housing_starts", "building_permits"],
        "chart_series_ids": ["housing_starts", "building_permits", "new_home_sales"],
        "thesis": "Housing is a sensitive rate-driven sector and often gives an early read on cyclical consumer confidence.",
        "compare_with": ["new_home_sales", "jobs_report"],
        "reasoning_tips": [
            "Permits lead starts. If permits weaken first, the housing signal may be rolling over before it shows up in construction activity.",
            "Housing matters because it drives durable demand, confidence, and local labor activity.",
        ],
    },
    {
        "id": "new_home_sales",
        "title": "New Home Sales",
        "source": "Census",
        "cadence": "Monthly",
        "section_id": "spending",
        "series_ids": ["new_home_sales"],
        "chart_series_ids": ["new_home_sales", "housing_starts", "building_permits"],
        "thesis": "New home sales are one of the clearest consumer-cycle and housing-demand signals.",
        "compare_with": ["housing_starts_permits"],
        "reasoning_tips": [
            "New home sales move with affordability, confidence, and rate sensitivity. They help you reason about the upper-end cyclical consumer.",
        ],
    },
    {
        "id": "initial_jobless_claims",
        "title": "Initial Jobless Claims",
        "source": "DOL",
        "cadence": "Weekly",
        "section_id": "stress",
        "series_ids": ["initial_jobless_claims", "initial_jobless_claims_4_week_average"],
        "chart_series_ids": ["initial_jobless_claims", "initial_jobless_claims_4_week_average", "unemployment_rate"],
        "thesis": "Claims are the fastest official labor-stress indicator in the entire dashboard.",
        "compare_with": ["jobs_report", "jolts"],
        "reasoning_tips": [
            "Use the four-week average more than the weekly print. It is less noisy and better for detecting a turn.",
            "Claims can deteriorate before the monthly jobs report shows obvious damage.",
        ],
    },
    {
        "id": "consumer_credit_g19",
        "title": "Consumer Credit G.19",
        "source": "Federal Reserve Board",
        "cadence": "Monthly",
        "section_id": "stress",
        "series_ids": ["consumer_credit_total", "consumer_credit_revolving", "consumer_credit_nonrevolving"],
        "chart_series_ids": ["consumer_credit_total", "consumer_credit_revolving", "consumer_credit_nonrevolving"],
        "thesis": "Consumer credit tells you whether households are leaning harder on borrowing to sustain spending.",
        "compare_with": ["personal_income_outlays", "household_debt_credit"],
        "reasoning_tips": [
            "Revolving credit is the more fragile part of the credit picture because it is typically expensive and tied to short-term strain.",
            "Credit growth without matching income growth can be a late-cycle warning.",
        ],
    },
    {
        "id": "financial_accounts_z1",
        "title": "Financial Accounts Z.1",
        "source": "Federal Reserve Board",
        "cadence": "Quarterly",
        "section_id": "stress",
        "series_ids": ["household_total_assets", "household_total_liabilities", "household_net_worth", "household_total_financial_assets", "household_nonfinancial_assets"],
        "chart_series_ids": ["household_total_assets", "household_total_liabilities", "household_net_worth"],
        "thesis": "Z.1 gives the broad household balance-sheet backdrop behind spending durability and vulnerability.",
        "compare_with": ["consumer_credit_g19", "household_debt_credit"],
        "reasoning_tips": [
            "Net worth helps explain why spending can stay resilient even when cash-flow measures soften.",
            "Look at liabilities with delinquency data. High liabilities are less dangerous when delinquency is stable.",
        ],
    },
    {
        "id": "household_debt_credit",
        "title": "Household Debt and Credit",
        "source": "New York Fed",
        "cadence": "Quarterly",
        "section_id": "stress",
        "series_ids": [
            "household_debt_total",
            "household_credit_card_balance",
            "household_mortgage_balance",
            "household_auto_loan_balance",
            "household_student_loan_balance",
            "household_debt_90_plus_delinquent_rate",
            "new_delinquent_total_rate",
            "new_serious_delinquent_total_rate",
            "household_credit_card_90_plus_delinquent_rate",
            "household_auto_loan_90_plus_delinquent_rate",
        ],
        "chart_series_ids": ["household_debt_90_plus_delinquent_rate", "new_delinquent_total_rate", "new_serious_delinquent_total_rate", "household_credit_card_90_plus_delinquent_rate"],
        "thesis": "This is the best report for seeing where household stress is building by loan type and delinquency severity.",
        "compare_with": ["consumer_credit_g19", "initial_jobless_claims", "financial_accounts_z1"],
        "reasoning_tips": [
            "Delinquency rates often deteriorate before the broad consumer narrative does. They are invaluable for catching hidden stress.",
            "Credit card and auto delinquency are often the fastest areas to weaken when lower-income consumers get squeezed.",
        ],
    },
    {
        "id": "inflation_metrics",
        "title": "Derived Inflation Metrics",
        "source": "Pipeline Derived",
        "cadence": "Monthly",
        "section_id": "inflation",
        "series_ids": [
            "cpi_headline_mom_pct",
            "cpi_headline_yoy_pct",
            "cpi_headline_3m_annualized_pct",
            "cpi_core_yoy_pct",
            "pce_price_index_yoy_pct",
            "core_pce_price_index_yoy_pct",
            "pce_price_index_3m_annualized_pct",
            "core_pce_price_index_3m_annualized_pct",
        ],
        "chart_series_ids": ["cpi_headline_yoy_pct", "cpi_core_yoy_pct", "pce_price_index_yoy_pct", "core_pce_price_index_yoy_pct"],
        "thesis": "These derived metrics convert raw price indexes into the rates investors actually reason with.",
        "compare_with": ["cpi", "personal_income_outlays", "labor_metrics"],
        "reasoning_tips": [
            "YoY is useful for context. MoM and three-month annualized are better for spotting inflection points.",
            "Always compare inflation rates against real wage growth and real income before deciding whether the consumer can absorb them.",
        ],
    },
    {
        "id": "real_spending_metrics",
        "title": "Derived Real Spending Metrics",
        "source": "Pipeline Derived",
        "cadence": "Monthly",
        "section_id": "spending",
        "series_ids": ["real_personal_spending", "real_personal_spending_yoy_pct", "real_retail_sales_proxy"],
        "chart_series_ids": ["real_personal_spending", "real_personal_spending_yoy_pct", "real_retail_sales_proxy"],
        "thesis": "This derived block strips out inflation so you can judge actual demand rather than nominal growth.",
        "compare_with": ["retail_sales", "personal_income_outlays", "real_income_metrics"],
        "reasoning_tips": [
            "Nominal spending can look healthy when inflation is high. Real spending tells you whether volume is really holding up.",
        ],
    },
    {
        "id": "labor_metrics",
        "title": "Derived Labor Metrics",
        "source": "Pipeline Derived",
        "cadence": "Monthly",
        "section_id": "labor",
        "series_ids": ["real_average_hourly_earnings_proxy", "real_wage_growth"],
        "chart_series_ids": ["real_average_hourly_earnings_proxy", "real_wage_growth", "cpi_headline_yoy_pct"],
        "thesis": "This report converts nominal wages into a purchasing-power framework.",
        "compare_with": ["jobs_report", "cpi", "inflation_metrics"],
        "reasoning_tips": [
            "Households experience wages in real terms. If real wages are weak, nominal labor strength can still feel bad on the ground.",
        ],
    },
    {
        "id": "real_income_metrics",
        "title": "Derived Real Income Metrics",
        "source": "Pipeline Derived",
        "cadence": "Monthly",
        "section_id": "inflation",
        "series_ids": ["real_disposable_personal_income_yoy_pct"],
        "chart_series_ids": ["real_disposable_personal_income_yoy_pct", "real_personal_spending_yoy_pct", "savings_rate"],
        "thesis": "This is the cleanest test of whether households are gaining or losing real spendable income.",
        "compare_with": ["personal_income_outlays", "real_spending_metrics", "inflation_metrics"],
        "reasoning_tips": [
            "Real disposable income is one of the best bridges between macro data and the lived consumer experience.",
        ],
    },
    {
        "id": "michigan_sentiment",
        "title": "University of Michigan Consumer Sentiment",
        "source": "University of Michigan",
        "cadence": "Monthly",
        "section_id": "psychology",
        "series_ids": [
            "michigan_sentiment_index",
            "michigan_inflation_expectations_1y",
            "michigan_inflation_expectations_5y",
        ],
        "chart_series_ids": [
            "michigan_sentiment_index",
            "michigan_inflation_expectations_1y",
            "michigan_inflation_expectations_5y",
        ],
        "thesis": "Michigan sentiment is one of the longest-running consumer surveys and has historically led spending inflections by one to three months.",
        "compare_with": ["jobs_report", "real_spending_metrics", "inflation_metrics"],
        "reasoning_tips": [
            "Look at the gap between current conditions and expectations. When expectations fall faster than current conditions, the consumer is front-running deterioration.",
            "5-year inflation expectations above 3.5% historically correlate with Fed policy hawkishness even when current inflation is cooling.",
            "Sentiment can diverge from hard spending data for extended periods. Use it as a leading indicator, not a coincident one.",
        ],
    },
    {
        "id": "savings_metrics",
        "title": "Savings Rate Depth",
        "source": "Pipeline Derived",
        "cadence": "Monthly",
        "section_id": "labor",
        "series_ids": ["savings_rate_3m_avg", "savings_rate_yoy_chg", "excess_savings_cumulative_proxy"],
        "chart_series_ids": ["savings_rate", "savings_rate_3m_avg", "excess_savings_cumulative_proxy"],
        "thesis": "The savings rate is the single most important indicator of whether consumer spending is sustainable or running on borrowed time.",
        "compare_with": ["personal_income_outlays", "real_spending_metrics", "credit_metrics"],
        "reasoning_tips": [
            "When the savings rate falls below 3% while spending grows, households are likely drawing on credit or depleted buffers.",
            "The excess savings proxy tells you how much post-pandemic buffer remains. When it turns deeply negative, the spending cushion is gone.",
            "A rising savings rate can be deflationary — it means households are pulling back. A falling rate can be either healthy confidence or stress.",
        ],
    },
    {
        "id": "distributional_financial_accounts",
        "title": "Distributional Financial Accounts (DFA)",
        "source": "Federal Reserve Board",
        "cadence": "Quarterly",
        "section_id": "distribution",
        "series_ids": [
            "dfa_net_worth_top1pct",
            "dfa_net_worth_next9pct",
            "dfa_net_worth_next40pct",
            "dfa_net_worth_bottom50pct",
        ],
        "chart_series_ids": [
            "dfa_net_worth_top1pct",
            "dfa_net_worth_next9pct",
            "dfa_net_worth_next40pct",
            "dfa_net_worth_bottom50pct",
        ],
        "thesis": "The DFA is the only official data source that breaks household wealth into distributional buckets. Without it, aggregate consumer analysis is an average that may represent almost no individual household.",
        "compare_with": ["financial_accounts_z1", "household_debt_credit", "real_income_metrics"],
        "reasoning_tips": [
            "A rising top-1% share with a flat bottom-50% share means growth is accruing to those least likely to increase marginal spending.",
            "Falling bottom-50% net worth YoY is a more reliable early warning of eventual aggregate consumer stress than delinquency alone.",
            "The DFA is quarterly with a 60-90 day lag. Use it as a structural lens, not a monthly trigger.",
        ],
    },
    {
        "id": "dfa_metrics",
        "title": "Derived DFA Metrics",
        "source": "Pipeline Derived",
        "cadence": "Quarterly",
        "section_id": "distribution",
        "series_ids": ["dfa_wealth_concentration_ratio", "dfa_bottom50_net_worth_yoy_pct"],
        "chart_series_ids": ["dfa_wealth_concentration_ratio", "dfa_bottom50_net_worth_yoy_pct"],
        "thesis": "These derived metrics convert raw DFA level data into actionable ratios and rates of change that reveal wealth polarization dynamics.",
        "compare_with": ["distributional_financial_accounts", "household_debt_credit"],
        "reasoning_tips": [
            "The concentration ratio provides structural context. It tends to rise during bull markets and fall during recessions.",
        ],
    },
    {
        "id": "housing_metrics",
        "title": "Derived Housing Metrics",
        "source": "Pipeline Derived",
        "cadence": "Monthly",
        "section_id": "housing",
        "series_ids": [
            "shelter_affordability_squeeze",
            "home_equity_extraction_proxy",
            "housing_starts_to_permits_ratio",
        ],
        "chart_series_ids": [
            "shelter_affordability_squeeze",
            "home_equity_extraction_proxy",
            "housing_starts_to_permits_ratio",
        ],
        "thesis": "These derived series convert raw housing data into actionable signals about affordability pressure, equity dynamics, and supply pipeline health.",
        "compare_with": ["housing_starts_permits", "new_home_sales", "real_income_metrics"],
        "reasoning_tips": [
            "The shelter affordability squeeze tells you whether housing costs are eating into real household budgets. It is more actionable than raw home price data.",
            "The equity extraction proxy is a late-cycle warning — households rarely extract equity in early expansion. Negative values deserve close attention.",
        ],
    },
    {
        "id": "credit_metrics",
        "title": "Derived Credit Stress Metrics",
        "source": "Pipeline Derived",
        "cadence": "Monthly",
        "section_id": "stress",
        "series_ids": [
            "consumer_credit_revolving_yoy_pct",
            "consumer_credit_total_yoy_pct",
            "household_credit_card_balance_yoy_pct",
        ],
        "chart_series_ids": [
            "consumer_credit_revolving_yoy_pct",
            "consumer_credit_total_yoy_pct",
        ],
        "thesis": "Credit growth rates reveal whether the consumer is borrowing in a healthy way or using credit as an income substitute.",
        "compare_with": ["household_debt_credit", "consumer_credit_g19", "real_spending_metrics"],
        "reasoning_tips": [
            "Revolving credit growth above 10% YoY while real income is flat is a late-cycle warning signal.",
            "Compare credit growth with savings rate direction. If both are moving in the stress direction simultaneously, the signal is more reliable.",
        ],
    },
]

INFLECTION_GUIDE = [
    {
        "id": "disinflation_soft_landing",
        "title": "Disinflation Without Labor Damage",
        "signals": [
            "CPI and PCE trend lower while unemployment stays contained.",
            "Real wages and real disposable income improve instead of deteriorating.",
            "Claims stay calm even as inflation cools.",
        ],
        "economic_read": "This is the classic soft-landing pattern. Households regain purchasing power without taking a large labor hit, which usually supports risk appetite.",
        "sector_tailwinds": ["Consumer discretionary", "Industrials", "Housing-sensitive cyclicals", "Small caps"],
        "sector_headwinds": ["Defensive sectors", "Commodity inflation winners"],
        "watch_in_dashboard": ["inflation", "labor", "spending"],
    },
    {
        "id": "sticky_inflation",
        "title": "Sticky Inflation and Higher-for-Longer Rates",
        "signals": [
            "Core CPI or Core PCE stop improving and re-accelerate.",
            "Real wage growth weakens because prices rise faster than pay.",
            "Housing and rate-sensitive activity soften under tighter financial conditions.",
        ],
        "economic_read": "This is the painful middle ground where nominal activity can still look decent, but household purchasing power erodes and policy stays restrictive.",
        "sector_tailwinds": ["Energy", "Selected financials", "Cash-flow-stable value names"],
        "sector_headwinds": ["Homebuilders", "Long-duration growth", "Highly levered consumer names"],
        "watch_in_dashboard": ["inflation", "spending", "stress"],
    },
    {
        "id": "labor_rollover",
        "title": "Labor Market Rollover",
        "signals": [
            "Claims rise before the jobs report weakens meaningfully.",
            "Openings and quits cool before unemployment clearly jumps.",
            "Real spending starts fading as income confidence slips.",
        ],
        "economic_read": "Labor is the backbone of the U.S. consumer. Once hiring momentum rolls over, downstream pressure tends to reach spending, credit, and cyclical equities.",
        "sector_tailwinds": ["Utilities", "Health care", "Consumer staples", "Longer-duration bonds"],
        "sector_headwinds": ["Consumer discretionary", "Industrials", "Regional banks"],
        "watch_in_dashboard": ["labor", "stress", "spending"],
    },
    {
        "id": "credit_stress",
        "title": "Consumer Credit Stress Broadens",
        "signals": [
            "Card and auto delinquencies rise together.",
            "Revolving credit grows faster than incomes and savings.",
            "Stress appears in lower-income cohorts before the headline consumer cracks.",
        ],
        "economic_read": "This pattern often means the consumer is still spending, but the quality of that spending is deteriorating because borrowing is replacing healthy cash-flow support.",
        "sector_tailwinds": ["Defensive quality", "Low-ticket staples", "Some discount retailers"],
        "sector_headwinds": ["Consumer finance", "Subprime lenders", "Lower-end discretionary"],
        "watch_in_dashboard": ["stress", "spending", "labor"],
    },
    {
        "id": "housing_turn",
        "title": "Housing Turn",
        "signals": [
            "Permits inflect before starts and sales follow.",
            "Inflation cools enough for rates to stop tightening the consumer.",
            "Labor stays firm enough to support household formation and confidence.",
        ],
        "economic_read": "Housing is one of the cleanest rate-sensitive transmission channels. When it turns, it often tells you whether the next move is reacceleration or deeper slowdown.",
        "sector_tailwinds": ["Homebuilders", "Building products", "Mortgage-linked activity", "Regional cyclicals"],
        "sector_headwinds": ["Sectors tied to inflation scarcity trades"],
        "watch_in_dashboard": ["spending", "inflation", "labor"],
    },
]


def _load_observations(processed_dir) -> tuple[list[dict[str, object]], dict[str, list[dict[str, object]]], dict[str, list[dict[str, object]]]]:
    observations: list[dict[str, object]] = []
    series_map: dict[str, list[dict[str, object]]] = {}
    report_map: dict[str, list[dict[str, object]]] = {}
    for path in sorted(processed_dir.glob("*_observations.json")):
        payload = read_json(path, default={})
        for observation in payload.get("observations", []):
            if not isinstance(observation, dict):
                continue
            observations.append(observation)
            series_id = str(observation.get("series_id", "")).strip()
            report_id = str(observation.get("report", "")).strip()
            if series_id:
                series_map.setdefault(series_id, []).append(observation)
            if report_id:
                report_map.setdefault(report_id, []).append(observation)

    for bucket in series_map.values():
        bucket.sort(key=lambda item: str(item.get("period_date", "")))
    for bucket in report_map.values():
        bucket.sort(key=lambda item: (str(item.get("period_date", "")), str(item.get("series_id", ""))))
    observations.sort(key=lambda item: (str(item.get("period_date", "")), str(item.get("series_id", ""))))
    return observations, series_map, report_map


def _parse_iso_date(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        try:
            return datetime.strptime(value, "%Y-%m-%d")
        except ValueError:
            return None


def _format_release_date(value: str) -> str:
    parsed = _parse_iso_date(value)
    if parsed is None:
        return value or "Unknown"
    return parsed.strftime("%b %d, %Y").replace(" 0", " ")


def _format_period(period_date: str, frequency: str) -> str:
    parsed = _parse_iso_date(period_date)
    if parsed is None:
        return period_date
    if frequency == "weekly":
        return f"Week ending {parsed.strftime('%b %d, %Y').replace(' 0', ' ')}"
    if frequency == "quarterly":
        quarter = ((parsed.month - 1) // 3) + 1
        return f"Q{quarter} {parsed.year}"
    return parsed.strftime("%b %Y")


def _latest(series_map: dict[str, list[dict[str, object]]], series_id: str) -> dict[str, object] | None:
    observations = series_map.get(series_id, [])
    return observations[-1] if observations else None


def _previous(series_map: dict[str, list[dict[str, object]]], series_id: str) -> dict[str, object] | None:
    observations = series_map.get(series_id, [])
    return observations[-2] if len(observations) > 1 else None


def _history(series_map: dict[str, list[dict[str, object]]], series_id: str, count: int | None = None) -> list[dict[str, object]]:
    observations = series_map.get(series_id, [])
    if count is None:
        return observations
    return observations[-count:]


def _pretty_series_title(series_id: str, latest: dict[str, object] | None) -> str:
    if series_id in CARD_SPECS:
        return str(CARD_SPECS[series_id]["title"])
    if latest is not None and latest.get("source_series_label"):
        return str(latest["source_series_label"])
    return series_id.replace("_", " ").title()


def _infer_formatter(series_id: str, latest: dict[str, object] | None) -> str:
    if series_id in CARD_SPECS and "formatter" in CARD_SPECS[series_id]:
        return str(CARD_SPECS[series_id]["formatter"])
    if latest is None:
        return "number1"
    unit = str(latest.get("unit", "")).lower()
    value = float(latest.get("value", 0.0))
    if unit == "percent":
        return "percent1"
    if unit in {"claims"}:
        return "claims_k"
    if unit in {"dollars_per_hour"}:
        return "dollars2"
    if unit in {"thousands_of_jobs", "thousands_of_persons", "annual_rate_thousands_units"}:
        return "thousands_to_m"
    if unit == "trillions_of_dollars":
        return "trillions2"
    if unit == "millions_of_dollars":
        return "millions_to_t" if abs(value) >= 1_000_000 else "millions_to_b"
    if unit in {"current dollars; level", "chained dollars; level"}:
        return "annualized_billions_to_t"
    if unit in {"fisher price index; level", "index_1982_84_100", "real_proxy_index"}:
        return "index1"
    if unit == "ratio; level" and series_id == "savings_rate":
        return "percent1"
    return "number1"


def _format_value(value: float, formatter: str) -> str:
    if formatter == "percent1":
        return f"{value:.1f}%"
    if formatter == "percent2":
        return f"{value:.2f}%"
    if formatter == "claims_k":
        return f"{value / 1000:.0f}k"
    if formatter == "thousands_to_m":
        return f"{value / 1000:.1f}M"
    if formatter == "annualized_billions_to_t":
        return f"${value / 1000:.1f}T"
    if formatter == "millions_to_b":
        return f"${value / 1000:.1f}B"
    if formatter == "millions_to_t":
        return f"${value / 1_000_000:.2f}T"
    if formatter == "trillions2":
        return f"${value:.2f}T"
    if formatter == "dollars2":
        return f"${value:.2f}"
    if formatter == "index1":
        return f"{value:.1f}"
    return f"{value:.1f}"


def _format_delta(current: float, prior: float | None, formatter: str) -> str:
    if prior is None:
        return "No prior comparison"
    delta = current - prior
    if formatter.startswith("percent"):
        return f"{delta:+.1f} pts vs prior"
    if formatter == "claims_k":
        return f"{delta / 1000:+.0f}k vs prior"
    if formatter == "thousands_to_m":
        return f"{delta / 1000:+.1f}M vs prior"
    if formatter == "annualized_billions_to_t":
        return f"{delta / 1000:+.1f}T vs prior"
    if formatter == "millions_to_b":
        return f"{delta / 1000:+.1f}B vs prior"
    if formatter == "millions_to_t":
        return f"{delta / 1_000_000:+.2f}T vs prior"
    if formatter == "trillions2":
        return f"{delta:+.2f}T vs prior"
    if formatter == "dollars2":
        return f"{delta:+.2f} vs prior"
    return f"{delta:+.1f} vs prior"


def _compute_trend(history: list[dict[str, object]], periods: int = 3) -> dict[str, object]:
    """Compute trend direction from recent history.

    Returns a dict with:
      - direction: "up", "down", or "flat"
      - arrow: unicode arrow character
      - momentum: short description
      - streak: number of consecutive periods in the same direction
    """
    if len(history) < 2:
        return {"direction": "flat", "arrow": "\u2192", "momentum": "Insufficient data", "streak": 0}

    recent = history[-min(periods + 1, len(history)):]
    deltas = [recent[i]["value"] - recent[i - 1]["value"] for i in range(1, len(recent))]

    avg_delta = sum(deltas) / len(deltas) if deltas else 0.0
    threshold = abs(recent[-1]["value"]) * 0.002 if recent[-1]["value"] != 0 else 0.01

    if avg_delta > threshold:
        direction = "up"
        arrow = "\u2197"  # ↗
    elif avg_delta < -threshold:
        direction = "down"
        arrow = "\u2198"  # ↘
    else:
        direction = "flat"
        arrow = "\u2192"  # →

    # Count streak
    streak = 0
    for i in range(len(history) - 1, 0, -1):
        delta = history[i]["value"] - history[i - 1]["value"]
        if direction == "up" and delta > 0:
            streak += 1
        elif direction == "down" and delta < 0:
            streak += 1
        elif direction == "flat" and abs(delta) <= threshold:
            streak += 1
        else:
            break

    if direction == "up":
        momentum = f"Rising for {streak} period{'s' if streak != 1 else ''}" if streak > 0 else "Ticking up"
    elif direction == "down":
        momentum = f"Falling for {streak} period{'s' if streak != 1 else ''}" if streak > 0 else "Ticking down"
    else:
        momentum = "Stable"

    return {"direction": direction, "arrow": arrow, "momentum": momentum, "streak": streak}


def _compute_percentile_rank(series_map: dict[str, list[dict[str, object]]], series_id: str, current_value: float) -> float | None:
    """Where does the current value sit vs its own full history? Returns 0-100 or None."""
    observations = series_map.get(series_id, [])
    if len(observations) < 6:
        return None
    values = sorted(float(obs.get("value", 0.0)) for obs in observations)
    count_below = sum(1 for v in values if v < current_value)
    return round((count_below / len(values)) * 100, 1)


def _tone_for_series(series_id: str, value: float) -> str:
    low_is_good = {
        "unemployment_rate": (4.2, 4.8),
        "initial_jobless_claims": (220000, 260000),
        "initial_jobless_claims_4_week_average": (220000, 260000),
        "cpi_headline_yoy_pct": (2.5, 3.25),
        "cpi_core_yoy_pct": (2.8, 3.4),
        "pce_price_index_yoy_pct": (2.5, 3.0),
        "core_pce_price_index_yoy_pct": (2.7, 3.2),
        "household_debt_90_plus_delinquent_rate": (2.5, 3.5),
        "new_delinquent_total_rate": (4.5, 6.0),
        "new_serious_delinquent_total_rate": (2.8, 4.0),
        # Phase 4: inflation detail
        "cpi_shelter_yoy_pct": (3.5, 5.0),
        "cpi_owners_equivalent_rent_yoy_pct": (3.5, 5.0),
        "cpi_services_ex_energy_yoy_pct": (3.0, 4.0),
        "cpi_shelter_vs_services_spread": (1.0, 2.5),
        # Phase 2: credit stress
        "consumer_credit_revolving_yoy_pct": (5.0, 10.0),
        "consumer_credit_total_yoy_pct": (4.0, 8.0),
        "household_credit_card_balance_yoy_pct": (5.0, 10.0),
        "household_credit_card_90_plus_delinquent_rate": (7.5, 10.0),
        "household_auto_loan_90_plus_delinquent_rate": (3.0, 4.5),
        # Phase 6: housing
        "shelter_affordability_squeeze": (0.5, 2.0),
        # Phase 7: DFA (low is good = lower concentration ratio)
        "dfa_wealth_concentration_ratio": (20.0, 30.0),
    }
    high_is_good = {
        "real_wage_growth": (1.0, 0.0),
        "real_disposable_personal_income_yoy_pct": (1.0, 0.0),
        "real_personal_spending_yoy_pct": (1.0, 0.0),
        "savings_rate": (4.5, 3.5),
        "jolts_quits_rate": (2.2, 1.8),
        # Phase 3: savings depth
        "savings_rate_3m_avg": (4.5, 3.5),
        "housing_starts_to_permits_ratio": (0.85, 0.75),
        # Phase 5: sentiment
        "michigan_sentiment_index": (80.0, 70.0),
    }
    if series_id in low_is_good:
        good, neutral = low_is_good[series_id]
        if value <= good:
            return "positive"
        if value <= neutral:
            return "neutral"
        return "caution"
    if series_id in high_is_good:
        good, neutral = high_is_good[series_id]
        if value >= good:
            return "positive"
        if value >= neutral:
            return "neutral"
        return "caution"
    return "neutral"


def _build_metric(series_map: dict[str, list[dict[str, object]]], series_id: str, history_count: int = 18) -> dict[str, object] | None:
    latest = _latest(series_map, series_id)
    if latest is None:
        return None
    previous = _previous(series_map, series_id)
    formatter = _infer_formatter(series_id, latest)
    latest_value = float(latest.get("value", 0.0))
    previous_value = float(previous.get("value", 0.0)) if previous is not None else None
    report_id = str(latest.get("report", ""))
    history = _history(series_map, series_id, count=history_count)
    history_dicts = [
        {
            "period_date": str(item.get("period_date", "")),
            "period_label": _format_period(str(item.get("period_date", "")), str(item.get("frequency", ""))),
            "value": float(item.get("value", 0.0)),
        }
        for item in history
    ]
    trend = _compute_trend(history_dicts)
    percentile = _compute_percentile_rank(series_map, series_id, latest_value)
    return {
        "series_id": series_id,
        "title": _pretty_series_title(series_id, latest),
        "why_it_matters": str(CARD_SPECS.get(series_id, {}).get("why", "")),
        "value": latest_value,
        "value_display": _format_value(latest_value, formatter),
        "delta_display": _format_delta(latest_value, previous_value, formatter),
        "trend_arrow": trend["arrow"],
        "trend_direction": trend["direction"],
        "trend_momentum": trend["momentum"],
        "trend_streak": trend["streak"],
        "percentile_rank": percentile,
        "period_label": _format_period(str(latest.get("period_date", "")), str(latest.get("frequency", ""))),
        "release_date": _format_release_date(str(latest.get("release_date", ""))),
        "frequency": str(latest.get("frequency", "")),
        "source": str(latest.get("source", "")),
        "unit": str(latest.get("unit", "")),
        "source_series_label": str(latest.get("source_series_label", "")),
        "tone": _tone_for_series(series_id, latest_value),
        "report_id": report_id,
        "drilldown_href": f"#report-{report_id}" if report_id else "",
        "history": history_dicts,
    }


def _build_chart(
    series_map: dict[str, list[dict[str, object]]],
    chart_series_ids: list[str],
    *,
    title: str,
    note: str,
    default_mode: str = "rebased",
) -> dict[str, object]:
    series_list = []
    for series_id in chart_series_ids:
        metric = _build_metric(series_map, series_id, history_count=18)
        if metric is None or len(metric["history"]) < 2:
            continue
        history = metric["history"]
        raw_points = [{"label": item["period_label"], "value": item["value"]} for item in history]
        base = history[0]["value"] or 1.0
        rebased_points = [
            {
                "label": item["period_label"],
                "value": (item["value"] / base) * 100.0 if base else 100.0,
            }
            for item in history
        ]
        series_list.append(
            {
                "series_id": series_id,
                "title": metric["title"],
                "tone": metric["tone"],
                "unit": metric["unit"],
                "raw_latest_display": metric["value_display"],
                "rebased_latest_display": f"{rebased_points[-1]['value']:.1f}",
                "raw_points": raw_points,
                "rebased_points": rebased_points,
            }
        )
    return {
        "title": title,
        "note": note,
        "default_mode": default_mode,
        "series": series_list,
    }


def _report_title(report_id: str) -> str:
    for spec in REPORT_SPECS:
        if spec["id"] == report_id:
            return str(spec["title"])
    return report_id.replace("_", " ").title()


def _build_pillars(series_map: dict[str, list[dict[str, object]]]) -> list[dict[str, object]]:
    def _metric_display(series_id: str) -> str:
        metric = _build_metric(series_map, series_id)
        return metric["value_display"] if metric is not None else "n/a"

    definitions = [
        {
            "id": "labor",
            "title": "Labor",
            "series_id": "unemployment_rate",
            "positive": "Still firm",
            "neutral": "Cooling but orderly",
            "caution": "Cracking",
            "detail_builder": lambda: f"Unemployment {_metric_display('unemployment_rate')}, claims avg {_metric_display('initial_jobless_claims_4_week_average')}.",
        },
        {
            "id": "inflation",
            "title": "Inflation",
            "series_id": "cpi_headline_yoy_pct",
            "positive": "Cooling",
            "neutral": "Sticky",
            "caution": "Re-accelerating risk",
            "detail_builder": lambda: f"CPI {_metric_display('cpi_headline_yoy_pct')}, Core PCE {_metric_display('core_pce_price_index_yoy_pct')}.",
        },
        {
            "id": "spending",
            "title": "Spending Power",
            "series_id": "real_personal_spending_yoy_pct",
            "positive": "Holding up",
            "neutral": "Mixed",
            "caution": "Fading",
            "detail_builder": lambda: f"Real spending {_metric_display('real_personal_spending_yoy_pct')}, real DPI {_metric_display('real_disposable_personal_income_yoy_pct')}.",
        },
        {
            "id": "stress",
            "title": "Stress",
            "series_id": "household_debt_90_plus_delinquent_rate",
            "positive": "Contained",
            "neutral": "Building slowly",
            "caution": "Rising",
            "detail_builder": lambda: f"90+ delinquency {_metric_display('household_debt_90_plus_delinquent_rate')}, new serious delinquency {_metric_display('new_serious_delinquent_total_rate')}.",
        },
    ]
    pillars = []
    for item in definitions:
        metric = _build_metric(series_map, item["series_id"])
        if metric is None:
            continue
        tone = metric["tone"]
        pillars.append(
            {
                "id": item["id"],
                "title": item["title"],
                "tone": tone,
                "stance": item[tone],
                "detail": item["detail_builder"](),
            }
        )
    return pillars


def _build_executive_text(series_map: dict[str, list[dict[str, object]]], regime_label: str = "", composite_score: float = 0.0) -> tuple[str, list[str], list[str]]:
    unemployment = _build_metric(series_map, "unemployment_rate")
    inflation = _build_metric(series_map, "cpi_headline_yoy_pct")
    real_wages = _build_metric(series_map, "real_wage_growth")
    spending = _build_metric(series_map, "real_personal_spending_yoy_pct")
    stress = _build_metric(series_map, "household_debt_90_plus_delinquent_rate")

    regime_descriptions = {
        "expansion": "The consumer is in expansion mode",
        "slowing": "The consumer is in a slowing phase with mixed signals",
        "stressed": "The consumer is showing signs of stress across multiple dimensions",
        "recessionary": "The consumer backdrop is recessionary with broad deterioration",
    }
    regime_prefix = regime_descriptions.get(regime_label, "The consumer backdrop is mixed")

    headline_bits = []
    if unemployment and unemployment["tone"] == "positive":
        headline_bits.append("labor is still firm")
    elif unemployment:
        headline_bits.append("labor is cooling")
    if inflation and inflation["tone"] == "positive":
        headline_bits.append("inflation has cooled materially")
    elif inflation:
        headline_bits.append("inflation is still constraining households")
    if stress and stress["tone"] == "caution":
        headline_bits.append("but household stress is rising")
    elif stress:
        headline_bits.append("while stress remains contained")

    headline = f"{regime_prefix}: " + ", ".join(headline_bits[:3]) + "."

    positives = []
    watchlist = []
    if real_wages and real_wages["tone"] == "positive":
        positives.append(f"Real wages are positive at {real_wages['value_display']} ({real_wages.get('trend_arrow', '')} {real_wages.get('trend_momentum', '')}).")
    if spending and spending["tone"] != "caution":
        positives.append(f"Real spending is still growing at {spending['value_display']} ({spending.get('trend_arrow', '')} {spending.get('trend_momentum', '')}).")
    if inflation and inflation["tone"] == "positive":
        positives.append(f"Headline CPI is running at {inflation['value_display']}, well below the 2022 peak.")

    if stress:
        watchlist.append(f"90+ day household delinquency sits at {stress['value_display']} ({stress.get('trend_arrow', '')} {stress.get('trend_momentum', '')}).")
    claims = _build_metric(series_map, "initial_jobless_claims_4_week_average")
    if claims:
        watchlist.append(f"Weekly claims average is {claims['value_display']} and should be watched for a turn.")
    core_pce = _build_metric(series_map, "core_pce_price_index_yoy_pct")
    if core_pce and core_pce["tone"] != "positive":
        watchlist.append(f"Core PCE remains sticky at {core_pce['value_display']}.")

    if not positives:
        positives.append("The data mix is balanced rather than outright bullish.")
    return headline, positives[:3], watchlist[:3]


def _build_data_freshness(series_map: dict[str, list[dict[str, object]]]) -> list[dict[str, object]]:
    """Build a freshness report showing the latest data point per source."""
    source_freshness: dict[str, dict[str, str]] = {}
    for series_id, observations in series_map.items():
        if not observations:
            continue
        latest = observations[-1]
        source = str(latest.get("source", "unknown"))
        period = str(latest.get("period_date", ""))
        release = str(latest.get("release_date", ""))
        freq = str(latest.get("frequency", ""))
        existing = source_freshness.get(source)
        if existing is None or period > existing.get("period_date", ""):
            source_freshness[source] = {
                "source": source,
                "period_date": period,
                "release_date": release,
                "frequency": freq,
                "series_id": series_id,
            }
    result = []
    for source, info in sorted(source_freshness.items()):
        result.append({
            "source": info["source"],
            "latest_period": _format_period(info["period_date"], info["frequency"]),
            "latest_release": _format_release_date(info["release_date"]),
            "frequency": info["frequency"],
            "freshness_status": _freshness_status(info["period_date"]),
        })
    return result


def _freshness_status(period_date: str) -> str:
    """Return 'fresh', 'aging', or 'stale' based on how recent the period is."""
    parsed = _parse_iso_date(period_date)
    if parsed is None:
        return "stale"
    now = datetime.utcnow()
    delta_days = (now - parsed).days
    if delta_days <= 45:
        return "fresh"
    if delta_days <= 120:
        return "aging"
    return "stale"


def _build_investor_guide(series_map: dict[str, list[dict[str, object]]]) -> dict[str, object]:
    claims = _build_metric(series_map, "initial_jobless_claims_4_week_average")
    inflation = _build_metric(series_map, "cpi_headline_yoy_pct")
    core_pce = _build_metric(series_map, "core_pce_price_index_yoy_pct")
    real_wages = _build_metric(series_map, "real_wage_growth")
    stress = _build_metric(series_map, "household_debt_90_plus_delinquent_rate")

    current_setup_bits = []
    if inflation is not None:
        current_setup_bits.append(f"headline CPI is {inflation['value_display']}")
    if core_pce is not None:
        current_setup_bits.append(f"core PCE is {core_pce['value_display']}")
    if real_wages is not None:
        current_setup_bits.append(f"real wages are {real_wages['value_display']}")
    if claims is not None:
        current_setup_bits.append(f"claims average {claims['value_display']}")
    if stress is not None:
        current_setup_bits.append(f"90+ day delinquency is {stress['value_display']}")

    setup_text = "Current setup: " + ", ".join(current_setup_bits[:5]) + "." if current_setup_bits else ""

    playbooks = []
    for item in INFLECTION_GUIDE:
        playbooks.append(
            {
                "id": item["id"],
                "title": item["title"],
                "signals": list(item["signals"]),
                "economic_read": item["economic_read"],
                "sector_tailwinds": list(item["sector_tailwinds"]),
                "sector_headwinds": list(item["sector_headwinds"]),
                "watch_links": [
                    {"id": section_id, "title": next(spec["title"] for spec in SECTION_SPECS if spec["id"] == section_id), "href": f"#{section_id}"}
                    for section_id in item["watch_in_dashboard"]
                ],
            }
        )

    return {
        "id": "investor-guide",
        "label": "Investor Guide",
        "title": "How Economic Turns Usually Travel Into Markets",
        "intro": (
            "Use this section when you want context, not just current numbers. These are the recurring macro inflection patterns "
            "that matter most for the U.S. consumer and the sectors that often respond first."
        ),
        "current_setup": setup_text,
        "playbooks": playbooks,
    }


def _build_section(spec: dict[str, object], series_map: dict[str, list[dict[str, object]]]) -> dict[str, object]:
    cards = []
    for series_id in spec["card_series_ids"]:
        metric = _build_metric(series_map, series_id)
        if metric is not None:
            cards.append(metric)
    return {
        "id": spec["id"],
        "title": spec["title"],
        "label": spec["label"],
        "intro": spec["intro"],
        "cards": cards,
        "chart": _build_chart(
            series_map,
            spec["chart_series_ids"],
            title=str(spec["chart_title"]),
            note=str(spec["chart_note"]),
            default_mode="rebased",
        ),
        "report_links": [
            {"id": report_id, "title": _report_title(report_id), "href": f"#report-{report_id}"}
            for report_id in spec["report_ids"]
        ],
    }


def _build_report_deep_dive(spec: dict[str, object], series_map: dict[str, list[dict[str, object]]], report_map: dict[str, list[dict[str, object]]]) -> dict[str, object]:
    metrics = []
    for series_id in spec["series_ids"]:
        metric = _build_metric(series_map, series_id, history_count=24)
        if metric is not None:
            metrics.append(metric)
    report_observations = report_map.get(str(spec["id"]), [])
    latest_release = ""
    if report_observations:
        latest_release = max(str(item.get("release_date", "")) for item in report_observations)
    summary = str(spec["thesis"])
    if metrics:
        featured = ", ".join(f"{metric['title']} {metric['value_display']}" for metric in metrics[:3])
        summary = f"{spec['thesis']} Latest read: {featured}."
    return {
        "id": spec["id"],
        "title": spec["title"],
        "source": spec["source"],
        "cadence": spec["cadence"],
        "section_id": spec["section_id"],
        "summary": summary,
        "latest_release": _format_release_date(latest_release),
        "metric_count": len(metrics),
        "metrics": metrics,
        "chart": _build_chart(
            series_map,
            spec["chart_series_ids"],
            title=f"{spec['title']} comparison chart",
            note="Rebased to 100 at the start of the visible window unless the series are already comparable rates.",
            default_mode="raw",
        ),
        "compare_with": [
            {"id": report_id, "title": _report_title(report_id), "href": f"#report-{report_id}"}
            for report_id in spec.get("compare_with", [])
        ],
        "reasoning_tips": list(spec.get("reasoning_tips", [])),
    }


def _build_regime_for_dashboard(series_map: dict[str, list[dict[str, object]]]) -> dict[str, object]:
    """Attempt regime classification using the observation dicts.

    The regime module expects Observation dataclasses, but the dashboard works
    with plain dicts.  We construct lightweight Observation-like objects so
    classify_regime can do its work.
    """
    obs_map: dict[str, list[Observation]] = {}
    for sid, observations in series_map.items():
        converted: list[Observation] = []
        for obs in observations:
            converted.append(Observation(
                series_id=str(obs.get("series_id", sid)),
                period_date=str(obs.get("period_date", "")),
                value=float(obs.get("value", 0.0)),
                frequency=str(obs.get("frequency", "")),
                unit=str(obs.get("unit", "")),
                source=str(obs.get("source", "")),
                report=str(obs.get("report", "")),
                release_date=str(obs.get("release_date", "")),
            ))
        obs_map[sid] = converted

    regime_label, composite, pillar_scores = classify_regime(obs_map)

    regime_tone = {
        "expansion": "positive",
        "slowing": "neutral",
        "stressed": "caution",
        "recessionary": "caution",
    }
    return {
        "regime": regime_label,
        "regime_display": regime_label.title(),
        "composite_score": round(composite, 1),
        "tone": regime_tone.get(regime_label, "neutral"),
        "pillar_scores": {k: v for k, v in pillar_scores.items() if v is not None},
    }


def _build_memo_ready(
    series_map: dict[str, list[dict[str, object]]],
    regime_info: dict[str, object],
    pillars: list[dict[str, object]],
    positives: list[str],
    watchlist: list[str],
    data_freshness: list[dict[str, object]],
) -> dict[str, object]:
    """Pre-assemble narrative inputs for the monthly memo generator.

    Returns a dict with everything memo.py needs to generate a deterministic
    narrative without re-parsing observations.
    """
    def _metric_snapshot(series_id: str) -> dict[str, object] | None:
        m = _build_metric(series_map, series_id)
        if m is None:
            return None
        return {
            "series_id": series_id,
            "title": m["title"],
            "value_display": m["value_display"],
            "delta_display": m["delta_display"],
            "tone": m["tone"],
            "trend_arrow": m["trend_arrow"],
            "trend_momentum": m["trend_momentum"],
            "period_label": m["period_label"],
            "why_it_matters": m["why_it_matters"],
        }

    key_series = [
        "unemployment_rate",
        "cpi_headline_yoy_pct",
        "core_pce_price_index_yoy_pct",
        "real_wage_growth",
        "real_personal_spending_yoy_pct",
        "savings_rate",
        "excess_savings_cumulative_proxy",
        "household_debt_90_plus_delinquent_rate",
        "consumer_credit_revolving_yoy_pct",
        "initial_jobless_claims_4_week_average",
        "michigan_sentiment_index",
        "michigan_inflation_expectations_5y",
        "cpi_shelter_yoy_pct",
        "cpi_services_ex_energy_yoy_pct",
        "shelter_affordability_squeeze",
        "dfa_bottom50_net_worth_yoy_pct",
        "dfa_wealth_concentration_ratio",
    ]
    snapshots = {}
    for sid in key_series:
        snap = _metric_snapshot(sid)
        if snap is not None:
            snapshots[sid] = snap

    # Identify series at extreme percentile ranks (>75th in bad direction)
    extreme_cautions = []
    for series_id, observations in series_map.items():
        if not observations:
            continue
        latest_val = float(observations[-1].get("value", 0.0))
        tone = _tone_for_series(series_id, latest_val)
        if tone == "caution":
            pct = _compute_percentile_rank(series_map, series_id, latest_val)
            if pct is not None and pct >= 75:
                title = _pretty_series_title(series_id, observations[-1])
                extreme_cautions.append({
                    "series_id": series_id,
                    "title": title,
                    "value_display": _format_value(latest_val, _infer_formatter(series_id, observations[-1])),
                    "percentile_rank": pct,
                })
    extreme_cautions.sort(key=lambda x: -x["percentile_rank"])

    stale_sources = [s for s in data_freshness if s["freshness_status"] == "stale"]

    return {
        "regime": regime_info,
        "pillars": pillars,
        "positives": positives,
        "watchlist": watchlist,
        "key_snapshots": snapshots,
        "extreme_cautions": extreme_cautions[:5],
        "stale_sources": stale_sources,
    }


def build_dashboard_data(settings) -> dict:
    ensure_project_directories(settings)
    _, series_map, report_map = _load_observations(settings.processed_dir)

    # Compute regime classification
    regime_info = _build_regime_for_dashboard(series_map)

    sections = [_build_section(spec, series_map) for spec in SECTION_SPECS]
    report_library = [_build_report_deep_dive(spec, series_map, report_map) for spec in REPORT_SPECS]
    headline, positives, watchlist = _build_executive_text(
        series_map,
        regime_label=regime_info["regime"],
        composite_score=regime_info["composite_score"],
    )
    pillars = _build_pillars(series_map)
    investor_guide = _build_investor_guide(series_map)
    executive_cards = [card for card in sections[0]["cards"]]
    data_freshness = _build_data_freshness(series_map)
    # Build memo_ready — pre-assembled inputs for the narrative generator (Phase 8)
    memo_ready = _build_memo_ready(series_map, regime_info, pillars, positives, watchlist, data_freshness)

    payload = {
        "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "navigation": [{"id": section["id"], "label": section["label"], "href": f"#{section['id']}"} for section in sections]
        + [
            {"id": investor_guide["id"], "label": investor_guide["label"], "href": f"#{investor_guide['id']}"},
            {"id": "report-library", "label": "Deep Dives", "href": "#report-library"},
            {"id": "data-freshness", "label": "Freshness", "href": "#data-freshness"},
        ],
        "executive_snapshot": {
            "title": "The U.S. Consumer Workbench",
            "headline": headline,
            "positives": positives,
            "watchlist": watchlist,
            "pillars": pillars,
            "cards": executive_cards,
            "regime": regime_info,
        },
        "sections": sections,
        "investor_guide": investor_guide,
        "report_library": report_library,
        "data_freshness": data_freshness,
        "memo_ready": memo_ready,
        "message": (
            f"Built dashboard dataset with {len(executive_cards)} fast-read cards, "
            f"{len(sections)} overview sections, {len(investor_guide['playbooks'])} inflection playbooks, "
            f"{len(report_library)} report deep dives, and regime: {regime_info['regime_display']}."
        ),
    }
    output_path = settings.processed_dir / "dashboard_data.json"
    write_json(output_path, payload)
    status = {
        "status": "built",
        "output_path": str(output_path),
        "executive_card_count": len(executive_cards),
        "section_count": len(sections),
        "inflection_playbook_count": len(investor_guide["playbooks"]),
        "report_count": len(report_library),
        "regime": regime_info["regime"],
        "message": payload["message"],
    }
    write_json(settings.processed_dir / "dashboard_build_status.json", status)
    return {**payload, "output_path": str(output_path)}
