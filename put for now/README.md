# Enriched Leads Data Analysis Pipeline

This project provides a comprehensive statistical and machine learning analysis pipeline for your enriched leads data (`enriched_leads.csv`).

## Features
- Data overview and missing value analysis
- Smart missing value handling
- Exploratory Data Analysis (EDA) with plots
- Feature engineering (interactions, encodings)
- Statistical tests (normality, correlations)
- Automated model building (classification/regression)
- Results and feature importance summaries

## Setup
1. **Install Python 3.8+** (if not already installed)
2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

## Usage
Run the analysis pipeline from the command line:
```bash
python enriched_leads_analysis.py
```

- By default, it analyzes `enriched_leads.csv` in the current directory.
- You can edit the script to analyze a different file or set a specific target column.

## Outputs
- Console output: Data overview, missing value stats, EDA summaries, statistical test results, and model performance.
- Plots: Shown interactively (histograms, correlation heatmaps, bar charts for categorical variables).
- Model results: Classification/regression reports and feature importances.

## Tips for Valuable Analysis
- **Target column:** The script will suggest target columns automatically, but you can specify one by editing the `run_complete_analysis(target_column='your_column')` call.
- **Interpretation:**
  - Use the EDA and feature importance outputs to understand which features drive your target.
  - Review missing value stats to decide if you need to clean or impute further.
- **Customization:**
  - You can run individual steps (EDA, feature engineering, modeling) by calling the respective methods in a notebook or script.
  - For large datasets, consider sampling or filtering for faster analysis.

## Troubleshooting
- If you see `ModuleNotFoundError`, make sure you installed all dependencies with `pip install -r requirements.txt`.
- For questions or custom analysis, edit the script or contact your data science lead.

--- 