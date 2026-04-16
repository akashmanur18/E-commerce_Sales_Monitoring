# E-Commerce Sales Monitoring & Performance Analysis

## Project Overview

This repository contains a complete data analytics pipeline built around the Global Superstore dataset. The goal is to transform raw order-level data into a cleaned, merged dataset, analyze business performance, and create a visual dashboard with advanced charts.

The project is implemented in `main.py` and is designed for:

- data ingestion from both CSV and Excel inputs
- dataset merging and unification
- data cleaning and validation
- feature engineering for richer analysis
- exploratory data analysis (EDA) and key performance indicator (KPI) reporting
- database storage using SQLite (with optional MySQL support)
- creation of a single combined visualization image
- export of a final cleaned dataset for further use

## Data Sources

The project uses two input files:

- `Global_Superstore2.csv`
- <a href="https://github.com/akashmanur18/E-commerce_Sales_Monitoring/blob/main/Global_Superstore2.csv">Sales_data_set.csv</a>

- `Global_Superstore2.xlsx`
- <a href="https://github.com/akashmanur18/E-commerce_Sales_Monitoring/blob/main/Global_Superstore2.xlsx">Sales_data_set.xlsx</a>

Both sources contain Global Superstore sales data. The pipeline reads both files and merges them on the common key `Product Name`.

### Why both CSV and Excel?

- The CSV file is the primary sales dataset and provides the main order history.
- The Excel file is loaded as an additional source to support the merge and may contain supplementary product-level details.

## Full Project Workflow

### 1. Read input files

The script reads the CSV using `pandas.read_csv` and the Excel workbook using `pandas.read_excel`.

### 2. Merge data sources

The CSV and Excel files are merged on `Product Name`, keeping all CSV rows and bringing in matching Excel fields. Duplicate columns created by the merge are dropped automatically.

### 3. Data cleaning and preprocessing

The pipeline cleans the merged dataset with these steps:

- remove exact duplicate rows
- report and fill missing values
- replace missing `Postal Code` values with `N/A`
- fill numeric columns with the median value
- fill categorical columns with the most common value (mode)
- parse `Order Date` and `Ship Date` into proper datetime fields
- convert `Quantity`, `Sales`, `Profit`, and `Discount` to numeric types
- remove rows where `Sales` is not positive
- detect outliers using the IQR method and report them without dropping them

### 4. Feature engineering

New analytical fields are created from the cleaned data:

- `Year`, `Month`, `Month Name`, `Quarter`, `Day of Week`
- `Ship Days` and `Delivery Speed`
- `Profit Margin %`
- `Revenue per Unit`
- `Discount %` and discount range category (`Disc Category`)
- `Profit Status` (`Profit` / `Loss`)
- label encoded codes for categorical fields used in correlation analysis

### 5. Exploratory data analysis (EDA)

The script computes and prints:

- descriptive statistics for financial and shipping metrics
- business KPIs such as total sales, total profit, average profit margin, average discount, average ship days, and loss-making orders
- grouped summaries by category, region, country, and segment
- a correlation matrix for numeric features

### 6. Database integration

The default pipeline saves the cleaned dataset into an SQLite database at `outputs/global_superstore.db`.

It also supports optional MySQL integration by setting `USE_MYSQL = True` and providing connection credentials in `main.py`.

### 7. Visualization dashboard
- <a href="https://github.com/akashmanur18/E-commerce_Sales_Monitoring/blob/main/output/ecommerce_dashboard_visualizations.png">Visualisations</a>

The project generates a combined dashboard image containing 12 advanced plots. The dashboard is saved as:
<img width="4024" height="6888" alt="ecommerce_dashboard_visualizations" src="https://github.com/user-attachments/assets/cefea092-4260-4921-8951-0418b3d70508" />

- `outputs/ecommerce_dashboard_visualizations.png`

The visualization set includes:

- Monthly sales trend by year
- Sales vs profit by category
- Regional profit heatmap
- Discount vs profit margin scatter
- Yearly profit by segment
- Sub-category profit/loss analysis
- Shipping mode sales share donut chart
- Quarterly sales area trend
- Profit margin distribution by category
- Correlation heatmap for numeric features
- Top 10 sub-categories by sales lollipop plot
- Market-wise profit distribution box plot

### 8. Export final cleaned dataset

After cleaning and feature engineering, the final unified dataset is saved as:

- `outputs/Global_Superstore_Final_Cleaned.csv`

This final CSV is saved using `utf-8-sig` for Excel compatibility.

## Installation

Install the required Python packages:

```bash
pip install pandas matplotlib seaborn scipy scikit-learn sqlalchemy mysql-connector-python openpyxl xlrd
```

## Usage

Run the project with:

```bash
python main.py
```

If you want to use MySQL instead of SQLite:

1. open `main.py`
2. set `USE_MYSQL = True`
3. update `DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_PORT`, and `DB_NAME`

## Project Outputs

The pipeline generates these output files:

- `outputs/ecommerce_dashboard_visualizations.png`
- `outputs/Global_Superstore_Final_Cleaned.csv`
- <a href="https://github.com/akashmanur18/E-commerce_Sales_Monitoring/blob/main/output/Global_Superstore_Final_Cleaned.csv"> Cleaned_csv_file</a>
- `outputs/global_superstore.db`

## Visualizations

A combined dashboard image is created and saved to `outputs/ecommerce_dashboard_visualizations.png`.

### Dashboard features

The dashboard helps answer business questions such as:

- which months have the highest sales trend?
- which categories generate the most revenue and profit?
- which regions and markets are most profitable?
- how does discount affect profit margin?
- which product sub-categories are loss-making?

### Preview

![Dashboard Preview](./outputs/ecommerce_dashboard_visualizations.png)

Screanshots = 
1 = <img width="1285" height="731" alt="Global_Store1" src="https://github.com/user-attachments/assets/ee6a1c4b-10f0-48c5-ac7e-e98879e5c4c0" />
2 = <img width="1285" height="723" alt="Global_Store2" src="https://github.com/user-attachments/assets/1c1ffda0-dd13-48bc-8255-ff7af4a397da" />
3 = <img width="1286" height="727" alt="Global_Store3" src="https://github.com/user-attachments/assets/2454f320-9402-49f8-828a-39185a080074" />
4 = <img width="1290" height="721" alt="Global_Store4" src="https://github.com/user-attachments/assets/1a499a05-d898-4485-911d-a1e5d4f9dbd9" />
5 = <img width="1290" height="727" alt="Global_Store5" src="https://github.com/user-attachments/assets/86508d50-c144-4353-9944-d50fdcb76178" />
6 = <img width="1290" height="726" alt="Global_Store6" src="https://github.com/user-attachments/assets/7227de90-d7a8-423e-a381-74e03fc991c3" />






## Notes

- Merge logic is based on `Product Name`, so product naming consistency matters.
- The analysis keeps outliers for completeness, while still flagging them.
- The final CSV is exported with Excel-friendly encoding.

## Author

Akash Manur | Data Analyst
