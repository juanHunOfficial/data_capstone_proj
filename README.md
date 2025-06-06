# 💳 Financial Services ETL Pipeline Project

A final data engineering project focused on building an end-to-end ETL pipeline for financial services data using both functional and object-oriented programming approaches. This project features a working command-line interface (CLI), a MySQL data warehouse, PySpark transformations, and Tableau dashboards for interactive visualizations.

---

## 📋 Table of Contents

- [📌 Project Overview](#project-overview)
- [🧰 Tech Stack](#tech-stack)
- [🔄 ETL Process](#etl-process)
- [📂 Repository Structure](#repository-structure)
- [📈 Visualizations](#visualizations)
- [🚀 How to Run](#how-to-run)
- [📌 Conclusion](#conclusion)

---

## 📌 Project Overview

This ETL pipeline extracts data from three JSON files and a public API, transforms the data based on a mapping document provided by Per Scholas, and loads it into a MySQL database. The project includes:

- Two separate implementations:
  - A **functional programming** MVP (`functional_capstone`)
  - A final **object-oriented** version (`oop_capstone`)
- A CLI for interacting with the ETL and query system
- Tableau dashboards for data visualization

---

## 🧰 Tech Stack

| Tool          | Description                  |
|---------------|------------------------------|
| ![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white) | Core programming language |
| ![MySQL](https://img.shields.io/badge/MySQL-4479A1?style=for-the-badge&logo=mysql&logoColor=white) | Data warehouse            |
| ![PySpark](https://img.shields.io/badge/PySpark-E34A86?style=for-the-badge&logo=apache-spark&logoColor=white) | Data transformation engine |
| ![Tableau](https://img.shields.io/badge/Tableau-E97627?style=for-the-badge&logo=tableau&logoColor=white) | Interactive data visualization |

---

## 🔄 ETL Process

**Data Sources:**
- `cdw_sapp_branch.json`
- `cdw_sapp_credit.json`
- `cdw_sapp_customer.json`
- API: [Loan Data JSON](https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json)

**Steps:**

1. **Extract**: Load local JSON files and fetch data from the API.
2. **Transform**: Apply necessary data cleaning and restructuring using PySpark, based on the mapping document.
3. **Load**: Insert cleaned and structured data into MySQL tables for efficient querying.

The transformation logic adheres strictly to the mapping document provided by Per Scholas to ensure accuracy and consistency.

---

## 📂 Repository Structure

📁 origin_data/
└── Raw JSON data files
📁 functional_capstone/
├── capstone_etl.py
├── front_end.py
├── main.py
├── test_visuals.ipynb
└── visualizations.py
📁 oop_capstone/
├── data/
├── database_connector.py
├── etl.py
├── main.py
├── menu.py
├── requirements.txt
└── visualizations.py


---

## 📈 Visualizations

Data visualizations are handled using Tableau for a modern, interactive experience. Though excluded from the CLI, a script (`visualizations.py`) is still available in both implementations for local testing and development.

🔗 **Tableau Dashboards**: *(Add link if hosted publicly)*

---

## 🚀 How to Run

1. Clone the repository:

   ```bash
   git clone https://github.com/yourusername/financial-etl-pipeline.git
   cd financial-etl-pipeline
   ```

2. Set up your Python environment:

    ```bash
    pip install -r oop_capstone/requirements.txt
    ```

3. Run the CLI (OOP version):
    ```bash
    python oop_capstone/main.py

    ```
4. For the functional prototype:
    ```bash
    python functional_capstone/main.py
    ```

# 📌 Conclusion
This project demonstrates the full lifecycle of data engineering tasks, from raw data extraction to interactive visualization. By offering both functional and object-oriented approaches, it provides a valuable comparison of software engineering paradigms in ETL development.

Whether you're testing your pipeline locally or visualizing trends through Tableau, this project lays a scalable foundation for real-world financial data processing.

🛠 Developed as part of the Per Scholas Data Engineering curriculum.