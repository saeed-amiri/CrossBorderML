# CrossBorderML

## Project Structure

```
CrossBorderML/
    ├── data
    ├── notebooks
    ├── output_examples
    ├── scripts
    ├── sql
    │   └── queries
    ├── src
    │   └── crossborderml
    │       └── conf
    │           └── pathes.yaml
    └── tests
```

###  Folder Descriptions

* **`src/crossborderml/`**: Main Python package containing all core modules for data loading, cleaning, modeling, and visualization.
* **`sql/`**: Contains queries into data.
* **`scripts/`**: Standalone Python scripts for running parts of the project pipeline (e.g., training, evaluation, or data import).
* **`data/`**: Directory for storing raw and processed datasets. Subfolders can help keep stages of the data pipeline organized.
* **`notebooks/`**: Jupyter notebooks used for exploratory data analysis (EDA), experiments, or prototyping.
* **`outputs/`**: Final results such as plots, reports, saved models, or evaluation metrics.
* **`tests/`**: Unit tests and validation scripts for ensuring code reliability and correctness.
* **`requirements.txt`**: List of Python dependencies for the project.
* **`pyproject.toml`**: Optional modern build system configuration for packaging and tool management.
* **`README.md`**: Project overview and documentation (this file).
