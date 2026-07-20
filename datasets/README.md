# Dataset

This project uses the **Olist Brazilian E-Commerce Dataset** for evaluating the automatic index creation mechanism.

## Dataset Source

The dataset is publicly available on Kaggle:

https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

## Required Files

Download the dataset and extract the CSV files into this directory.

The project uses the following datasets:

- olist_customers_dataset.csv
- olist_orders_dataset.csv
- olist_order_items_dataset.csv
- olist_products_dataset.csv
- olist_order_reviews_dataset.csv

## Why the Dataset Is Not Included

The dataset is publicly available and can be downloaded directly from Kaggle. To keep this repository lightweight and avoid redistributing third-party data, the CSV files are not included in this repository.

After downloading, place the required CSV files inside this `dataset/` directory before loading them into PostgreSQL.