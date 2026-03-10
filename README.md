# LLM-Powered Hepsiburada URL Classification Pipeline

This project implements a **web crawling and classification pipeline for e-commerce product URLs** from Hepsiburada.  
The system collects product links using a crawler and classifies them using **regex-based filtering and Large Language Models (OpenAI API).**

The goal of the project is to build a **structured dataset of categorized e-commerce URLs** for further data analysis and product categorization.

---

## Features

- Web crawling of product URLs and metadata
- Automated URL classification
- Hybrid classification approach:
  - Regex-based filtering
  - LLM-based semantic classification
- Structured data storage
- Scalable pipeline for e-commerce data analysis

---

## Technologies Used

- Python
- Scrapy
- OpenAI API (Large Language Models)
- PostgreSQL
- Regex
- YAML
- Git

---

## System Architecture

The pipeline consists of the following components:

1. **Web Crawler**
   - Scrapy-based crawler collects product URLs and metadata.

2. **Data Processing**
   - URLs are normalized and filtered using regex rules.

3. **LLM Classification**
   - OpenAI API is used to classify URLs into product categories.

4. **Data Storage**
   - Processed data is stored in PostgreSQL for further analysis.
## Use Cases

- E-commerce product categorization
- Market research
- Product dataset creation
- Data pipelines for recommendation systems

---

## Author

**Emre Peker**  
Computer Engineering Student  
Istanbul Bilgi University
