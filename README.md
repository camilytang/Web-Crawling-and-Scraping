# Carlist.my High-Performance Web Scraper

A high-performance distributed web crawling and data processing system for Carlist.my, Malaysia's largest automotive marketplace. This project was developed for SECP3133 High Performance Data Processing course at Universiti Teknologi Malaysia (UTM).

## Project Overview

This system extracts real-time vehicle listing data from Carlist.my and processes it using multiple data processing libraries to compare performance optimization techniques.

- **Total Records Collected:** 174,150 vehicle listings
- **Data Processing Libraries:** Pandas, Polars, Modin, Dask
- **Web Scraping Tools:** httpx, urllib, requests, parsel, Selectolax, BeautifulSoup

## Team Members & Library Usage

| Team Member | Role | Web Crawler | Web Parser | Data Processing |
|-------------|------|-------------|------------|-----------------|
| Camily Tang Jia Lei | Team Leader | urllib | BeautifulSoup | Pandas |
| Marcus Joey Sayner | Developer | httpx | parsel | Polars |
| Muhammad Luqman Hakim | Developer | urllib | Selectolax | Modin |
| Goh Jing Yang | Developer | requests | BeautifulSoup | Dask |

## Data Extracted

The system extracts the following vehicle attributes:

- Car Name, Brand, Model
- Manufacture Year
- Body Type, Fuel Type
- Mileage, Transmission, Color
- Price (RM), Installment (RM)
- Condition (New/Used/Reconditioned)
- Seat Capacity
- Location, Sales Channel

## Performance Optimization Techniques

| Library | Optimization Method |
|---------|---------------------|
| Polars | Multi-threading, Rust backend |
| Modin | Parallel execution with Dask |
| Dask | Task-based parallelism, lazy evaluation |
| Pandas | Baseline (single-threaded) |

## Performance Metrics Measured

- Elapsed Time (Wall Time)
- CPU Time & CPU Usage Percentage
- Current & Peak Memory Usage
- Throughput (rows/second)

## Results Summary

| Library | Best For | Throughput |
|---------|----------|------------|
| Polars | Large in-memory datasets | 150M rows/sec |
| Pandas | Small to medium datasets | 120M rows/sec |
| Modin | Parallel scaling | 30-40M rows/sec |
| Dask | Out-of-core data | Varies |
