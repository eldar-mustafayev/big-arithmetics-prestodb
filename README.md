# Ethereum Historical Token Balance Calculation SQL Query

![GitHub last commit](https://img.shields.io/github/last-commit/eldar-mustafayev/big-arithmetics-prestodb)
![GitHub stars](https://img.shields.io/github/stars/eldar-mustafayev/big-arithmetics-prestodb?style=social)

## Overview

This repository contains a complex SQL query that utilizes Arbitrary-precision arithmetic to cumulatively sum and subtract 256-bit integers on Ethereum transaction data. The query is designed to calculate the historical token balance of each Ethereum address using PrestoDB.

The Ethereum transaction data required for this query can be obtained using the [Ethereum ETL tool](https://github.com/blockchain-etl/ethereum-etl), which efficiently extracts blockchain data and stores it in a database for analysis.

## Table of Contents

- [Overview](#overview)
- [Query Description](#query-description)
- [Requirements](#requirements)
- [Setup](#setup)
- [Usage](#usage)


## Query Description

The SQL query employs Arbitrary-precision arithmetic to accurately perform cumulative addition and subtraction of 256-bit integers on Ethereum transaction data. By leveraging PrestoDB, it efficiently processes token transfers to calculate the token balance of each Ethereum address at different points in time.

## Requirements

- PrestoDB: The query is designed to run on a PrestoDB cluster or Amazon Athena.
- Ethereum ETL Tool: Users must first use the Ethereum ETL tool to extract and load historical Ethereum transaction data into the PrestoDB database.

## Setup

To set up the project and run the SQL query, follow these steps:

1. Setup Amazon Athena or connect to an existing PrestoDB cluster.

2. Use the [Ethereum ETL tool](https://github.com/blockchain-etl/ethereum-etl) to extract and load historical Ethereum transaction data into the PrestoDB database.

3. Ensure that the historical Ethereum transaction data is available and up-to-date in the PrestoDB database.


## Usage

1. Open your PrestoDB client or interface.

2. Copy the contents of the `calculate_historic_balance.sql` file into the query editor.

3. Execute the SQL query to calculate the historical token balance of each Ethereum address.
