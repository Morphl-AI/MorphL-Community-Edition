# MorphL Model for Predicting Churning Users for Publishers (Google Analytics 360 & BigQuery)

## Introduction

Large websites from the publishing industry use [Google Analytics 360](https://marketingplatform.google.com/about/analytics-360/) to track their users. Google Analytics 360 reports are useful for analyzing trends in the overall traffic and optimizing conversion rates. At the same time, the abundance of aggregated data makes it difficult to identify patterns in user behaviour, even by experienced marketers.

Using Google Cloud Platform, it is possible to connect [BigQuery](https://cloud.google.com/bigquery/) to Google Analytics 360 and retrieve data into BigQuery tables. Based on this data, we can implement various use cases, such as churning visitors.

## Problem Setting

Having access to granular data, **we can predict when a user (client ID) is going to churn**. We have defined churned users as previously retained users that do not return to the website before a time interval (threshold) has passed. **By retained users**, we mean users that have visited the website at least twice in the past (they have at least 2 sessions).

We should clarify that the **Client ID refers to a browser**, not to a user account, thus it doesn't contain any personal data. It is possible to associate the Client ID with a user account (across devices), however in this particular use case, all client ids refer to browsers.

The data exported from Google Analytics 360 into BigQuery consists of sessions data.

## Prerequisites

Please see here a tutorial about [connecting BigQuery to Google Analytics 360](https://support.google.com/analytics/answer/3416092?hl=en). This project assumes that this step has already been implemented and that the data has been imported into a BigQuery dataset.

Additional setup steps are also required for providing access to BigQuery & allowing data retrieval. Please see more details [here](bq_extractor).

## Features and Data Labeling

The most relevant data related to a users history we can obtain from Google Analytics includes:

- Sessions (total sessions for each user, in a time interval);
- Bounces
- Events
- Session duration
- Pageviews
- Device Category (mobile, desktop or tablet)
- Days since last session (used only for training the model)

From the Google Analytics data, we can calculate `Days since last session` as the difference between the most recent session date and the end of our training interval. The duration of the training and predictions intervals can differ and the training / prediction windows do not overlap.

For predicting churn, we have labeled the users as churned / not churned by:

- Calculating the average time between sessions of retained users (`Avg. days between sessions`).
- Label the data. If a user has a value of `Days Since Last Session > mean(Avg. days between sessions)`, he is labeled as churned (`Churned` = 0 or 1).
- `Days since last session` and `Avg. days between sessions` will not be included as features in the training set, as they are heavily correlated with the label `Churned.`

## Pipelines Architecture

This repository contains the code for the churned users pipelines, including model training and predictions. The code runs on the [MorphL Platform Orchestrator](https://github.com/Morphl-AI/MorphL-Orchestrator) which creates 2 pipelines: **Training Pipeline** and **Prediction Pipeline**.

Both pipelines require a **BigQuery extractor** to retrieve data from BigQuery, in `.avro` format. For each pipeline, a different query format is used (see `training/query.sql.template` and `prediction/query.sql.template`).

### Training Pipeline

All components from this pipeline are run on a weekly basis.

#### 1. Pre-processor for formatting data

It is implemented using PySpark and it is responsible for processing the data retrieved from Big Query (`.avro` files) and saving it into Cassandra tables. It also labels the data.

#### 2. Pre-processor for transforming data

Applies data transformations such as power transforms and feature scaling. This pre-processor is also used by the prediction pipeline.

It returns a Dask dataframe.

#### 3. Model generator

Takes a Dask dataframe on initialization. It will train and save the model as a .h5 file, together with a json file which includes the model scores.

For training the model we have used Keras / TensorFlow.

### Prediction Pipeline

#### 1. Pre-processors for formatting and transforming data

Uses the same pre-processors (PySpark and Dask) as the training pipeline, but in "prediction" mode. The same process is applied: formatting the data, followed by power transforms and feature scaling. As a difference, in "prediction" mode, the data is not labeled.

#### 2. Batch inference

It is used for making predictions and saving them in the Cassandra database.

#### 3. Endpoint

After the prediction pipeline is triggered, predictions can be accessed at an endpoint. See Wiki for details.
