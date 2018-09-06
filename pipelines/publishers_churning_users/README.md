# MorphL Model for Predicting Churning Users for Publishers

## Introduction

A lot of websites from the publishing industry use Google Analytics to track their users. Google Analytics reports are useful for analyzing trends in the overall traffic and optimizing conversion rates. At the same time, the abundance of aggregated data makes it difficult to identify patterns in user behaviour, even by experienced marketers.

By default, Google Analytics includes a series of reports, for example viewing a total of users and sessions from a particular date interval.

The free version of the Google Analytics Reporting API v4 doesn't export any client ids from the **User Explorer report**. However, it is possible to make these available by creating a custom dimension with the same value as a Client ID, a process we have [documented on our Github account](https://github.com/Morphl-AI/MorphL-Collectors-Requirements/tree/master/google-analytics). This allows the analytics API to export data at the Client ID, Session or Hit level, instead of returning only aggregated data.

We should clarify that the **Client ID refers to a browser**, not to a user account, thus it doesn't contain any personal data. It is possible to associate the Client ID with a user account (across devices), however in this particular use case, all client ids refer to browsers.

## Problem Setting

Having access to granular data, **we can predict when a user is going to churn**. We have defined churned users as previously retained users that do not return to the website before a time interval (threshold) has passed. **By retained users**, we mean users that have visited the website at least twice in the past (they have at least 2 sessions).

Our training sets are going to aggregate session and hit data at the user level.

## Features and Data Labeling

The most relevant data related to a users history we can obtain from the [Google Analytics API v4](https://developers.google.com/analytics/devguides/reporting/core/dimsmets) includes:

- Sessions (total sessions for each user, in a time interval);
- Session duration (total sessions duration for each user, in a time interval);
- Avg. session duration
- Entrances
- Bounces
- Pageviews
- Unique pageviews
- Screen Views
- Page value
- Exits
- Time on Page
- Avg. Time on Page
- Page Load Time (ms)
- Avg. Page Load Time (sec)
- Days since last session;
- Count of sessions (total number of sessions for the user, independent of the selected time interval)
- Hits (total hits for each user, in a time interval);
- Device Category (mobile, desktop or tablet)

For predicting churn, we have labeled the users as churned / not churned by:

- Calculating the average time between sessions of retained users (`Avg. days between sessions`).
- Label the data. If a user has a value of `Days Since Last Session > mean(Avg. days between sessions)`, he is labeled as churned (`Churned` = 0 or 1).
- `Days since last session` and `Avg. days between sessions` will not be included as features in the training set, as they are heavily correlated with the label `Churned.`

The model can be improved by predicting future churned users (users that are currently not churned, but will churn in the future).

## Pipelines Architecture

This repository contains the code for the churned users pipelines, including model training and predictions. The code runs on the [MorphL Platform Orchestrator](https://github.com/Morphl-AI/MorphL-Orchestrator) which creates 3 pipelines: **Ingestion Pipeline**, **Training Pipeline** and **Prediction Pipeline**.

### Ingestion Pipeline

#### 1. Google Analytics Connector

It is responsible for authenticating to the Google Analytics API v4 using a service account and retrieving data. See the **Features and Data Labeling** section for a complete list of Google Analytics dimensions and metrics. The Google Analytics data is saved in Cassandra tables.

The connector runs daily and it can also be used to retrieve historical data (for backfilling).

### Training Pipeline

All components from this pipeline are run on a weekly basis.

#### 1. Pre-processor for formatting data

It is implemented using PySpark and it is responsible for processing the data retrieved from the Google Analytics API. It reads the data (in JSON format) and transforms it into SQL-like Cassandra tables. It also labels the data.

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

After the prediction pipeline is triggered, predictions can be accessed at an endpoint. See the MorphL Platform Orchestrator for details.
