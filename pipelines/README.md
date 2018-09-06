# MorphL Pipelines / Models

At MorphL, we follow a process when adding new models. We start by creating a Proof of Concept (using various Python scripts and Colab / Jupyter), which allows us to iterate quickly and optimize the model. When we are happy with the results, we implement the pipelines for the model and integrate it into the MorphL architecture.

## Creating a Successful Proof of Concept

### Gathering data

Depending on the mobile/web application's traffic, you’ll need to wait for the data to collect for a few weeks or 1 to 3 months. If you need to wait more than that to get to a few hundred thousands records, you might not have enough data to begin with, at which point your problem is not ML, you have to look somewhere else.

### Preparing that data

Once you have enough data to work with, at least for a PoC, we need to load it into a suitable place and prepare it for use in our machine learning algorithm.

In our case, we started by exporting data from Google Analytics. We used various visualization tools (such as Google Data Studio), connected them to the Google Analytics Reporting API v4 and simply exported the dimensions and metrics into CSV files.

We then pre-processed the data (deduping, randomization, normalization, error correction and more).

### Choosing a model

It's important to setup a baseline to improve from. As an example, for one of our usecases (predicting churning users for publishers), we implemented logistic regression (first with scikit-learn, before switching to Keras / TensorFlow).

We got our initial accuracy (0.83) and loss (0.42) and these are the numbers that we have to further optimize by trying out different models, playing with the features or even considering adding more data into the mix.

### Training & Evaluation (& Testing)

A good rule of thumb to use for a training-evaluation split somewhere on the order of 80/20, 70/30 or 60/20/20 if we consider testing.

### Parameter tuning

On the same training set, Keras gave better results, so we continued the process by trying different optimizers, loss functions and tweaking the hyperparameters.

Without going into too much technical details, adjustment or tuning is a heavily experimental process that depends on the specifics of the training set and model.

### Prediction

This is the step where we get to answer some questions. In the case of churn prediction, we can finally use our model to ask whether a given user is going to churn or not.
