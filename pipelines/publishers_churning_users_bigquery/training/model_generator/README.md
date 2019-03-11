# Model Generator for Predicting Churning Users for Publishers (Google Analytics 360 & BigQuery)

## Purpose

The purpose of this class is to take a dask dataframe on initialization, train a model and save it to the disk as .h5 file, evaluate the model and save its scores in a .json file.

## Usage

Make sure the following environment variables are set:

    - `DAY_AS_STR`: the current day as a string.
    - `UNIQUE_HASH`: a unique hash that will be attributed to the model and scores files.
    - `MODELS_DIR`: the models directory.

Initialize a `ModelGenerator` object with a dask dataframe, make sure the labels are correct and the `churned` column is present.

Call the `ModelGenerator` object's `generate_and_save_model()` method.

## Notes

If the warning: "FutureWarning: Conversion of the second argument of issubdtype from float to np.floating is deprecated." is encountered, upgrade the "h5.py" package to version number 2.8.0 by running: "conda update h5py".
