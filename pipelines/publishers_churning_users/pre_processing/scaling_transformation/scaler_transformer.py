import dask.dataframe as dd
import numpy as np
from os import getenv
from sklearn.externals import joblib
from sklearn.preprocessing.data import PowerTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, Normalizer
from sklearn.impute import SimpleImputer


class ScalerTransformer:
    """
    This class scales and applies multiple transformations to the labeled data from the dask dataframe object it is initialized with
    and returns a dataframe with the modified data.

    The passed dataframe should have the labels specified in the __init__ method of the class. Any other labels will be ignored and will not
    be present in the returned dataframe.

    Attributes:
        num_labels: The labels of the numeric columns, used to determine the type of transformation to apply.
        gauss_labels: The labels of the columns which represent amounts of time, used to determine which columns to logarithmize.
        cat_labels: The labels for categorical data.
        dask_df: The dataframe that the class is initialized with. Must be a Dask type dataframe.
        day_as_str: Environment variable that contains the day of the last training as a string.
        unique_hash: Environment variable that contains a hash generated when the data is processed for training. This helps us distinguish between transformations that occured
                    on the same day.
        training_or_prediction: Environment variable that contains the string "training" or the string "prediction" depending on whether the data
                                is being processed for training or inference.
        models_dir: Environment variable that contains the path to the models directory.
    """

    def __init__(self, dask_df):
        """Inits ScalerTransformer with the given dask dataframe, labels and environment variables."""
        self.num_labels = ['pageviews', 'unique_pageviews',
                           'u_sessions', 'entrances', 'bounces', 'exits', 'session_count']
        self.gauss_labels = ['session_duration', 'time_on_page']
        self.cat_labels = ['is_desktop', 'is_mobile', 'is_tablet']
        self.dask_df = dask_df
        self.day_as_str = getenv('DAY_AS_STR')
        self.unique_hash = getenv('UNIQUE_HASH')
        self.training_or_prediction = getenv('TRAINING_OR_PREDICTION')
        self.models_dir = getenv('MODELS_DIR')

    def get_transformed_numeric_data(self):
        """Transforms the numeric data from the dask dataframe contained in 'self.dask_dataframe', selected based on the contents of 'self.num_labels'.

        Returns:
            A dataframe with the scaled and transformed columns.
        """
        updated_data_bc = {}

        # Iterate through the numeric labels.
        for column in self.num_labels:
            # For each column, add 1 to shift data to right and avoid zeros.
            # We need to call 'computed()' on the column so that we can retrieve its values and apply 'reshape()'.
            data_in_column = self.dask_df[column]
            data = data_in_column.compute().values.reshape(-1, 1) + 1

            # For each column, compose the path and name of the file which holds the
            # 'PowerTransformer' object with the fitted lambdas using the model directory,
            # the day of the last training (current day if we are preprocessing for training) and a unique hash.
            pkl_file = f'{self.models_dir}/{self.day_as_str}_{self.unique_hash}_ga_chp_box_cox_{column}.pkl'

            # If predicting load the specific 'PowerTransformer' object for this column and apply the transformation.
            if self.training_or_prediction == 'prediction':
                box_cox = joblib.load(pkl_file)
                data_bc = box_cox.transform(data)
            # If training, fit the 'PowerTransformer' and save the object in the specified column's file then apply the transformation.
            else:
                # Create a 'PowerTransformer' object using the 'box-cox' method.
                box_cox = PowerTransformer(method='box-cox')
                box_cox.fit(data)
                joblib.dump(box_cox, pkl_file)
                data_bc = box_cox.transform(data)

            updated_data_bc[column] = data_bc.T.tolist()[0]

        # Append all the columns to an array and generate a dask dataframe from it with the data
        # transformed using Box-Cox.
        bc_list = []

        for column in self.num_labels:
            bc_list.append(updated_data_bc[column])

        bc_array = np.array(bc_list).transpose()

        transformed_bc_data = dd.from_array(
            bc_array, chunksize=200000, columns=self.num_labels)

        # Generate a similar .pkl file name and path for the 'Pipeline' type object with the fitted hyperparameters.
        pkl_file = f'{self.models_dir}/{self.day_as_str}_{self.unique_hash}_ga_chp_pipeline.pkl'

        # If predicting, load the pipeline and use it to transform the data.
        if self.training_or_prediction == 'prediction':
            pipeline = joblib.load(pkl_file)
            transformed_numeric = pipeline.transform(transformed_bc_data)
        else:
            # If training, generate a 'Pipeline' using a 'SimpleImputer', 'Normalizer' and 'StandardScaler'.
            pipeline = Pipeline([
                # Replace zeros with mean value.
                ('imputer', SimpleImputer(strategy="mean", missing_values=0)),
                # Scale in interval (0, 1).
                ('normalizer', Normalizer()),
                # Substract mean and divide by variance.
                ('scaler', StandardScaler()),
            ])
            # Fit the pipeline and save it to the specified file then apply the transformation.
            pipeline.fit(transformed_bc_data)
            joblib.dump(pipeline, pkl_file)
            transformed_numeric = pipeline.transform(transformed_bc_data)

        return dd.from_array(transformed_numeric, chunksize=200000, columns=self.num_labels)

    def get_transformed_gauss_data(self):
        """Applies the natural logarithm of 1 plus the value for the time related columns.

        Returns:
            A dataframe with the transformed time data.
        """

        # Get the time columns.
        logged_data = self.dask_df[self.gauss_labels]

        # Transform the data for each of the columns.
        for column in self.gauss_labels:
            logged_data[column] = np.log1p(self.dask_df[column])

        logged_data_array = np.array(logged_data)
        return dd.from_array(logged_data_array, chunksize=200000, columns=self.gauss_labels)

    def get_churned_data(self):
        """Slices the 'churned' column from the dataframe and returns it.

        Returns:
            A dask dataframe with the 'churned' column.
        """
        churned_data_array = np.array(self.dask_df['churned'])
        return dd.from_array(churned_data_array, chunksize=200000, columns=['churned'])

    def get_cat_data(self):
        """Slices the categorical columns from the dask dataframe and returns them.

        Returns:
            A dask dataframe with the categorical columns.
        """
        cat_data_array = np.array(self.dask_df[self.cat_labels])
        return dd.from_array(cat_data_array, chunksize=200000, columns=self.cat_labels)

    def get_client_id_data(self):
        """Slices the 'client_id' column from the dask dataframe and returns it.

        Returns:
            A dask dataframe with the 'client_id' column.
        """
        client_id_data_array = np.array(self.dask_df['client_id'])
        return dd.from_array(client_id_data_array, chunksize=200000, columns=['client_id'])

    def get_transformed_data(self):
        """Calls all the methods to transform the data then concatenates the dataframes.

        Returns:
            A dask dataframe with all the transformed data.
        """
        # The list of dataframes that need to be concatenated.
        concat_list = []

        # Only add the 'client_id' column if we are predicting because we need it for identification.
        if self.training_or_prediction == 'prediction':
            concat_list.append(self.get_client_id_data())

        concat_list.append(self.get_transformed_numeric_data())
        concat_list.append(self.get_transformed_gauss_data())
        concat_list.append(self.get_cat_data())

        # Only add the 'churned' column if we are training because it is the output column for our model.
        if self.training_or_prediction == 'training':
            concat_list.append(self.get_churned_data())

        return dd.concat(concat_list, axis=1)
