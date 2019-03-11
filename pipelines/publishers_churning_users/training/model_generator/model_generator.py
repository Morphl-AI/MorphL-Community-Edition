from os import getenv
from sklearn.model_selection import train_test_split
from keras.optimizers import RMSprop
from keras.models import Sequential
from keras.layers import Dense
import json


class ModelGenerator:
    """
    This class initializes with a dask dataframe, trains a Binary Classifier model on it and saves it into a .h5 file on the disk along with the evaluation scores
    which are saved in a .json file.

    Attributes:
        day_as_str: Environment variable that contains the current day as a string.
        unique_hash: Environment variable that contains a hash generated when the data is processed for training. This helps us distinguish between transformations that occured
                    on the same day.
        models_dir: Environment variable that contains the path to the models directory.
        train_set: 60% of the randomly split dataframe. Used for training.
        validation_set: 20% of the randomly split dataframe. Used for validation.
        test_set: 20% of the randomly split dataframe. Used for testing.
    """

    def __init__(self, dask_df):
        """Inits ModelGenerator with the given dask dataframe and environment variables,
        then splits the dataframe into testing, training and validation sets.

        """
        self.day_as_str = getenv('DAY_AS_STR')
        self.unique_hash = getenv('UNIQUE_HASH')
        self.models_dir = getenv('MODELS_DIR')

        train_validation_set, self.test_set = dask_df.random_split(
            [0.8, 0.2], random_state=42)
        self.train_set, self.validation_set = train_validation_set.random_split(
            [0.8, 0.2], random_state=42)

    def get_XY_train_test_validation_sets(self):
        """Separates the output column 'churned' from the training, validation and test sets.

        Returns:
            A dict with the input sets (X) and output sets (Y). For example:
            {
                'train_X': dask.dataframe,
                'train_Y': dask.dataframe,
                'validation_X': dask.dataframe,
                'validation_Y': dask.dataframe,
                'test_X': dask.dataframe,
                'test_Y': dask.dataframe
            }
        """
        sets = {}

        # All sets are computed so we can operate on them. The output label 'churned
        # is dropped and placed into a separate set.
        sets['train_X'] = self.train_set.drop('churned', axis=1).compute()
        sets['train_Y'] = self.train_set['churned'].copy().compute()

        sets['validation_X'] = self.validation_set.drop(
            'churned', axis=1).compute()
        sets['validation_Y'] = self.validation_set['churned'].copy().compute()

        sets['test_X'] = self.test_set.drop('churned', axis=1).compute()
        sets['test_Y'] = self.test_set['churned'].copy().compute()

        return sets

    def generate_and_save_model(self):
        """Generates, trains and evaluates a Keras Sequential model with one layer and saves it to the disk."""

        # Initialize the model and get the training, test and validation sets by calling 'get_XY_train_test_validation_sets()'
        model = Sequential()
        sets = self.get_XY_train_test_validation_sets()

        # Determine the number of input variables.
        input_dim = len(sets['test_X'].columns)

        # Add a layer to the model with a sigmoid activation.
        model.add(Dense(1, input_dim=input_dim, activation='sigmoid'))

        # Initialize an RMSprop optimizer.
        rmsprop = RMSprop(
            lr=0.001, rho=0.9, epsilon=None, decay=0.0)

        # Configure the model for training, specifing the loss function as binary crossentropy
        # and the metric as accuracy.
        model.compile(optimizer=rmsprop, loss='binary_crossentropy',
                      metrics=['accuracy'])

        # Train the model using the training and validation sets.
        model.fit(sets['train_X'], sets['train_Y'], epochs=50, verbose=0,
                  validation_data=(sets['validation_X'], sets['validation_Y']))

        # Evaluate the model using the test set.
        score = model.evaluate(sets['test_X'], sets['test_Y'], verbose=0)

        scores = {'loss': score[0], 'accuracy': score[1]}

        # Save the evaluation scores to a .json file who's name and path are made up of 'day_as_str', 'unique_hash' and 'model_dir' respectively.
        churn_scores_json_file = f'{self.models_dir}/{self.day_as_str}_{self.unique_hash}_ga_chp_churn_scores.json'
        with open(churn_scores_json_file, 'w') as writer:
            writer.write(json.dumps(scores))

        # Save the model in a similar way.
        churn_model_file = f'{self.models_dir}/{self.day_as_str}_{self.unique_hash}_ga_chp_churn_model.h5'
        model.save(churn_model_file)
