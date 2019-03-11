import datetime
from sys import argv, exit

OPTIONS = [5, 10, 30, 60, 90, 120, 180, 270, 365]
opt_len = len(OPTIONS)
valid_inputs = set([str(i+1) for i in range(opt_len)])


def display_options(interval_type='training'):
    print('How many days should we use for ' + interval_type.upper() + '? \n')
    for (j, num_days) in enumerate(OPTIONS):
        choice = j + 1
        print('{}) {} days'.format(choice, num_days))

    print('')
    interval = input(
        'Select one of the numerical options 1 thru {}: '.format(opt_len))
    print('')

    if interval not in valid_inputs:
        print('No valid choice was selected, aborting.')
        print('')
        exit(1)

    return OPTIONS[int(interval) - 1]


# Read duration for the training and the predictions intervals
training_interval = display_options('training')
predictions_interval = display_options('predictions')

now = datetime.datetime.now()

# Display training & prediction windows dates
date_to_p = now - datetime.timedelta(days=1)
date_from_p = date_to_p - \
    datetime.timedelta(days=predictions_interval-1)

date_to_t = now - datetime.timedelta(days=predictions_interval + 1)
date_from_t = date_to_t - \
    datetime.timedelta(days=training_interval-1)

print('Initial training window: {} - {} ({} days)'.format(
    date_from_t.strftime('%Y-%m-%d'),
    date_to_t.strftime('%Y-%m-%d'),
    training_interval))

print('Initial predictions window: {} - {} ({} days)'.format(
    date_from_p.strftime('%Y-%m-%d'),
    date_to_p.strftime('%Y-%m-%d'),
    predictions_interval))


# Set the training interval
with open(argv[1], 'w') as fh1:
    fh1.write(str(training_interval))

# Set the predictions interval
with open(argv[2], 'w') as fh2:
    fh2.write(str(predictions_interval))

# Set the today's date as py code
with open(argv[3], 'w') as fh3:
    now = datetime.datetime.now()
    fh3.write(now.__repr__())
