import datetime
from sys import argv, exit

def get_record(i, num_days_ago, ref_dt):
    dt = ref_dt - datetime.timedelta(days=num_days_ago)
    one_day_prior = ref_dt - datetime.timedelta(days=num_days_ago+1)
    return (i, {'days_worth_of_data_to_load': str(num_days_ago),
                'asYYYY-MM-DD': dt.strftime('%Y-%m-%d'),
                'as_py_code': one_day_prior.__repr__()})

OPTIONS = [5, 10, 30, 60, 120, 180, 270, 365]
opt_len = len(OPTIONS)
valid_inputs = set([str(i+1) for i in range(opt_len)])
n = datetime.datetime.now()
tomorrow = n + datetime.timedelta(days=1)
lookup_dict = \
    dict([get_record(i + 1, num_days_ago, n) for (i, num_days_ago) in enumerate(OPTIONS)])
for _ in range(5):
    print('')
print('How much historical data should be loaded?\n')
for (j, num_days_ago) in enumerate(OPTIONS):
    choice = j + 1
    print('{}) {} - present time ({} days worth of data)'.format(
        choice,
        lookup_dict[choice]['asYYYY-MM-DD'],
        num_days_ago))
print('')
entered_choice = input('Select one of the numerical options 1 thru {}: '.format(opt_len))
print('')
if entered_choice in valid_inputs:
    choice = int(entered_choice)
    with open(argv[1], 'w') as fh1:
        fh1.write(lookup_dict[choice]['as_py_code'])
    with open(argv[2], 'w') as fh2:
        fh2.write(tomorrow.__repr__())
    with open(argv[3], 'w') as fh3:
        fh3.write(lookup_dict[choice]['days_worth_of_data_to_load'])
else:
    print('No valid choice was selected, aborting.')
    print('')
    exit(1)

