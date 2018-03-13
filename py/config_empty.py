import os
import datetime

key = ''
today = datetime.datetime.now()
cutoff_date =  datetime.datetime(2016,1,1)
root_dir = ''  # where is data being stored
input_file = '' # where are the channel IDS coming from.
IS_DEV = False
IS_HPC = False
