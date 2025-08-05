# -*- coding: utf-8 -*-
'''
##################################
It's a real knife fight in Malibu!
##################################
'''

##################################
### - <tool name>      - ###
### - Author: <name>   - ###
### - Creation Date: ddMMMYY - ###
##################################

#
# 
#

######################################
### --- Import All the Things! --- ###
######################################

import pandas as pd
import numpy as np
import os
import fnmatch
import timeit
import sys
import datetime
#import hashlib

#import time
'''
from facebook_business.adobjects.serverside.action_source import ActionSource
#from facebook_business.adobjects.serverside.content import Content
from facebook_business.adobjects.serverside.custom_data import CustomData
#from facebook_business.adobjects.serverside.delivery_category import DeliveryCategory
from facebook_business.adobjects.serverside.event import Event
from facebook_business.adobjects.serverside.event_request import EventRequest
from facebook_business.adobjects.serverside.user_data import UserData
from facebook_business.api import FacebookAdsApi
'''
#import logging
#from tkinter import Tk, Label, Frame, Entry, Button, LabelFrame, Radiobutton, Text, Scrollbar, IntVar, Checkbutton, Menu, Variable, Toplevel
#from tkinter import filedialog as fd
#from threading import Thread
import warnings
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)
pd.options.mode.chained_assignment = None
import logging
import logging.handlers

#from hubspot import HubSpot
#from hubspot.crm.contacts import PublicObjectSearchRequest

global client
client = 'LANT'
global tool
tool = 'grow_database'
global log_time
log_time = pd.Timestamp('now').value
global log_file
log_file = f'.\log\{client}_{tool}_{log_time}.log'


def logging_setup():
#--- logging module setup
    # Change root logger level from WARNING (default) to NOTSET in order for all messages to be delegated.
    logging.getLogger().setLevel(logging.NOTSET)
    
    # Add stdout handler, with level CRITICAL
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.CRITICAL)
    formater = logging.Formatter('%(name)-13s: %(levelname)-8s %(message)s')
    console.setFormatter(formater)
    logging.getLogger().addHandler(console)
    
    # Add file rotating handler, with level INFO
    rotatingHandler = logging.handlers.RotatingFileHandler(filename=log_file)
    rotatingHandler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    rotatingHandler.setFormatter(formatter)
    logging.getLogger().addHandler(rotatingHandler)
    global logger
    logger = logging.getLogger(__name__)
    #/-- logging

global cwd
cwd = os.getcwd()

#######################
### --- /Import --- ###
#######################

###########################
### --- Definitions --- ###
###########################

def read_variables(cwd, path):    
    docustring('Reading variables...','Reading in variables file')
    os.chdir(cwd)
    for file in os.listdir(os.getcwd()):
        if fnmatch.fnmatch(file, path):
            variables = pd.read_excel(file)    
    return(variables)


def docustring(*argv):
    i = 0
    for arg in argv:
        if i ==0:
            print(arg)
            i+=1
    for arg in argv:
        logger.info(arg)
    return



def write_out(df):

    docustring('Writing output...','Write out leads, contacts, and opps')
    output_path = variables['Output File'][0]
    docustring('Writng output to {}'.format(output_path),'Write output')
    #--- csv and/or xlsx output
    df.to_csv(output_path, index=False)
    #with pd.ExcelWriter(output_path, engine='xlsxwriter', engine_kwargs={'options':{'strings_to_urls': False}}) as writer:
    #    leads_out.to_excel(writer, sheet_name='Leads', index=False)
    #    contacts_out.to_excel(writer, sheet_name='Contacts', index=False)
    #    opps_out.to_excel(writer, sheet_name='Opps', index=False)
        
    return

def report_input(path):
    docustring('Reading in {}'.format(path), 'Read input report')
    if 'csv' in path:
        df = pd.read_csv(path, low_memory=False, on_bad_lines='skip')
    else:
        df = pd.read_excel(path, sheet_name='Data Planet Report')
    return(df)

def dict_maker(df, index_name, map_to):
    df_table = df[[index_name, map_to]].dropna()
    df_dict = df_table.set_index(index_name)[map_to].to_dict()
    return(df_dict)

def main():
    start_time = timeit.default_timer()
    docustring('Running LANT Grow Database tool...','Running main method')

    cr_path = variables['Input Files'][0]
    hub_path = variables['Input Files'][1]
    output_cols = variables['Output Cols'].dropna().tolist()
    
    cr = report_input(cr_path)
    hub = report_input(hub_path)

    #filter down to paid categories
    paid_cats = variables['Paid Categories'].dropna().tolist()
    cr = cr.loc[cr['Category'].isin(paid_cats)]
    
    #filter down to valuable actions
    action_list = variables['Valuable Actions'].dropna().tolist()
    cr['valueable'] = cr[action_list].sum(axis=1)
    cr = cr.loc[cr['valueable'] > 0]    
    
    #create Action Taken column
    action_list.reverse()
    for action in action_list:
        cr.loc[cr[action]==1, 'Action Taken'] = action
    
    emails = cr[cr.columns[cr.columns.to_series().str.contains('email')]]
    emails = emails.replace('nan','')
    emails.loc[:,'combined'] = emails.apply(lambda x: ''.join(x.dropna()), axis=1)
    cr.loc[:,'Email_Address'] = emails['combined'].astype(str)
    cr['Email_Address'] = cr['Email_Address'].str.lower()
    hub['Email_Address'] = hub['Email'].str.lower()
    
    docustring('Joining CR and Hubspot reports...', 'Merge CR and Hub reports')
    combined = pd.merge(cr, hub, how='left', on='Email_Address')
    combined.rename(columns={'action_timestamp':'Action Time', 'Segment':'Keyword', 'Record ID':'Hubspot ID'}, inplace=True)
    
    network_dict = dict_maker(variables, 'Paid Categories', 'Paid Network')
    combined['Paid Network'] = combined['Category'].map(network_dict)
    
    combined = combined.filter(output_cols)
    combined['Hubspot ID'] = combined['Hubspot ID'].astype(str)
    
    write_out(combined) # write output
    


    elapsed = timeit.default_timer() - start_time
    docustring('Process complete in {:0.2f} seconds'.format(elapsed), 'Process complete in {:0.2f} seconds'.format(elapsed))
    #input('Press any key to exit...')
    

############################
### --- /Definitions --- ###
############################

if __name__ == '__main__':
    try:
        logging_setup()
        global variables
        variables = read_variables(cwd, 'variable*.xlsx')
        
        #warn_time = pd.Timestamp('now').strftime('%d%b%y')
        #global warning_file
        #warn_file = variables['Output Files:'][10] + f'\{client}_{tool}_{warn_time}_warning.csv'
        
        main()
    except:
        logger.exception('Got exception on handler')
        os.chdir(cwd)
        #email_attachment(log_file)
        raise