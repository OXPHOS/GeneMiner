# !/usr/bin/env python3.6
# -*- coding:utf-8 -*-

"""
Values used for front end

Author: Pan Deng

"""
from app import conn

# Retrieve disease list from database
cur = conn.cursor()
cur.execute('''SELECT DISTINCT disease_type FROM project_summary WHERE disease_type IS NOT NULL''');
cancers = sorted(list(zip(*cur.fetchall()))[0])

# Filtypes and analysis can be run
fclinical= {'label': 'Clinical report', 'value': 'clinical'}
fgeneexpr = {'label': 'Gene expression analysis', 'value': 'geneexpr'}
fsnp = {'label': 'SNP analysis', 'value': 'snp'}
fnull = {'label': '', 'value': ''}

# Cancer types
# eg. {'label': 'Head and Neck', 'value': 'HeadandNeck'}
cancertypeList = [{'label': '', 'value': ''}]
cancertypeList.extend([{'label': x, 'value': x} for x in cancers])


# Master selection dict
# eg.  'HeadandNeck': [{'label': 'Clinical report', 'value': 'clinical'},
#                       {'label': 'Gene expression analysis', 'value': 'geneexpr'},
#                       {'label': 'SNP analysis', 'value': 'snp'}]
dropdownDict = {x['value']: [fclinical] for x in cancertypeList}
dropdownDict[''] = []

# For cancer types have stage information, include the gene expression analysis tab
for key in dropdownDict.keys():
    cur.execute('''
                SELECT COUNT(DISTINCT disease_stage) 
                FROM patient_info
                WHERE disease_type = '%s'
                    AND disease_stage ~ 'Stage'
                ''' % key);
    if cur.fetchone()[0] > 1:
        dropdownDict[key].append(fgeneexpr)
# Include a SNP analysis for Breast cancer
dropdownDict['Breast'].append(fsnp)


# Gene expr analysis stages
stages = {'Bladder': ['Stage II', 'Stage III'],
          'Breast': ['Stage I', 'Stage II'],
          'Colon': ['Stage II', 'Stage III'],
          'Esophagus': ['Stage IIB', 'Stage IIA'],
          'Extremities': [],
          'Head and Neck': ['Stage II', 'Stage III'],
          'Kidney': ['Stage II', 'Stage III'],
          'Liver': ['Stage I', 'Stage II'],
          'Lung': ['Stage IA', 'Stage IB'],
          'Other  Specify': [],
          'Pancreas': ['Stage IIA', 'Stage IIB'],
          'Rectum': [],
          'Stomach': ['Stage IIIA', 'Stage IIIB'],
          'Thyroid': ['Stage II', 'Stage III'],
          'Trunk': []
}

colors = {
    'background': '#111111',
    'text': '#F6F6FA'
}
