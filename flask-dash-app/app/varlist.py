colors = {
    'background': '#111111',
    'text': '#F6F6FA'
}

# Filtypes and analysis can be run
fclinical= {'label': 'Clinical report', 'value': 'clinical'}
fgeneexpr = {'label': 'Gene expression analysis', 'value': 'geneexpr'}
fsnp = {'label': 'SNP analysis', 'value': 'snp'}
fnull = {'label': '', 'value': ''}

#
cancertypeList = [
    {'label': '', 'value': ''},
    {'label': 'Breast cancer', 'value': 'Breast'},
    {'label': 'Brain tumor', 'value': 'Brain'},
    {'label': 'Lung cancer', 'value': 'Lung'}
]

# Master selection dict
dropdownDict = {
    '':[],
    'Breast': [fclinical, fgeneexpr, fsnp],
    'Brain': [fclinical, fsnp],
    'Lung': [fclinical, fsnp]
}
