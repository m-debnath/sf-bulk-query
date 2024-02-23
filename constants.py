ASSET_QUERY_PARAM_EXECUTE_TIME = '2024-02-19T22:00:00Z'  # CHANGE THIS TO PROPER TIME OF EXECUTION FOR EVERY BATCH
JOB_TO_QUERY = {
    'Asset': f'''
SELECT Account.KRN_ID__c, vlocity_cmt__ContractId__r.EnterpriseServiceId__c, vlocity_cmt__ContractId__r.IRMA_Customer_ID__c, Id, External_System_Id__c, vlocity_cmt__RecurringTotal__c, vlocity_cmt__OneTimeTotal__c, Product2.ProductCode, Product2.Name, Product2.Family, Product2.Promis_Code__c, InstallDate, Quantity, ExternalInterfaceId__c, PropositionType__c FROM Asset where vlocity_cmt__ProvisioningStatus__c = 'Active' and (BillingEndDate__c = null or BillingEndDate__c >= {ASSET_QUERY_PARAM_EXECUTE_TIME})
''',
    'BIL_Identifier_Mapping__c': '''
SELECT Asset__r.Account.KRN_ID__c, Asset__r.ExternalInterfaceId__c, Asset__r.vlocity_cmt__LineNumber__c, AssetEffectiveQuantity__c, Asset__r.Name, Asset__r.vlocity_cmt__ProvisioningStatus__c, Asset__r.Product2.ProductCode, Asset__r.Service_ID__c, Asset__r.PropositionType__c, ESMEE_Id__c, Asset__r.BillingStartDate__c, Asset__r.BillingEndDate__c FROM BIL_Identifier_Mapping__c
'''
}
COLUMN_HEADER_MAPPING = {
    'Asset': {
        '"Account.KRN_ID__c"': '"CUSTOMER ID"',
        '"vlocity_cmt__ContractId__r.EnterpriseServiceId__c"': '"CONTRACT ID"',
        '"vlocity_cmt__ContractId__r.IRMA_Customer_ID__c"': '"IRMA CUSTOMER ID"',
        '"Id"': '"ASSET ID"',
        '"External_System_Id__c"': '"ROUTIT ORDER ID"',
        '"vlocity_cmt__RecurringTotal__c"': '"RECCURING COST"',
        '"vlocity_cmt__OneTimeTotal__c"': '"ONE TIME COST"',
        '"Product2.ProductCode"': '"PRODUCT CODE"',
        '"Product2.Name"': '"PRODUCT NAME"',
        '"Product2.Family"': '"PRODUCT FAMILY"',
        '"Product2.Promis_Code__c"': '"PROMIS CODE"',
        '"InstallDate"': '"ACTIVATION DATE"',
        '"Quantity"': '"QUANTITY"',
        '"ExternalInterfaceId__c"': '"EXTERNAL INTERFACE ID"',
        '"PropositionType__c"': '"PORTFOLIO ID"'
    },
    'BIL_Identifier_Mapping__c': {
        '"Asset__r.Account.KRN_ID__c"': '"KRN"',
        '"Asset__r.ExternalInterfaceId__c"': '"EXTERNAL INTERFACE ID"',
        '"Asset__r.vlocity_cmt__LineNumber__c"': '"LINE NUMBER"',
        '"Asset__r.Name"': '"PRODUCT NAME"',
        '"Asset__r.vlocity_cmt__ProvisioningStatus__c"': '"ASSET STATUS"',
        '"Asset__r.Product2.ProductCode"': '"PRODUCT CODE"',
        '"Asset__r.Service_ID__c"': '"SERVICE ID"',
        '"Asset__r.PropositionType__c"': '"PROPOSITION"',
        '"Asset__r.BillingStartDate__c"': '"ACTIVATION DATE"',
        '"Asset__r.BillingEndDate__c"': '"DEACTIVATION DATE"',
        '"AssetEffectiveQuantity__c"': '"QUANTITY"',
        '"ESMEE_Id__c"': '"CHARGE ID"'
    }
}
ADDITIONAL_COLUMN_MAPPING = {
    'Asset': {
        'PARTNER ID': '67490'
    },
    'BIL_Identifier_Mapping__c': {}
}
FILE_NAME_MAPPING = {
    'Asset': f'ESMEE_RABBIT_DATA_<FILE_SUFFIX>_<JOB_ID>.csv',
    'BIL_Identifier_Mapping__c': f'ESMEE_RABBIT_BILLINGDATA_<FILE_SUFFIX>_<JOB_ID>.csv'
}

API_POLL_FREQ_SECONDS = 0.25
MAX_RETRY_TIMEOUT_SECONDS = 2*60
MAX_RETRY_COUNT = MAX_RETRY_TIMEOUT_SECONDS/API_POLL_FREQ_SECONDS
BATCH_STATUS_COMPLETED = 'Completed'
JOB_STATUS_CLOSED = 'Closed'

AUTH_HEADER_NAME = 'X-SFDC-Session'

APP_BANNER = '''
=================================================================================
888~~\           888 888   _           ,88~-_                                    
888   | 888  888 888 888 e~ ~         d888   \  888  888  e88~~8e  888-~\ Y88b  /
888 _/  888  888 888 888d8b          88888    | 888  888 d888  88b 888     Y888/ 
888  \  888  888 888 888Y88b         88888    | 888  888 8888__888 888      Y8/  
888   | 888  888 888 888 Y88b         Y888 \ /  888  888 Y888    , 888       Y   
888__/  "88_-888 888 888  Y88b         `88__X   "88_-888  "88___/  888      /    
                                             \                            _/     
=================================================================================
'''