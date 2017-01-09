#***********************************************************************************************
#* Loads the configuration from .cfg file in current directory and returns the params dictionary
#*
#* Author: Kapil Sharma
#* Date: 08/30/2016
#***********************************************************************************************
def getConfig(configPath, logger):
    params = {} 
    logger.info('Parsing configuration from ' + configPath)
    
    with open (configPath, 'r') as cfg:
        for line in cfg:
            line = line.rstrip() #removes trailing whitespace and '\n' chars
            
            if '=' not in line: 
                continue #skips blanks and comments w/o =
            
            if line.startswith('#'): 
                continue #skips comments which contain =
            
            key, value = line.split('=', 1)
            
            if key == "emailFrom":
                if '@' not in value:
                    params[key] = value.strip() + '@email.com' 
                else:
                    params[key] = value.strip()            
            elif key == 'emailTo':
                emailTo = []
            
                for email in value.strip().split(','):
                    if '@' not in email:
                        emailTo.append(email.strip() + '@email.com')
                    else:
                        emailTo.append(email.strip())
                        
                params[key] = emailTo
            else:
                params[key] = value.strip()
                
    logger.info('Configuration loaded')
    
    return params
