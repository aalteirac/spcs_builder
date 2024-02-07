import snowflake.connector
import pathlib
from snowflake.connector import DictCursor
import pandas as pd
from mysecrets import adminpass,adminuser,spcs_user_pass,accountname
import os
import sys
import time

notReadyEndpointMessage='Endpoints provisioning in progress'

def disconnect():
    cur.close()

def connect(usr,password,acc,role):
    con = snowflake.connector.connect(
        user=usr,
        password=password,
        account=acc,
        role=role
    )
    global cur
    cur= con.cursor(DictCursor)

def upload(cursor,file,stage):
    cur=pathlib.Path().resolve()
    cursor.execute(
    f'''PUT file://{cur}/{file} @{stage} AUTO_COMPRESS=FALSE SOURCE_COMPRESSION=NONE OVERWRITE = TRUE''')   
    ret=cursor.execute(f'''ALTER STAGE {stage} REFRESH''').fetchall() 
    return ret   

def getEndpoints(cursor,service):
    edp=cursor.execute(f'''SHOW ENDPOINTS IN SERVICE {service};''').fetchall()
    edp=pd.DataFrame(edp)
    return edp

def getDBs(cursor):
    dbs=cursor.execute("SHOW DATABASES").fetchall()
    dbs=pd.DataFrame(dbs)
    return dbs

def getSchemas(cursor,db):
    scs=cursor.execute("SHOW SCHEMAS in "+db).fetchall()
    scs=pd.DataFrame(scs)
    return scs

def getStages(cursor,sc):
    scs=cursor.execute("SHOW STAGES in "+sc).fetchall()
    scs=pd.DataFrame(scs)
    return scs

def getRegistry(cursor,sc,repo):
    scs=cursor.execute(f'''SHOW IMAGE REPOSITORIES LIKE '%{repo}%' IN {sc}''').fetchall()
    scs=pd.DataFrame(scs)
    return scs

def transferOwner(cursor,type,asset,role):
    ret=cursor.execute(f'''GRANT OWNERSHIP ON {type} {asset} TO ROLE {role}; ''').fetchall()
    ret=pd.DataFrame(ret)
    return ret 

def grantBindToRole(cursor,role):
    ret=cursor.execute(f'''GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO ROLE {role}''').fetchall()
    ret=pd.DataFrame(ret)
    return ret 

def grantRoleToUser(cursor,role,usr):
    ret=cursor.execute(f'''GRANT ROLE {role} TO USER {usr}; ''').fetchall()
    ret=pd.DataFrame(ret)
    return ret 

def grantPoolToRole(cursor,pool,role):
    ret=cursor.execute(f'''GRANT USAGE, MONITOR ON COMPUTE POOL {pool} TO ROLE {role}; ''').fetchall()
    ret=pd.DataFrame(ret)
    return ret 

def grantCreateDB(cursor,role):
    ret=cursor.execute(f'''GRANT CREATE DATABASE ON ACCOUNT TO ROLE {role};''').fetchall()
    ret=pd.DataFrame(ret)
    return ret  

def grantIntegrationToRole(cursor,integration,role):
    ret=cursor.execute(f'''GRANT USAGE ON INTEGRATION {integration} TO ROLE {role};  ''').fetchall()
    ret=pd.DataFrame(ret)
    return ret      

def setContext(cursor,db,sc):
    dbr=cursor.execute(f''' USE DATABASE {db};''').fetchall()
    scr=cursor.execute(f''' USE SCHEMA {sc};''').fetchall()
    ret=pd.DataFrame(dbr)
    ret2=pd.DataFrame(scr)
    return pd.concat([ret,ret2])

def createRole(cursor,role):
    role=cursor.execute(f''' CREATE ROLE IF NOT EXISTS {role};''').fetchall()
    role=pd.DataFrame(role)
    return role

def createUser(cursor,user,passw,defrole):
    usr=cursor.execute(f'''CREATE USER  IF NOT EXISTS {user} PASSWORD='{passw}' DEFAULT_ROLE = {defrole} DEFAULT_SECONDARY_ROLES = ('ALL') MUST_CHANGE_PASSWORD = FALSE;''').fetchall()
    usr=pd.DataFrame(usr)
    return usr          

def createDB(cursor,dbname):
    ret=cursor.execute(f'''CREATE DATABASE IF NOT EXISTS {dbname};  ''').fetchall()
    ret=pd.DataFrame(ret)
    return ret  

def createSchema(cursor,dbname,scname):
    ret=cursor.execute(f''' CREATE SCHEMA IF NOT EXISTS {dbname}.{scname};  ''').fetchall()
    ret=pd.DataFrame(ret)
    return ret

def createStage(cursor,dbname,scname,stname):
    ret=cursor.execute(f''' CREATE STAGE IF NOT EXISTS {dbname}.{scname}.{stname} DIRECTORY = ( ENABLE = true ); ''').fetchall()
    ret=pd.DataFrame(ret)
    return ret  

def createRepo(cursor,dbname,scname,repo):
    ret=cursor.execute(f''' CREATE IMAGE REPOSITORY IF NOT EXISTS {dbname}.{scname}.{repo}; ''').fetchall()
    ret=pd.DataFrame(ret)
    return ret      

def createNetworkRule(cursor,ruleName):
    ret=cursor.execute(f''' CREATE OR REPLACE NETWORK RULE {ruleName}
                            TYPE = 'HOST_PORT'
                            MODE= 'EGRESS'
                            VALUE_LIST = ('0.0.0.0:443','0.0.0.0:80'); ''').fetchall()
    ret=pd.DataFrame(ret)
    return ret

def createExternalInt(cursor,intName,networkRule):
    ret=cursor.execute(f''' CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION {intName}
                            ALLOWED_NETWORK_RULES = ({networkRule})
                            ENABLED = true;   ''').fetchall()
    ret=pd.DataFrame(ret)
    return ret

def createEgress(cursor,egname):
    ret=cursor.execute(f''' CREATE SECURITY INTEGRATION IF NOT EXISTS {egname}
                            TYPE=oauth
                            OAUTH_CLIENT=snowservices_ingress
                            ENABLED=true; ''').fetchall()
    ret=pd.DataFrame(ret)
    return ret

def createComputePool(cursor,pool,minn=1,maxn=1,family='STANDARD_1'):
    ret=cursor.execute(f''' CREATE COMPUTE POOL IF NOT EXISTS {pool}
                            MIN_NODES = {minn}
                            MAX_NODES = {maxn}
                            INSTANCE_FAMILY = {family};''').fetchall()
    ret=pd.DataFrame(ret)
    return ret    

def createServiceFromSpec(cursor,name,pool,integration,spec,minn=1,maxn=1):
    servcommand=f''' 
 CREATE SERVICE IF NOT EXISTS {name} IN COMPUTE POOL {pool}
 FROM SPECIFICATION $$
 {spec}
 $$
 MIN_INSTANCES={minn}
 MAX_INSTANCES={maxn}
 EXTERNAL_ACCESS_INTEGRATIONS = ({integration});'''           
    ret=cursor.execute(servcommand).fetchall()
    ret=pd.DataFrame(ret)
    return ret  

def createService(cursor,name,pool,integration,stage,spec,minn=1,maxn=1):
    ret=cursor.execute(f''' CREATE SERVICE IF NOT EXISTS {name}
                IN COMPUTE POOL {pool}
                FROM @{stage}
                SPEC='{spec}'
                MIN_INSTANCES={minn}
                MAX_INSTANCES={maxn}
                EXTERNAL_ACCESS_INTEGRATIONS = ({integration});''').fetchall()
    ret=pd.DataFrame(ret)
    return ret  

def waitForEnpoints(cur,service):
    global endpoints
    endpoints=[]
    done=True
    ret=getEndpoints(cur,service)
    for index, row in ret.iterrows():
        entry=row['ingress_url']
        endpoints.append(entry)
    for p in endpoints:
        if notReadyEndpointMessage in p:
            done=False
            break    
    if done==True:     
        return f'''Endpoints Ready!  {['https://'+s for s in endpoints]})'''
    else:
        print(f'''Waiting for Enpoints...  {endpoints})''')
        time.sleep(20)
        return waitForEnpoints(cur,service)    

def execLocalCommand(cmd):
    os.system(cmd)  

def readYaml(file):
    with open(file) as f:
        contents = f.read()
        return contents

def completeSpecImageName(yaml,url,version=0):
    repo=url.split("/",1)
    yaml=yaml.replace(f'''image: {spcs_docker_image}''',f'''image: /{repo[1]}/{spcs_docker_image}:v{version}''')
    return yaml

def loginDocker(url,user,passw):
    execLocalCommand(f'''docker login {url}/ -u {user} -p {passw}''') 

def pushImage(url,img,version=0):
    print('Pushing ' +img)
    execLocalCommand(f'''docker push {url}/{img}:v{version}''')

def tagImage(url,img,version=0):
    print('Tagging ' +img)
    execLocalCommand(f'''docker tag {img} {url}/{img}:v{version}''')

def checkDockerInstalled():
    return True

def listDockerImages():
    return True   






initalRole='ACCOUNTADMIN'
account=accountname
admin_user=adminuser
admin_pass=adminpass
spcs_egress='snowservices_ingress_oauth'.upper()
spcs_db='test_spcs_db'.upper()
spcs_sc='test_spcs_sc'.upper()
spcs_st='test_spcs_stage'.upper()
spcs_repo='test_spcs_repo'.upper()
spcs_role='TEST_SPCS_ROLE'.upper()
spcs_usr='test_spcs_usr'.upper()
spcs_usr_pass=spcs_user_pass
spcs_net_rule='test_spcs_net_rule'.upper()
spcs_integration='test_spcs_integration'.upper()
spcs_pool='test_spcs_pool'
spcs_service='test_spcs_service'

spcs_yaml_file='yaml/sample_grafana.yaml'
spcs_docker_image='grafana/grafana'
 

connect(admin_user,admin_pass,account,initalRole)
try:
    res=transferOwner(cur,'DATABASE',spcs_db,initalRole)
    print(f'''Transfering DB ownership {res["status"].iloc[0]}''')

    res=transferOwner(cur,'SCHEMA',spcs_db+"."+spcs_sc,initalRole)
    print(f'''Transfering SCHEMA ownership {res["status"].iloc[0]}''')

    res=transferOwner(cur,'STAGE',spcs_db+"."+spcs_sc+"."+spcs_st,initalRole)
    print(f'''Transfering STAGE ownership {res["status"].iloc[0]}''')

    res=transferOwner(cur,'IMAGE REPOSITORY',spcs_db+"."+spcs_sc+"."+spcs_repo,initalRole)
    print(f'''Transfering IMAGE REPOSITORY ownership {res["status"].iloc[0]}''')
except:
    print('Initial Objects not yet created...')

res=createEgress(cur,spcs_egress)
print(f'''Create EGRESS {res["status"].iloc[0]}''')

res=createRole(cur,spcs_role)
print(f'''Create ROLE {res["status"].iloc[0]}''')

res=createUser(cur,spcs_usr,spcs_usr_pass,spcs_role)
print(f'''Create USER {res["status"].iloc[0]}''')

res=grantRoleToUser(cur,spcs_role,spcs_usr)
print(f'''Grant ROLE TO USER {res["status"].iloc[0]}''')

res=grantBindToRole(cur,spcs_role)
print(f'''Grant BIND SERVICE TO ROLE {res["status"].iloc[0]}''')

res=createDB(cur,spcs_db)
print(f'''Create DB {res["status"].iloc[0]}''')

res=createSchema(cur,spcs_db,spcs_sc)
print(f'''Create SCHEMA {res["status"].iloc[0]}''')

res=setContext(cur,spcs_db,spcs_sc)
print(f'''Set Context {res["status"].iloc[0]}''')

res=createNetworkRule(cur,spcs_net_rule)
print(f'''Create NETWORK RULE {res["status"].iloc[0]}''')

res=createExternalInt(cur,spcs_integration,spcs_net_rule)
print(f'''Create EXTERNAL ACCESS {res["status"].iloc[0]}''')

res=grantIntegrationToRole(cur,spcs_integration,spcs_role)
print(f'''Grant INTEGRATION TO ROLE {res["status"].iloc[0]}''')

res=createStage(cur,spcs_db,spcs_sc,spcs_st)
print(f'''Create STAGE {res["status"].iloc[0]}''')

res=createRepo(cur,spcs_db,spcs_sc,spcs_repo)
print(f'''Create IMAGE REPOSITORY {res["status"].iloc[0]}''')

res=transferOwner(cur,'DATABASE',spcs_db,spcs_role)
print(f'''Transfering DB ownership {res["status"].iloc[0]}''')

res=transferOwner(cur,'SCHEMA',spcs_db+"."+spcs_sc,spcs_role)
print(f'''Transfering SCHEMA ownership {res["status"].iloc[0]}''')

res=transferOwner(cur,'STAGE',spcs_db+"."+spcs_sc+"."+spcs_st,spcs_role)
print(f'''Transfering STAGE ownership {res["status"].iloc[0]}''')

res=transferOwner(cur,'IMAGE REPOSITORY',spcs_db+"."+spcs_sc+"."+spcs_repo,spcs_role)
print(f'''Transfering IMAGE REPOSITORY ownership {res["status"].iloc[0]}''')

res=createComputePool(cur,spcs_pool)
print(f'''Create COMPUTE POOL {res["status"].iloc[0]}''')

res=grantPoolToRole(cur,spcs_pool,spcs_role)
print(f'''GRANT POOL USAGE TO ROLE {res["status"].iloc[0]}''')

disconnect()

print(f'''Switching USER connection...''')

connect(spcs_usr,spcs_usr_pass,account,spcs_role)

res=setContext(cur,spcs_db,spcs_sc)
print(f'''Set Context {res["status"].iloc[0]}''')

res=upload(cur,spcs_yaml_file,spcs_st)
print(f'''Uploading YAML...''')

res=getRegistry(cur,spcs_db+'.'+spcs_sc,spcs_repo)
spcs_url=res["repository_url"].iloc[0]
print(f'''Getting Repository URL {spcs_url}''')

loginDocker(spcs_url,spcs_usr,spcs_usr_pass)
tagImage(spcs_url,spcs_docker_image)
pushImage(spcs_url,spcs_docker_image)

ret=readYaml(spcs_yaml_file)
spcs_yaml_content=completeSpecImageName(ret,spcs_url)
print(f'''Qualifying Image name in YAML file...''')

res=createServiceFromSpec(cur,spcs_service,spcs_pool,spcs_integration,spcs_yaml_content)
print(f'''Create SERVICE {res["status"].iloc[0]}''')

ret=waitForEnpoints(cur,spcs_service)
print(ret)


# spcs_yaml_file='yaml/sample_cortex.yaml'
# spcs_docker_image='rag_cortex'

# res=createService(cur,spcs_service,spcs_pool,spcs_integration,spcs_st,spcs_yaml_file)
# print(f'''Create SERVICE {res["status"].iloc[0]}''')
# DROP DB OPTION with current role

# TEST
# connect(spcs_usr,spcs_usr_pass,'kl70905.eu-central-1',spcs_role)
# res=getRegistry(cur,spcs_db+'.'+spcs_sc,spcs_repo)
# spcs_url=res["repository_url"].iloc[0]
# ret=readYaml(spcs_yaml_file)
# ret=completeSpecImageName(ret,spcs_url)
# print(ret)
# connect(spcs_usr,spcs_usr_pass,'kl70905.eu-central-1',spcs_role)
# setContext(cur,spcs_db,spcs_sc)
# res=createService(cur,spcs_service,spcs_pool,spcs_integration,spcs_st,'sample.yaml')
# print(res)
# ret=waitForEnpoints(cur,spcs_service)
# print(ret)
# sys.exit(0)
# TEST
