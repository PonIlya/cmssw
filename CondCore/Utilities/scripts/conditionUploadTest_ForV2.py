#!/usr/bin/env python

from __future__ import print_function
import cx_Oracle
import sqlite3
import subprocess
import json
import os
import shutil
import datetime
import argparse
import time
from enum import Enum

# Requirement 1: a conddb key for the authentication with valid permission on writing on prep CMS_CONDITIONS account 
#                this could be dropped introducing a specific entry in the .netrc 
# Requirement 2: an entry "Dropbox" in the .netrc for the authentication

base_tag_name = 'test_CondUpload2'

input_tags = {
    # "label" : ("tag", start_since1, interval, step1, step2, start_since2 )
    "run" : ("runinfo_31X_hlt", 200000, 100, 20, 10, 200200),
    "lumi" : ("BeamSpotOnlineTestLegacy", 1447562892541957, 100, 20, 10, 1454190027079685 ),
    "time" : ("EcalLaserAPDPNRatios_prompt_v2", 6936865094961725440, 500000000000000, 20000000000000, 10000000000000
              , 6937263354394181632 )}

class DB:
    def __init__(self, serviceName, schemaName ):
        self.serviceName = serviceName
        self.schemaName = schemaName
        self.connStr = None

    def connect( self ):
        command = "cmscond_authentication_manager -s %s --list_conn | grep '%s@%s'" %(self.serviceName,self.schemaName,self.serviceName)
        pipe = subprocess.Popen( command, shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        out = pipe.communicate()[0]
        srvconn = '%s@%s' %(self.schemaName,self.serviceName)
        rowpwd = out.split(srvconn)[1].split(self.schemaName)[1]
        pwd = ''
        for c in rowpwd:
            if c != ' ' and c != '\n':
                pwd += c
        self.connStr =  '%s/%s@%s' %(self.schemaName,pwd,self.serviceName)

    def getFCSR( self, timetype ):
        command = "conddb showFCSR"
        pipe = subprocess.Popen( command, shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        out = pipe.communicate()[0]
        lines = out.split('\n')
        line = lines[4]
        ind0 = line.find("Time:")
        if ind0 == -1:
            raise Exception('The FCSR could not been obtained.')
        ind1 = line.find("[")
        return int(line[ind0+5:ind1])

    def setSynchronizationType( self, tag, synchType ):
        db = cx_Oracle.connect(self.connStr)
        cursor = db.cursor()
        db.begin()
        cursor.execute('UPDATE TAG SET SYNCHRONIZATION =:SYNCH WHERE NAME =:NAME',(synchType,tag,))
        db.commit()

    def getLastInsertedSince( self, tag, snapshot ):
        db = cx_Oracle.connect(self.connStr)
        cursor = db.cursor()
        query = 'SELECT SINCE, INSERTION_TIME FROM IOV WHERE TAG_NAME =:TAG_NAME AND INSERTION_TIME >:TIME ORDER BY SINCE'
        #print('Executing query: %s with time=%s'%(query,snapshot.strftime("%Y/%m/%d %H:%M:%S")))
        cursor.execute(query,(tag,snapshot))
        row = cursor.fetchone()
        return row

    def removeTag( self, tag ):
        db = cx_Oracle.connect(self.connStr)
        cursor = db.cursor()
        db.begin()
        cursor.execute('DELETE FROM TAG_METADATA WHERE TAG_NAME =:TAG_NAME',(tag,))
        cursor.execute('DELETE FROM IOV WHERE TAG_NAME =:TAG_NAME',(tag,))
        cursor.execute('DELETE FROM TAG_LOG WHERE TAG_NAME=:TAG_NAME',(tag,))
        cursor.execute('DELETE FROM TAG WHERE NAME=:NAME',(tag,))
        db.commit()

    def verifyTagMetadata( self, tag, destTag ):
        db = cx_Oracle.connect(self.connStr)
        cursor = db.cursor()
        query = 'SELECT MIN_SERIALIZATION_V, MIN_SINCE FROM TAG_METADATA WHERE TAG_NAME =:TAG_NAME'
        cursor.execute(query,(tag,))
        rows = cursor.fetchall()
        vers = []
        i = 0
        for r in rows:
            vers.append((r[0],r[1]))
            print('ver[%s] min_ser %s min_since %s'%(i,r[0],r[1]))
            i+=1
        cursor = db.cursor()
        cursor.execute(query,(destTag,))
        rows = cursor.fetchall()
        dvers = []
        i = 0
        for r in rows:
            dvers.append((r[0],r[1]))
            #print('ver[%s] min_ser %s min_since %s'%(i,r[0],r[1]))
            i += 1
        sz = len(vers)
        dsz = len(dvers)
        if sz != dsz:
            print('ERROR: N v1 vers=%s != N v2 vers=%s'%(dsz,sz))
            return False
        ret = True
        for i in range(0,sz):
            if dvers[i][0] != vers[i][0]:
                print('ERROR: v2 min_ser[%s]=%s != v1 min_ser[%s]=%s' %(i,dvers[i][0],i,vers[i][0]))
                ret = False
            if dvers[i][1] != vers[i][1]:
                print('ERROR: v2 min_since[%s]=%s != v1 min_since[%s]=%s' %(i,dvers[i][1],i,vers[i][1]))
                ret = False
        return ret
        

    def verifyTag( self, tag, destTag, destDB=None ):
        db = cx_Oracle.connect(self.connStr)
        cursor = db.cursor()
        query = 'SELECT SINCE, PAYLOAD_HASH FROM IOV WHERE TAG_NAME =:TAG_NAME ORDER BY SINCE'
        cursor.execute(query,(tag,))
        rows = cursor.fetchall()
        iovs = []
        i = 0
        for r in rows:
            iovs.append((r[0],r[1]))
            #print('iov[%s] since %s hash %s'%(i,r[0],r[1]))
            i+=1
        if destDB is not None:
            db = sqlite3.connect(destDB)
        cursor = db.cursor()
        cursor.execute(query,(destTag,))
        rows = cursor.fetchall()
        diovs = []
        i = 0
        for r in rows:
            diovs.append((r[0],r[1]))
            #print('iov[%s] since %s hash %s'%(i,r[0],r[1]))
            i += 1
        sz = len(iovs)
        dsz = len(diovs)
        if sz != dsz:
            print('ERROR: N src iovs=%s != N dest iovs=%s'%(dsz,sz))
            return False
        ret = True
        for i in range(0,sz):
            if diovs[i][0] != iovs[i][0]:
                print('ERROR: src since[%s]=%s != dest since[%s]=%s' %(i,diovs[i][0],i,iovs[i][0]))
                ret = False
            if diovs[i][1] != iovs[i][1]:
                print('ERROR: src hash[%s]=%s != dest hash[%s]=%s' %(i,diovs[i][1],i,iovs[i][1]))
                ret = False
        return ret

class TestMode(Enum):
    v1 = 0
    v2 = 1
    COMPARE = 2

def makeBaseFile( inputTag, startingSince, endingSince ):
    cwd = os.getcwd()
    baseFile = '%s_%s' %(inputTag,startingSince)
    baseFilePath = '%s.db' %os.path.join(cwd,baseFile)
    if os.path.exists( baseFilePath ):
        os.remove( baseFilePath )
    command = "conddb --yes copy %s %s --destdb %s.db -f %s -t %s" %(inputTag,inputTag,baseFile,startingSince,endingSince)
    #print(command)
    pipe = subprocess.Popen( command, shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    out = pipe.communicate()[0]
    if not os.path.exists( '%s.db' %baseFile ):
        msg = 'ERROR: base file has not been created: %s' %out
        raise Exception( msg )
    return baseFile
        

def makeMetadataFile( inputTag, destTag, since, description ):
    cwd = os.getcwd()
    metadataFile = os.path.join(cwd,'%s.txt') %destTag
    if os.path.exists( metadataFile ):
        os.remove( metadataFile )
    metadata = {}
    metadata[ "destinationDatabase" ] = "oracle://cms_orcoff_prep/CMS_CONDITIONS"
    tagList = {}
    tagList[ destTag ] = { "dependencies": {}, "synchronizeTo": "any" }
    metadata[ "destinationTags" ] = tagList
    metadata[ "inputTag" ] = inputTag
    metadata[ "since" ] = since
    metadata[ "userText" ] = description
    fileName = destTag+".txt"
    with open( fileName, "w" ) as file:
        file.write(json.dumps(metadata,file,indent=4,sort_keys=True))

def uploadFile( fileName, logFileName, v2 ):
    command = "uploadConditions.py -a /build/gg %s" %fileName
    if v2:
        command = './uploadConditions_v2.py --metadataFile %s.txt --server https://cms-conddb-dev.cern.ch/cmsDbCondUpload/ --netrc /build/gg/.netrc --sourceDB %s.db' %(fileName,fileName)
    print(command)
    pipe = subprocess.Popen( command, shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    out = pipe.communicate()[0]
    print(out)
    lines = out.split('\n')
    ret = False
    endLineKey = 'upload ended with code:'
    if v2:
        endLineKey = 'Process completed without issues.'
    for line in lines:
        if line.startswith(endLineKey):
            if v2:
                ret = True
            else:
                returnCode = line.split(endLineKey)[1].strip()
                if returnCode == '0':
                    ret = True
            break
    with open(logFileName,'a') as logFile:
        logFile.write(out)
    time.sleep(2.)
    return ret

class UploadTest:
    def __init__(self, db, mode):
        self.db = db
        self.mode = mode
        self.errors = 0
        self.logFileName = 'conditionUploadTest.log'
        self.upload_id = 0

    def makeSourceFiles( self, baseFile, inputTag, destTag, destSince, synchro ):
        self.upload_id += 1
        destFileName = destTag
        destFile = '%s.db' %destFileName
        shutil.copyfile( '%s.db' %baseFile, destFile )
        descr = 'Testing conditionsUpload with synch:%s - Upload #%s' %(synchro,self.upload_id)
        makeMetadataFile( inputTag, destTag, destSince, descr )
        return destFileName

    def log( self, msg ):
        print(msg)
        with open(self.logFileName,'a') as logFile:
            logFile.write(msg)
            logFile.write('\n')

    def logError( self, msg ):
        self.log('ERROR: %s'%msg)
        self.errors += 1

    def cleanUp( self, tag ):
        self.db.removeTag( tag )
        if self.mode == TestMode.COMPARE:
            self.db.removeTag( '%s_v2'%tag )

    def checkUpload( self,  destTag, destFileName, synchro, destSince, success, expectedAction, v2 ):
        insertedSince = None
        error = 0
        beforeUpload = datetime.datetime.utcnow()
        ret = uploadFile( destFileName, self.logFileName, v2 )
        if ret != success:
            self.log( 'ERROR: the return value for the upload of tag %s with sychro %s was %s, while the expected result is %s' %(destTag,synchro,ret,success))
            error = 1
        else:
            row = self.db.getLastInsertedSince( destTag, beforeUpload )
            if row is None:
                print('ERROR: Nothing inserted in the last upload...')
                if expectedAction != 'FAIL':
                    error = 1
                else:
                    print('# OK: Expected action=FAIL')
                self.errors += error
                if error==1:
                    print('##### Workflow %s FAILED!'%id)
                return error, None
            if ret == True:
                if expectedAction == 'CREATE' or expectedAction == 'INSERT' or expectedAction == 'APPEND':
                    print('## since inserted: %s - expected value: %s'%(row[0],destSince))
                    if destSince != row[0]:
                        self.log( 'ERROR: the since inserted is %s, expected value is %s - expected action: %s' %(row[0],destSince,expectedAction))
                        error = 1
                    else:
                        self.log( '# OK: Found expected value for last since inserted: %s timestamp: %s' %(row[0],row[1]))
                        insertedSince = row[0]
                        if expectedAction == 'CREATE':
                            self.db.setSynchronizationType( destTag, synchro ) 
                elif expectedAction == 'SYNCHRONIZE':
                    print('#sychronizing...')
                    if destSince == row[0]:
                        self.log( 'ERROR: the since inserted %s has not been synchronized with the FCSR - expected action: %s' %(row[0],expectedAction))
                        error = 1
                    else:
                        self.log( '# OK: Found synchronized value for the last since inserted: %s timestamp: %s' %(row[0],row[1]))
                        insertedSince = row[0]
                else:
                    self.log( 'ERROR: found an appended since %s - expected action: %s' %(row[0],expectedAction))
                    error = 1
            else:
                if not row is None:
                    self.log( 'ERROR: found new insered since: %s timestamp: %s' %(row[0],row[1]))
                    if expectedAction != 'FAIL':
                        self.log( 'ERROR: Upload failed. Expected value: %s' %(destSince))
                        error = 1
                    else:
                        self.log( '# OK: Upload failed as expected.')
        self.errors += error
        return error, insertedSince

    def upload( self, id, inputTag, baseFile, destTag, synchro, destSince, success, expectedAction ):
        insertedSince = None
        error = 0
        v2 = False
        if self.mode == TestMode.v2:
            v2 = True
        destFileName = self.makeSourceFiles( baseFile, inputTag, destTag, destSince, synchro )
        self.log( '# %s ---------------------------------------------------------------------------'%id)
        self.log( '# Testing tag %s with synch=%s, destSince=%s - expecting ret=%s action=%s' %(destTag,synchro,destSince,success,expectedAction))
        destFile = '%s.db' %destFileName
        metaDestFile = '%s.txt' %destFileName
        beforeUpload = datetime.datetime.utcnow()
        error,insertedSince = self.checkUpload( destTag, destFileName, synchro, destSince, success, expectedAction, v2 )
        os.remove( destFile )
        os.remove( metaDestFile )
        if error==1:
            self.errors += 1
            print('##### Workflow %s FAILED!'%id)
            return insertedSince
        if self.mode == TestMode.COMPARE:
            v2 = True
            destTagv2 = '%s_v2'%destTag
            destFileName = self.makeSourceFiles( baseFile, inputTag, destTagv2, destSince, synchro )
            destFile = '%s.db' %destFileName
            metaDestFile = '%s.txt' %destFileName
            beforeUpload = datetime.datetime.utcnow()
            error,insertedSincev2 = self.checkUpload( destTagv2, destFileName, synchro, destSince, success, expectedAction, v2 )
            os.remove( destFile )
            os.remove( metaDestFile )
            if error==0:
                if insertedSince != insertedSincev2:
                    print('Error: last inserted since v1=%s differs from last inserted since v2=%s'%(insertedSince,insertedSincev2))
                    self.errors += 1
                if not self.db.verifyTag( destTag, destTagv2 ):
                    print('Error: v1 tag and v2 tag differs.')
                    self.errors +=1
                else:
                    print('# OK: v1 tag and v2 tag contents are equals.')
                if not self.db.verifyTagMetadata( destTag, destTagv2 ):
                    print('Error: v1 tag metadata and v2 tag metadata differs.')
                    self.errors +=1
                else:
                    print('# OK: v1 tag metadata and v2 tag metadata are equals.')
            else:
                self.errors += 1
                print('##### Workflow %s FAILED!'%id)    
        if self.errors > 0:
            raise Exception('ERROR: Workflow %s FAILED!' %id)
        return insertedSince

def main():
    parser = argparse.ArgumentParser(description='Validation tool for the upload services.')
    parser.add_argument('--v2', '-v2', action='store_true', help='Validation for the cmsCondDbUploader service')
    parser.add_argument('--compare', '-c', action='store_true', help='Compare for the outcome from the two services')
    parser.add_argument('--timetype', '-t', type=str, default='run', help='Timetype of the IOV')
    args = parser.parse_args()
    timetype = args.timetype
    if timetype not in input_tags.keys():
        print('ERROR: timetype %s is invalid.'%timetype)
        return -1
    print('Testing...')
    serviceName = 'cms_orcoff_prep'
    schemaName = 'CMS_CONDITIONS'
    db = DB(serviceName,schemaName)
    db.connect()
    input_data = input_tags[timetype]
    inputTag = input_data[0]
    destSince0 = input_data[1]
    interval = input_data[2]
    step0 = input_data[3]
    step1 = input_data[4]
    destSince1 = input_data[5]
    bfile0 = makeBaseFile( inputTag,destSince0,destSince0+interval)
    bfile1 = makeBaseFile( inputTag,destSince1,destSince1+interval)
    mode = TestMode.v1
    if args.v2:
        mode = TestMode.v2
    elif args.compare:
        mode = TestMode.COMPARE
    test = UploadTest( db, mode )
    # test with synch=any
    tag = '%s_any' %base_tag_name
    test.cleanUp( tag )
    test.upload( 1, inputTag, bfile0, tag, 'any', destSince0, True, 'CREATE' )
    if not db.verifyTag( tag, inputTag, '%s.db'%bfile0 ):
        test.logError('imported tag is not a proper copy of the source tag.')
    # expected to fail: the target since = destSince0 can't be found in the target file.
    test.upload( 2, inputTag, bfile1, tag, 'any', destSince0, False, 'FAIL' )  
    test.upload( 3, inputTag, bfile0, tag, 'any', destSince0+step0, True, 'APPEND' )  
    test.upload( 4, inputTag, bfile0, tag, 'any', destSince0+step1, True, 'INSERT')  
    test.upload( 5, inputTag, bfile0, tag, 'any', destSince0+step0, True, 'INSERT')  
    test.cleanUp( tag )
    # test with synch=validation
    tag = '%s_validation' %base_tag_name
    test.cleanUp( tag )
    test.upload( 6, inputTag, bfile0, tag, 'validation', destSince0, True, 'CREATE')  
    test.upload( 7, inputTag, bfile0, tag, 'validation', destSince0, True, 'INSERT')  
    bfile2 = makeBaseFile( inputTag,destSince0,destSince0+interval)
    test.upload( 8, inputTag, bfile2, tag, 'validation', destSince0+step0, True, 'APPEND')  
    test.upload( 9, inputTag, bfile0, tag, 'validation', destSince0+step1, True, 'INSERT')  
    test.cleanUp( tag )
    # test with synch=mc
    tag = '%s_mc' %base_tag_name
    test.cleanUp( tag )
    test.upload( 10, inputTag, bfile1, tag, 'mc', 1, False, 'FAIL')
    bfile3 = makeBaseFile( inputTag,1,interval)
    test.upload( 11, inputTag, bfile3, tag, 'mc', 1, True, 'CREATE')  
    test.upload( 12, inputTag, bfile3, tag, 'mc', 1, False, 'FAIL')  
    test.upload( 13, inputTag, bfile3, tag, 'mc', 200, False, 'FAIL') 
    test.cleanUp( tag )
    fcsr = None
    if timetype != 'time':
        # test with synch=hlt
        tag = '%s_hlt' %base_tag_name
        test.cleanUp( tag )
        test.upload( 14, inputTag, bfile0, tag, 'hlt', destSince0, True, 'CREATE')  
        test.upload( 15, inputTag, bfile0, tag, 'hlt', destSince0+2*interval, True, 'SYNCHRONIZE')  
        fcsr = test.upload( 16, inputTag, bfile0, tag, 'hlt', destSince0+interval, True, 'SYNCHRONIZE')  
        if not fcsr is None:
            since = fcsr + 2*interval
            test.upload( 17, inputTag, bfile0, tag, 'hlt', since, True, 'APPEND')  
            since = fcsr + interval
            test.upload( 18, inputTag, bfile0, tag, 'hlt', since, True, 'INSERT')          
        test.cleanUp( tag )
        # test with synch=express
        tag = '%s_express' %base_tag_name
        test.cleanUp( tag )
        test.upload( 19, inputTag, bfile0, tag, 'express', destSince0, True, 'CREATE')  
        test.upload( 20, inputTag, bfile0, tag, 'express', destSince0+2*interval, True, 'SYNCHRONIZE')  
        fcsr = test.upload( 21, inputTag, bfile0, tag, 'express', destSince0+interval, True, 'SYNCHRONIZE')  
        if not fcsr is None:
            since = fcsr + 2*interval
            test.upload( 22, inputTag, bfile0, tag, 'express', since, True, 'APPEND')  
            since = fcsr + interval
            test.upload( 23, inputTag, bfile0, tag, 'express', since, True, 'INSERT')  
        test.cleanUp( tag )
        # test with synch=prompt
        tag = '%s_prompt' %base_tag_name
        test.cleanUp( tag )
        test.upload( 24, inputTag, bfile0, tag, 'prompt', destSince0, True, 'CREATE')  
        test.upload( 25, inputTag, bfile0, tag, 'prompt', destSince0+2*interval, True, 'SYNCHRONIZE')  
        fcsr = test.upload( 26, inputTag, bfile0, tag, 'prompt', destSince0+interval, True, 'SYNCHRONIZE')  
        if not fcsr is None:
            since = fcsr + 2*interval
            test.upload( 27, inputTag, bfile0, tag, 'prompt', since, True, 'APPEND')  
            since = fcsr + interval
            test.upload( 28, inputTag, bfile0, tag, 'prompt', since, True, 'INSERT')  
        test.cleanUp( tag )
        # test with synch=pcl:
        tag = '%s_pcl' %base_tag_name
        test.cleanUp( tag )
        test.upload( 29, inputTag, bfile0, tag, 'pcl', destSince0, True, 'CREATE')  
        test.upload( 30, inputTag, bfile0, tag, 'pcl', destSince0+2*interval, False, 'FAIL')  
        if not fcsr is None:
            since = fcsr + 2*interval
            test.upload( 31, inputTag, bfile0, tag, 'pcl', since, True, 'APPEND')  
            since = fcsr + interval
            test.upload( 32, inputTag, bfile0, tag, 'pcl', since, True, 'INSERT')  
        test.cleanUp( tag )
    # test with synch=offline
    tag = '%s_offline' %base_tag_name
    test.cleanUp( tag )
    test.upload( 33, inputTag, bfile0, tag, 'offline', destSince0, True, 'CREATE')  
    test.upload( 34, inputTag, bfile0, tag, 'offline', destSince0+2*interval, True, 'APPEND')
    test.upload( 35, inputTag, bfile0, tag, 'offline', destSince0+interval, False, 'FAIL' ) 
    test.upload( 36, inputTag, bfile0, tag, 'offline', destSince0+2*interval, False, 'FAIL' ) 
    test.upload( 37, inputTag, bfile0, tag, 'offline', destSince0+3*interval, True, 'APPEND' ) 
    test.cleanUp( tag )
    # test with synch=runmc
    #tag = '%s_runmc' %base_tag_name
    #test.upload( inputTag, bfile0, tag, 'runmc', 1, True, 'CREATE')  
    #test.upload( inputTag, bfile0, tag, 'runmc', 1000, True, 'APPEND')
    #test.upload( inputTag, bfile0, tag, 'runmc', 500, False, 'FAIL' ) 
    #test.upload( inputTag, bfile0, tag, 'runmc', 1000, False, 'FAIL' ) 
    #test.upload( inputTag, bfile0, tag, 'runmc', 2000, True, 'APPEND' )
    #test.cleanUp( tag )
    #os.remove( '%s.db' %bfile0 )
    #os.remove( '%s.db' %bfile1 )
    print('Done. Errors: %s' %test.errors)
    
if __name__ == '__main__':
    main()
