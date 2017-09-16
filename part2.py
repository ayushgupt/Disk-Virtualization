# Disk Creation and Deletion Supported; Fragmentation Problem solved;
from Queue import *
import random

class BlockData:
    def __init__(self,blockSize):
        self.data=bytearray(blockSize)


class BlockMetaData:
    def __init__(self):
        self.errorFlag=False
        self.replicationBlock=None
        self.free=True
        self.allotted=False
        self.diskID=None


    def freeFromDisk(self):
        self.allotted=False
        self.diskID=None
        self.free=True
        self.replicationBlock=None

    def allotToDisk(self,disID):
        self.diskID=disID
        self.allotted=True


class DiskData:
    def __init__(self,idD,sizeD,blockLis,replica):
        self.idDisk=idD
        self.sizeDisk=sizeD
        self.blockList=blockLis
        self.replicaList=replica



class FileSystem:
    def __init__(self,blockSize,simError,errorProb):
        self.simulateErrorFlag=simError
        self.errorProb=errorProb
        self.corruptBlocksCount=0
        #self.errorInOrig=0
        #self.errorInReplica=0
        self.blockSize=blockSize
        self.diskA = [BlockData(blockSize) for i in range(200)]
        self.diskB = [BlockData(blockSize) for i in range(300)]
        self.blocksMetaData = [BlockMetaData() for i in range(500)]
        self.diskList={}
        self.freeBlockList= Queue()
        for t in range(500):
            self.freeBlockList.put(t)

    def writeBlock(self, blockNum, writeData):
        if blockNum<1 or blockNum>500:
            print "Block Number is Invalid!"
            return False
        if len(writeData)>self.blockSize:
            print "Too much data to write in 1 block"
            return False
        if self.blocksMetaData[blockNum-1].errorFlag:
            print "Cant write to Block, It has Error"
            return False
        if not self.blocksMetaData[blockNum-1].free:
            print "Over Writing to a Block"
        self.blocksMetaData[blockNum-1].free=False
        if blockNum>=1 and blockNum<=200:
            self.diskA[blockNum-1].data[0:len(writeData)]=writeData
        else:
            self.diskB[blockNum-201].data[0:len(writeData)]=writeData
        return True

    def readBlock(self,blockNum,readData):
        if blockNum < 1 or blockNum > 500:
            print "Block Number is Invalid!"
            return False
        if self.blocksMetaData[blockNum-1].free:
            print "Block is Free, Nothing to Read!"
            return True
        if self.blocksMetaData[blockNum-1].errorFlag:
            print "Block is corrupted! Cant read from it :("
            return False
        if self.simulateErrorFlag and random.uniform(0,100)<10:
            self.blocksMetaData[blockNum-1].errorFlag=True
            print "Block just got Corrupted! Cant read from it :("
            self.corruptBlocksCount+=1
            return False
        lengthToRead=min(self.blockSize,len(readData))
        print "reading Length",lengthToRead
        if blockNum>=1 and blockNum<=200:
            readData[0:lengthToRead]=self.diskA[blockNum-1].data[0:lengthToRead]
        else:
            readData[0:lengthToRead]=self.diskB[blockNum-201].data[0:lengthToRead]
        return True

    def printDiskAllocation(self):
        for i in range(500):
            diskName="NONE"
            if(self.blocksMetaData[i].diskID != None):
                diskName=self.blocksMetaData[i].diskID
            print i+1,"::",diskName

    def createDisk(self,diskID,numBlocks):
        #check if DiskID is already there
        if(diskID in self.diskList):
            print "Such ID for disk already exists"
            return False
        totalBlocksNeeded=2*numBlocks
        if(totalBlocksNeeded>self.freeBlockList.qsize()):
            print "Dont have So many free Blocks!"
            return False
        #set block as allotted
        currBlockList=[]
        for t in range(numBlocks):
            currBlockNum=self.freeBlockList.get()
            self.blocksMetaData[currBlockNum].allotToDisk(diskID)
            currBlockList.append(currBlockNum)
        currBlockListReplica = []
        for t in range(numBlocks):
            currBlockNum = self.freeBlockList.get()
            self.blocksMetaData[currBlockNum].allotToDisk(diskID)
            currBlockListReplica.append(currBlockNum)
        newDisk=DiskData(diskID,numBlocks,currBlockList,currBlockListReplica)
        self.diskList[diskID]=newDisk
        return True

    def deleteDisk(self,id):
        if not id in self.diskList:
            print "There is no disk with such an ID!"
            return False
        for blocksNumber in self.diskList[id].blockList:
            self.blocksMetaData[blocksNumber].freeFromDisk()
            self.freeBlockList.put(blocksNumber)
        for blocksNumber in self.diskList[id].replicaList:
            self.blocksMetaData[blocksNumber].freeFromDisk()
            self.freeBlockList.put(blocksNumber)
        self.diskList.pop(id)
        return True

    def replicationBlock(self,diskId):
        for blockNum in self.diskList[diskId].replicaList:
            if(self.blocksMetaData[blockNum].free and not self.blocksMetaData[blockNum].errorFlag):
                return blockNum
        return None

    def writeDisk(self, diskId, blockNum, writeData):
        if not diskId in self.diskList:
            print "There is no such diskId present"
            return False
        if blockNum<1 or blockNum> len(self.diskList[diskId].blockList):
            print "Block Number is outside bounds of Disk"
            return False
        writeTry=self.writeBlock(self.diskList[diskId].blockList[blockNum-1]+1, writeData)
        if not writeTry:
            print "Am not able to write to that block"
            return False
        #Find a replication block
        replicaBlockNum=self.replicationBlock(diskId)
        if replicaBlockNum==None:
            print "Can't find a block to replicate on this disk"
        replicaWriteTry=self.writeBlock(replicaBlockNum+1, writeData)
        if replicaWriteTry:
            print "Successfully wrote in Replica! :)"
        else:
            print "Am not able to write in Replica! :("
        return True

    def readDisk(self,diskId,blockNum,readData):
        if not diskId in self.diskList:
            print "There is no such diskId present"
            return False
        if blockNum<1 or blockNum> len(self.diskList[diskId].blockList):
            print "Block Number is outside bounds of Disk"
            return False
        origReadTry=self.readBlock(self.diskList[diskId].blockList[blockNum-1]+1, readData)
        if(not origReadTry):
            print "Am Not able to read from the Original Block"
        else:
            print "read correctly from the Original Block"
            return True
        origPhyId=self.diskList[diskId].blockList[blockNum-1]
        replPhyId=self.blocksMetaData[origPhyId].replicationBlock
        if(replPhyId == None):
            print "Dont have a replica to read from! :("
        replReadTry=self.readBlock(self.diskList[diskId].blockList[blockNum-1]+1, readData)
        if(not replReadTry):
            print "Am not able to read from replica :("
        else:
            print "Correctly read from the replica :)"
            return True
        return False




def runBlockTests():
    myFileSystem= FileSystem(100,False,0.1)
    writeData1 = bytearray(b'2014CS50281')
    writeData2 = bytearray(b'2014CS50435')
    writeData3 = bytearray(b'2014CS10218')
    writeData4 = bytearray(b'2014CS50258')
    # Normal Writing to Free Blocks
    print "TEST:Normal Writing to Free Blocks"
    print ("Writing to 200 Block ,Data= "+writeData1.decode('utf-8'))
    myFileSystem.writeBlock(200,writeData1)
    print ("Writing to 400 Block ,Data= " + writeData2.decode('utf-8'))
    myFileSystem.writeBlock(400, writeData2)
    #OverWriting
    print "TEST:OverWriting"
    myFileSystem.writeBlock(400, writeData3)
    #Writing to inValid Blocks
    print "TEST:Writing to inValid Blocks"
    myFileSystem.writeBlock(600, writeData4)
    #Writing Large Data than BlockSize
    print "TEST:Writing Large Data than BlockSize"
    bigChar= ["a" for i in range(200)]
    bigString="-".join(bigChar)
    myFileSystem.writeBlock(100,bigString)

    dataToRead = bytearray(11)
    #Reading Normal Block
    print "TEST:Reading Normal Block"
    myFileSystem.readBlock(200,dataToRead)
    print "readData=",dataToRead.decode('utf-8')
    #Reading Empty Block
    print "TEST:Reading Empty Block"
    myFileSystem.readBlock(205, dataToRead)
    #Reading invalid Block
    print "TEST:Reading invalid Block"
    myFileSystem.readBlock(0, dataToRead)


def runDiskTests():
    writeData1 = bytearray(b'2014CS50281')
    writeData2 = bytearray(b'2014CS50435')
    myFileSystem = FileSystem(100,True,0.1)
    print "TEST:trying to create normal disk::\ncreateDisk(\"ayushDisk\",300)"
    result=myFileSystem.createDisk("ayushDisk",300)
    if result: print "SUCCESS"
    else: print "FAILURE"
    print "TEST:trying to create disk with same ID::\ncreateDisk(\"ayushDisk\", 100)"
    result =myFileSystem.createDisk("ayushDisk", 100)
    if result: print "SUCCESS"
    else: print "FAILURE"
    print "TEST:trying to create another disk::\ncreateDisk(\"deepakDisk\",100)"
    result =myFileSystem.createDisk("deepakDisk",100)
    if result: print "SUCCESS"
    else: print "FAILURE"
    print "TEST:trying to delete normal disk::\ndeleteDisk(\"ayushDisk\")"
    result =myFileSystem.deleteDisk("ayushDisk")
    if result: print "SUCCESS"
    else: print "FAILURE"
    print "TEST:trying to delete already deleted disk::\ndeleteDisk(\"ayushDisk\")"
    result =myFileSystem.deleteDisk("ayushDisk")
    if result: print "SUCCESS"
    else: print "FAILURE";
    print "TEST:trying to create another disk::\ncreateDisk(\"kapilDisk\",250)"
    result =myFileSystem.createDisk("kapilDisk",250)
    if result: print "SUCCESS"
    else: print "FAILURE"
    print "TEST:trying to create another disk::\ncreateDisk(\"ankitDisk\",120)"
    result =myFileSystem.createDisk("ankitDisk",120)
    if result: print "SUCCESS"
    else: print "FAILURE"
    print "TEST:trying to make a disk ::\ncreateDisk(\"ayushDisk\",30)"
    result =myFileSystem.createDisk("ayushDisk",30)
    if result: print "SUCCESS"
    else: print "FAILURE"
    #print myFileSystem.printDiskAllocation()
    print "TEST:trying to make a disk ::\ncreateDisk(\"amanDisk\", 1)"
    result =myFileSystem.createDisk("amanDisk", 1)
    if result: print "SUCCESS"
    else: print "FAILURE"
    #myFileSystem.printDiskAllocation()



    readBuffer1=bytearray(11)
    readBuffer2 = bytearray(11)
    print "TEST::writeDisk(\"ankitDisk\",10,writeData1)::::::Normal Write To Disk"
    myFileSystem.writeDisk("ankitDisk",10,writeData1)
    print "TEST::readDisk(\"ankitDisk\",10,readBuffer1)::::::Normal Read From Disk"
    myFileSystem.readDisk("ankitDisk",10,readBuffer1)
    print "readData=",readBuffer1.decode('utf-8')
    print "TEST::writeDisk(\"kapilDisk\",300,writeData2)::::::write to invalid Disk"
    myFileSystem.writeDisk("kapilDisk",300,writeData2)
    print "TEST::readDisk(\"ayushDisk\", 40, readBuffer1)::::::read from invalid Location"
    myFileSystem.readDisk("ayushDisk", 40, readBuffer1)
    print "TEST::readDisk(\"ayushDisk\", 20, readBuffer1)::::::read from Free block"
    myFileSystem.readDisk("ayushDisk", 20, readBuffer1)
    print "TEST::myFileSystem.readDisk(\"amanDisk\", 100, readBuffer2)::::::read from Invalid Disk"
    myFileSystem.readDisk("amanDisk", 100, readBuffer2)

    #Write once to ayushDisk of 30 length
    for i in range(30):
        myFileSystem.writeDisk("ayushDisk",i+1,writeData1)

    for k in range(1000):
        for t in range(30):
            myFileSystem.readDisk("ayushDisk",t+1,readBuffer1)

    print "errorBlocks",myFileSystem.corruptBlocksCount

if __name__ == '__main__':
    print "Disk Tests Start"
    runDiskTests()
    print "\n\n\n\n\n\n\n\n\nBlock Tests Start"
    runBlockTests()