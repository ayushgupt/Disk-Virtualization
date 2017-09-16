# Disk Creation and Deletion Supported; Contiguous Allocation;

class BlockData:
    def __init__(self,blockSize):
        self.data=bytearray(blockSize)


class BlockMetaData:
    def __init__(self):
        self.free=True
        self.allotted=False
        self.diskID=None

    def freeFromDisk(self):
        self.allotted=False
        self.diskID=None
        self.free=True

    def allotToDisk(self,disID):
        self.diskID=disID
        self.allotted=True

class DiskData:
    def __init__(self,idD,sizeD,startIndex):
        self.idDisk=idD
        self.sizeDisk=sizeD
        self.startIndexZeroIndexed=startIndex
    def blocksInDisk(self):
        return range(self.startIndexZeroIndexed,self.startIndexZeroIndexed+self.sizeDisk)


class FileSystem:
    def __init__(self,blockSize):
        self.blockSize=blockSize
        self.diskA = [BlockData(blockSize) for i in range(200)]
        self.diskB = [BlockData(blockSize) for i in range(300)]
        self.blocksMetaData = [BlockMetaData() for i in range(500)]
        self.diskList={}

    def writeBlock(self, blockNum, writeData):
        if blockNum<1 or blockNum>500:
            print "Block Number is Invalid!"
            return
        if len(writeData)>self.blockSize:
            print "Too much data to write in 1 block"
            return
        if not self.blocksMetaData[blockNum-1].free:
            print "Over Writing to a Block"
        self.blocksMetaData[blockNum-1].free=False
        if blockNum>=1 and blockNum<=200:
            self.diskA[blockNum-1].data[0:len(writeData)]=writeData
        else:
            self.diskB[blockNum-201].data[0:len(writeData)]=writeData
        return


    def readBlock(self,blockNum,readData):
        if blockNum < 1 or blockNum > 500:
            print "Block Number is Invalid!"
            return
        if self.blocksMetaData[blockNum-1].free:
            print "Block is Free, Nothing to Read!"
            return
        lengthToRead=min(self.blockSize,len(readData))
        print "reading Length",lengthToRead
        if blockNum>=1 and blockNum<=200:
            readData[0:lengthToRead]=self.diskA[blockNum-1].data[0:lengthToRead]
        else:
            readData[0:lengthToRead]=self.diskB[blockNum-201].data[0:lengthToRead]
        return

    def createDisk(self,diskID,numBlocks):
        #check if DiskID is already there
        if(diskID in self.diskList):
            print "Such ID for disk already exists"
            return False
        #check if continuos space exists
        blockStart=-1
        for i in range(500):
            if(not self.blocksMetaData[i].allotted):
                if((i+numBlocks)>=500):
                    print "Don't have these many blocks to make Disk"
                    return False
                else:
                    if all(not blocks.allotted for blocks in self.blocksMetaData[i:i+numBlocks] ):
                        blockStart=i
                        break
        if blockStart==(-1):
            print "NOT SO MUCH Continuous blocks available!"
            return False
        #set block as allotted
        for blocks in self.blocksMetaData[blockStart: blockStart + numBlocks]:
            blocks.allotToDisk(diskID)
        newDisk=DiskData(diskID,numBlocks,blockStart)
        self.diskList[diskID]=newDisk
        return True

    def deleteDisk(self,id):
        if not id in self.diskList:
            print "There is no disk with such an ID!"
            return False
        rangeTrav=self.diskList[id].blocksInDisk
        for blocksNumber in rangeTrav:
            self.blocksMetaData[blocksNumber].freeFromDisk()
        self.blocksMetaData.pop(id)
        return True

    def writeDisk(self, diskId, blockNum, writeData):
        if not diskId in self.diskList:
            print "There is no such diskId present"
            return False
        if blockNum<1 or blockNum> self.diskList[diskId].sizeDisk:
            print "Block Number is outside bounds of Disk"
            return False
        self.writeBlock(self.diskList[diskId].startIndexZeroIndexed + blockNum - 1, writeData)
        return True

    def readDisk(self,diskId,blockNum,readData):
        if not diskId in self.diskList:
            print "There is no such diskId present"
            return False
        if blockNum<1 or blockNum> self.diskList[diskId].sizeDisk:
            print "Block Number is outside bounds of Disk"
            return False
        self.readBlock(self.diskList[diskId].startIndexZeroIndexed + blockNum - 1, readData)
        return True





def runBlockTests():
    myFileSystem= FileSystem(100)
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
    myFileSystem = FileSystem(100)
    print "TEST:trying to create normal disk"
    result=myFileSystem.createDisk("ayushDisk",300)
    if result: print "SUCCESS"
    else: print "FAILURE"
    print "TEST:trying to create disk with same ID"
    result =myFileSystem.createDisk("ayushDisk", 100)
    if result: print "SUCCESS"
    else: print "FAILURE"
    print "TEST:trying to create another disk"
    result =myFileSystem.createDisk("deepakDisk",100)
    if result: print "SUCCESS"
    else: print "FAILURE"
    print "TEST:trying to delete normal disk"
    result =myFileSystem.deleteDisk("ayushDisk")
    if result: print "SUCCESS"
    else: print "FAILURE"
    print "TEST:trying to delete already deleted disk"
    result =myFileSystem.deleteDisk("ayushDisk")
    if result: print "SUCCESS"
    else: print "FAILURE";
    print "TEST:trying to create another disk"
    result =myFileSystem.createDisk("KapilDisk",250)
    if result: print "SUCCESS"
    else: print "FAILURE"
    print "TEST:trying to create another disk which is not possible without fragmentation"
    result =myFileSystem.createDisk("AnkitDisk",120)
    if result: print "SUCCESS"
    else: print "FAILURE"
    print "TEST:trying to make a disk that should be made"
    result =myFileSystem.createDisk("ayushDisk",100)
    if result: print "SUCCESS"
    else: print "FAILURE"
    print "TEST:trying to make a disk that should be made"
    result =myFileSystem.createDisk("ankitDisk", 50)
    if result: print "SUCCESS"
    else: print "FAILURE"



if __name__ == '__main__':
    runDiskTests()
    runBlockTests()