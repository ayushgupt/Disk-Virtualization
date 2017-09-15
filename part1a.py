# This file supports read write to a block; it doesn't support disk creation

class BlockData:
    def __init__(self,blockSize):
        self.data=bytearray(blockSize)

class BlockMetaData:
    def __init__(self):
        self.free=True


class FileSystem:
    def __init__(self,blockSize):
        self.blockSize=blockSize
        self.diskA = [BlockData(blockSize) for i in range(200)]
        self.diskB = [BlockData(blockSize) for i in range(300)]
        self.blocksMetaData = [BlockMetaData() for i in range(500)]

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



def runTests():
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


if __name__ == '__main__':
    runTests()