# diskA = [bytearray(100) for i in range(200)]
# diskB = [bytearray(100) for i in range(300)]

diskA = []
for i in range (200) :
    b = bytearray(100)
    diskA.append (b)


diskB = []
for i in range (300) :
    b = bytearray (100)
    diskB.append(b)


class metaData:
    def __init__(self):
        self.size = 0
        self.free = True


metaDataArray = []
for i in range(500) :
    meta = metaData()
    metaDataArray.append(meta)


def writeBlock (block_No, block_inf):
    if (len(block_inf) > 100):
        print ("Block Data Big : A block can have atmost 100 bytes of data")
        return
    if (block_No < 1 or block_No >500):
        print ("Invalid Block No : Please Enter a block number between 1 and 500")
        return
    meta = metaDataArray[block_No-1]
    meta.free = False
    meta.size = len(block_inf)
    if block_No <= 200:
        block = diskA[block_No - 1]
    else:
        block = diskB[block_No - 200 - 1]
    block[:len(block_inf)] = block_inf


def readBlock(block_No, block_inf):
    if (block_No < 1 or block_No >500):
        print ("Invalid Block No : Please Enter a block number between 1 and 500")
        return
    metadata = metaDataArray[block_No - 1]
    if (metadata.free):
        print ("Block Free: No Information Stored in Block")
        return
    else:
        if block_No <= 200:
            block = diskA[block_No - 1]
        else:
            block = diskB[block_No - 200 - 1]
        size = min(len(block_inf), metadata.size)
        block_inf[:size] = block[:size]