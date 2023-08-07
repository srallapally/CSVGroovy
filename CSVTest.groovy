@Grab('com.xlson.groovycsv:groovycsv:1.3')
import static com.xlson.groovycsv.CsvParser.parseCsv
import java.nio.channels.FileLock
import java.nio.channels.FileChannel
import java.nio.channels.OverlappingFileLockException

def loadCsvData = {
    def sourceFileName = '/Users/sanjay.rallapally/Downloads/ap-tungsten.csv'
    def sourceFile = new File(sourceFileName)
    def csvContent = new File('/Users/sanjay.rallapally/Downloads/ap-tungsten.csv').text
    def csvData = parseCsv(separator: ',', readFirstLine: false,csvContent)
    def header = ['User_Name', 'User_First_Name', 'User_Last_Name', 'Groups']

    def bakFileName = sourceFileName + "_bak."+System.currentTimeMillis()
    def bakFile = new File(bakFileName)
    bakFile << csvContent
    println "File copied to ${bakFileName}"

    def tmpFileName = sourceFileName + "_delete."+System.currentTimeMillis()
    def tmpFile = new File(tmpFileName)
    def alData = []
    def userName = "100009"
    def firstName = "MARY-SUE"
    def lastName = "LYNCH"
    def groups = ["DEFAULT", "DIRECTOR_GRP", "GSFAP50M"]
    for (line in csvData) {
        //println line.User_Name
        if (line.User_Name.equalsIgnoreCase('100009')) {
            tmpFile.write(header.collect { "\"${it}\"" }.join(','))
            tmpFile.append('\n\"'+line.User_Name+'\",\"'+line.User_First_Name+'\",\"'+line.User_Last_Name+'\",\"'+line.Groups+'\"')
            println "Removed ${line.User_Name}"
        } else {
            //println "Add " + line
            alData << [line.User_Name, line.User_First_Name, line.User_Last_Name, line.Groups]
        }
    }
    alData << [userName, firstName, lastName, groups.join(',')]
    // Sort by 'User_Name'.
    alData.sort {a, b -> a[0].compareTo(b[0])}
    /*
    for (line in csvData) {
        if (line[0] == '100009') {
            tmpFile.write(header.collect { "\"${it}\"" }.join(','))
            tmpFile.append('\n\"'+line.User_Name+'\",\"'+line.User_First_Name+'\",\"'+line.User_Last_Name+'\",\"'+line.Groups+'\"')
        }
    }
    csvData = parseCsv(separator: ',', readFirstLine: false,csvContent)
    def alData = []
    for (line1 in csvData) {
        //println line1.User_Name
        if (line1[0] == '100009') {
            //csvData.remove()
            println "Removed ${line1.User_Name}"
        } else {
            alData.add(line1)
        }
    }
    */
    def status = openFileWithLock(sourceFileName, header, alData)
    return csvData
}

def openFileWithLock(String filePath, ArrayList header, ArrayList data){
    // open the file with a RandomAccessFile
    File f = new File(filePath)
    FileChannel channel = new RandomAccessFile(f, 'rw').channel

    // try to get an exclusive lock on this channel
    FileLock lock = null
    while (true) {
        try {
            lock = channel.tryLock()
            break
        } catch (OverlappingFileLockException e) {
            // File is open somewhere else and locked, sleep for a bit and try again
            sleep(1000)
        }
    }
    f.write(header.collect { "\"${it}\"" }.join(','))
    data.each { record ->
        def formattedRecord = record.collect { "\"${it}\"" }.join(', ')
        f.append("\n${formattedRecord}")
    }
    //for (line in data) {
    //     //println line
    //       f.append('\n\"'+line.User_Name+'\",\"'+line.User_First_Name+'\",\"'+line.User_Last_Name+'\",\"'+line.Groups+'\"')
    //}
    // make sure to release the lock and close the channel when done
    lock.release()
    channel.close()
    return true
}





//def searchUserId = { userIdToFind, csvMap ->
//    return csvMap[userIdToFind]
//}

def csvMap = loadCsvData()

//def userRow = searchUserId('100330', csvMap)  // Replace 'userid_to_find' with the actual id you are searching
//csvMap.remove('100330')
//if (userRow) {
  //  println("Found user: ${userRow}")
//} else {
 //   println("User not found.")
//}

