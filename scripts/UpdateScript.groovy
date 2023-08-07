@Grab('com.xlson.groovycsv:groovycsv:1.3')
import static com.xlson.groovycsv.CsvParser.parseCsv

import org.forgerock.openicf.connectors.groovy.OperationType
import org.forgerock.openicf.connectors.groovy.ScriptedConfiguration
import org.identityconnectors.common.logging.Log
import org.identityconnectors.framework.common.objects.Attribute
import org.identityconnectors.framework.common.objects.AttributesAccessor
import org.identityconnectors.framework.common.objects.ObjectClass
import org.identityconnectors.framework.common.objects.OperationOptions
import org.identityconnectors.framework.common.objects.Uid
import org.identityconnectors.framework.common.exceptions.ConnectorException

import java.nio.channels.FileLock
import java.nio.channels.FileChannel
import java.nio.channels.OverlappingFileLockException

def operation = operation as OperationType
def configuration = configuration as ScriptedConfiguration

def objectClass = objectClass as ObjectClass
def options = options as OperationOptions
def updateAttributes = new AttributesAccessor(attributes as Set<Attribute>)
def uid = uid as Uid
def log = log as Log


switch (operation) {
    case OperationType.UPDATE:
        println "Entering update script for " + objectClass
        switch(objectClass){
            case ObjectClass.ACCOUNT:
                def fileLocation = configuration.propertyBag.__ACCOUNT__.fileloc
                def header = ['User_Name', 'User_First_Name', 'User_Last_Name', 'Groups']


                def userName = null
                def firstName = null
                def lastName = null
                def groups = []

                if (updateAttributes.hasAttribute("userName")) {
                    userName = updateAttributes.findString("userName")
                }

                if (updateAttributes.hasAttribute("firstName")) {
                    firstName = updateAttributes.findString("firstName")
                }

                if (updateAttributes.hasAttribute("lastName")) {
                    lastName = updateAttributes.findString("lastName")
                }

                if (updateAttributes.hasAttribute("groups")) {
                    groups = updateAttributes.findStringList("groups")
                }
                def result = writeAccountFileWithLock(fileLocation, header, userName, firstName, lastName, groups,log)
                if(result == true){
                    println "Updated file"
                    log.info("Updated file")
                } else {
                    throw new ConnectorException("Failed to update file")
                }
                return userName
            case ObjectClass.GROUP:
                def fileLocation = configuration.propertyBag.__GROUP__.fileloc
                def header = ["GROUP_NAME","GROUP_DESC"]

                def groupName = null
                def groupDescription = null

                if (updateAttributes.hasAttribute("groupName")) {
                    groupName = updateAttributes.findString("groupName")
                }

                if (updateAttributes.hasAttribute("groupDescription")) {
                    groupDescription = updateAttributes.findString("groupDescription")
                }

                def result = writeGroupFileWithLock(fileLocation, header, groupName, groupDescription,log)
                if(result == true){
                    println "Updated file"
                    log.info("Updated file")
                } else {
                    throw new ConnectorException("Failed to update file")
                }

                return groupName
            default:
                throw new ConnectorException("UpdateScript can not handle object type: " + objectClass.objectClassValue)


        }
    default:
        throw new ConnectorException("UpdateScript can not handle object type: " + objectClass.objectClassValue)
}

def writeGroupFileWithLock(String filePath, ArrayList header, String groupName, String groupDescription, Log logger){
    File f = new File(filePath)
    if(!f.exists()){
        throw new ConnectorException("File not found: " + filePath)
    } else {
        println "writeGroupFileWithLock:File found: " + filePath
        logger.info("writeGroupFileWithLock:File found: ")
    }

    def csvContent = new File(filePath).text
    def csvData = parseCsv(separator: ',', readFirstLine: false,csvContent)
    def newData = csvData.collect { row ->
        [GROUP_NAME: row.GROUP_NAME, GROUP_DESC: row.GROUP_DESC]
    }
    // Sort by 'User_Name'.
    newData.sort { a, b -> a.GROUP_NAME <=> b.GROUP_NAME }
    def alData = []

    for (line in newData) {
        if (line.GROUP_NAME.equalsIgnoreCase(groupName)) {
            def tmpFileName = filePath + "_delete."+System.currentTimeMillis()
            def tmpFile = new File(tmpFileName)
            tmpFile.write(header.collect { "\"${it}\"" }.join(','))
            tmpFile.append('\n\"'+line.GROUP_NAME+'\",\"'+line.GROUP_DESC+'\"')
            println "writeGroupFileWithLock:Removed:"+ line.GROUP_NAME
            logger.info("writeGroupFileWithLock:Removed:")
        } else {
            alData << [line.GROUP_NAME, line.GROUP_DESC]
            println "writeGroupFileWithLock: Added:"
            logger.info("writeGroupFileWithLock: Added:")
        }
    }

    println "writeGroupFileWithLock: Appending changed record"
    logger.info("writeGroupFileWithLock: Appending changed record")

    try {
        alData << [groupName, groupDescription]
        println "writeGroupFileWithLock: Added ${userName}"
        logger.info("writeGroupFileWithLock: Added row")
    } catch (Exception e) {
        println "writeGroupFileWithLock:Error adding data: ${e}"
        logger.info("writeGroupFileWithLock:Error adding data:")
    }
    // Sort by 'User_Name'.
    try {
        alData.sort {a, b -> a[0].compareTo(b[0])}
    } catch (Exception e) {
        println "writeGroupFileWithLock:Error sorting data: ${e}"
    }
    //println alData
    def bakFileName = filePath + "_bak."+System.currentTimeMillis()
    def bakFile = new File(bakFileName)
    bakFile << csvContent
    println "writeGroupFileWithLock:File copied to ${bakFileName}"
    logger.info("writeGroupFileWithLock:File copied")

    // open the file with a RandomAccessFile
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
    alData.each { record ->
        def formattedRecord = record.collect { "\"${it}\"" }.join(', ')
        f.append("\n${formattedRecord}")
    }

    // make sure to release the lock and close the channel when done
    lock.release()
    channel.close()

    return true

}

def writeAccountFileWithLock(String filePath, ArrayList header, String userName, String firstName, String lastName, ArrayList groups, Log logger){
    File f = new File(filePath)
    if(!f.exists()){
        throw new ConnectorException("File not found: " + filePath)
    } else {
        println "writeAccountFileWithLock:File found: " + filePath
        logger.info("writeAccountFileWithLock:File found: ")
    }

    def csvContent = new File(filePath).text
    def csvData = parseCsv(separator: ',', readFirstLine: false,csvContent)
    def newData = csvData.collect { row ->
        [User_Name: row.User_Name, User_First_Name: row.User_First_Name, User_Last_Name: row.User_Last_Name, Groups: row.Groups]
    }
    // Sort by 'User_Name'.
    newData.sort { a, b -> a.User_Name <=> b.User_Name }
    def alData = []

    for (line in newData) {
        //println line.User_Name
        if (line.User_Name.equalsIgnoreCase(userName)) {
            def tmpFileName = filePath + "_delete."+System.currentTimeMillis()
            def tmpFile = new File(tmpFileName)
            tmpFile.write(header.collect { "\"${it}\"" }.join(','))
            tmpFile.append('\n\"'+line.User_Name+'\",\"'+line.User_First_Name+'\",\"'+line.User_Last_Name+'\",\"'+line.Groups+'\"')
            println "writeAccountFileWithLock:Removed:"+ line.User_Name
            logger.info("writeAccountFileWithLock:Removed:")
        } else {
            alData << [line.User_Name, line.User_First_Name, line.User_Last_Name, line.Groups]
            println "writeAccountFileWithLock: Added:"
            logger.info("writeAccountFileWithLock: Added:")
        }
    }
    println "writeAccountFileWithLock: Appending changed record"
    logger.info("writeAccountFileWithLock: Appending changed record")

    try {
        alData << [userName, firstName, lastName, groups.join(',')]
        println "writeAccountFileWithLock: Added ${userName}"
        logger.info("writeAccountFileWithLock: Added row")
    } catch (Exception e) {
        println "writeAccountFileWithLock:Error adding data: ${e}"
        logger.info("writeAccountFileWithLock:Error adding data:")
    }
    // Sort by 'User_Name'.
    try {
        alData.sort {a, b -> a[0].compareTo(b[0])}
    } catch (Exception e) {
        println "writeFileWithLock:Error sorting data: ${e}"
    }
    //println alData
    def bakFileName = filePath + "_bak."+System.currentTimeMillis()
    def bakFile = new File(bakFileName)
    bakFile << csvContent
    println "writeAccountFileWithLock:File copied to ${bakFileName}"
    logger.info("writeAccountFileWithLock:File copied")

    // open the file with a RandomAccessFile
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
    alData.each { record ->
        def formattedRecord = record.collect { "\"${it}\"" }.join(', ')
        f.append("\n${formattedRecord}")
    }

    // make sure to release the lock and close the channel when done
    lock.release()
    channel.close()

    return true
}