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
                def result = writeFileWithLock(fileLocation, header, userName, firstName, lastName, groups,log)
                if(!result){
                    throw new ConnectorException("Failed to update file")
                }
            case ObjectClass.GROUP:
                def fileLocation = configuration.propertyBag.__GROUP__.fileloc
                def header = ['Group_Name', 'Group_Description', 'Group_Members']

                def groupName = null
                def groupDescription = null
                def groupMembers = []

                if (updateAttributes.hasAttribute("groupName")) {
                    groupName = updateAttributes.findString("groupName")
                }

                if (updateAttributes.hasAttribute("groupDescription")) {
                    groupDescription = updateAttributes.findString("groupDescription")
                }

                if (updateAttributes.hasAttribute("groupMembers")) {
                    groupMembers = updateAttributes.findStringList("groupMembers")
                }
                def result = writeFileWithLock(fileLocation, header, groupName, groupDescription, groupMembers,log)
                if(result == true){
                    println "Updated file"
                    log.info("Updated file")
                } else {
                    throw new ConnectorException("Failed to update file")
                }
        }
    default:
        throw new ConnectorException("UpdateScript can not handle object type: " + objectClass.objectClassValue)
}
return uid

def writeFileWithLock(String filePath, ArrayList header, String userName, String firstName, String lastName, ArrayList groups, Log logger){
    File f = new File(filePath)
    if(!f.exists()){
        throw new ConnectorException("File not found: " + filePath)
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
        println line.User_Name
        if (line.User_Name.equalsIgnoreCase(userName)) {
            def tmpFileName = filePath + "_delete."+System.currentTimeMillis()
            def tmpFile = new File(tmpFileName)
            tmpFile.write(header.collect { "\"${it}\"" }.join(','))
            tmpFile.append('\n\"'+line.User_Name+'\",\"'+line.User_First_Name+'\",\"'+line.User_Last_Name+'\",\"'+line.Groups+'\"')
        } else {
            alData << [line.User_Name, line.User_First_Name, line.User_Last_Name, line.Groups]
        }
    }

    try {
        alData << [userName, firstName, lastName, groups.join(',')]
    } catch (Exception e) {
        println "writeFileWithLock:Error adding data: ${e}"
        logger.info("writeFileWithLock:Error adding data:")
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
    lock.release()
    channel.close()

    return true
}